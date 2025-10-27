#!/usr/bin/env python3
"""
Ottawa Council Vote Tracker Scraper
Enhanced with direct councillor linking.

Scrapes voting records from the Ottawa City Council eScribe system
and stores them in Airtable for analysis and visualization.
"""

import os
import re
import time
import logging
from datetime import datetime, timedelta

import httpx
from bs4 import BeautifulSoup

try:
    from pyairtable import Table
except Exception:
    Table = None


# === CONFIGURATION ===
AIRTABLE_TOKEN = os.environ.get("AIRTABLE_TOKEN")
BASE_ID = os.environ.get("AIRTABLE_BASE_ID")

# In-memory cache to avoid repeated councillor lookups/creates during a run
councillor_cache = {}

# Airtable table names
MEETINGS_TABLE = "Meetings"
MOTIONS_TABLE = "Motions"
VOTES_TABLE = "Votes"
COUNCILLORS_TABLE = "Councillor"  # your table name

BASE_URL = "https://pub-ottawa.escribemeetings.com/"
ROLLING_WINDOW_DAYS = 14

# Logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


# === HTTP HELPERS ===
def safe_request(method, url, json=None, max_retries=3, timeout=30):
    for i in range(max_retries):
        try:
            r = httpx.request(method, url, json=json, timeout=timeout)
            r.raise_for_status()
            return r
        except Exception as e:
            logger.warning(
                "%s %s failed (%d/%d): %s", method.upper(), url, i + 1, max_retries, e
            )
            time.sleep(2)
    logger.error("%s %s failed after retries.", method.upper(), url)
    return None


def get_meetings(start, end):
    """Fetch meetings from the Ottawa eScribe calendar API."""
    payload = {
        "calendarStartDate": f"{start}T00:00:00-04:00",
        "calendarEndDate": f"{end}T00:00:00-04:00",
    }
    r = safe_request(
        "post", f"{BASE_URL}MeetingsCalendarView.aspx/GetCalendarMeetings", json=payload
    )
    if r is None:
        return []
    try:
        data = r.json()
    except Exception as e:
        logger.warning("Invalid JSON from eScribe API: %s", e)
        return []

    # eScribe wraps result in {'d': [...] } sometimes
    if isinstance(data, dict) and "d" in data:
        return data["d"] or []
    if isinstance(data, list):
        return data
    return []


# === PARSING ===
def parse_votes(html):
    """Extract motion and voting data from a meeting minutes HTML page."""
    soup = BeautifulSoup(html, "html.parser")
    motions = []
    for item in soup.select(".AgendaItemContainer"):
        motions.append(parse_motion_item(item))
    return motions


def parse_motion_item(item):
    """Parse a single motion (.AgendaItemContainer) into a dictionary."""
    title_el = item.select_one(".AgendaItemTitle a")
    result_el = item.select_one(".MotionResult")
    voters_table = item.select_one(".MotionVoters")

    for_names, against_names = [], []
    if voters_table:
        for_names, against_names = parse_voters_table(voters_table)

    return {
        "title": title_el.text.strip() if title_el else None,
        "result": result_el.text.strip() if result_el else None,
        "for_names": for_names,
        "against_names": against_names,
    }


def parse_voters_table(table):
    """Return (for_names, against_names) parsed from a MotionVoters table."""
    for_names, against_names = [], []
    for row in table.select("tr"):
        label = row.select_one(".VoterVote")
        names_el = row.select_one(".VotesUsers")
        if not label or not names_el:
            continue
        raw_names = [
            n.strip().strip(",") for n in names_el.text.split(",") if n.strip()
        ]
        # normalize internal whitespace and remove leading 'and '
        names = [
            re.sub(r"^and\s+", "", re.sub(r"\s+", " ", n).strip(), flags=re.I)
            for n in raw_names
        ]
        if label.text.strip().startswith("For"):
            for_names.extend(names)
        elif label.text.strip().startswith("Against"):
            against_names.extend(names)
    return for_names, against_names


def normalize_outcome(text):
    """Normalize motion result text to 'Passed' / 'Failed' if possible."""
    if not text:
        return None
    t = text.lower()
    if "carried" in t or "passed" in t or "adopted" in t:
        return "Passed"
    if "lost" in t or "failed" in t or "not carried" in t:
        return "Failed"
    return text.strip()


def deduplicate_motions(motions):
    """Return motions with duplicate (title, result) removed, preserving order."""
    seen = set()
    out = []
    for m in motions:
        key = (m.get("title"), m.get("result"))
        if key in seen:
            continue
        seen.add(key)
        out.append(m)
    return out


# === AIRTABLE HELPERS ===
def safe_airtable_create(table_obj, payload, max_retries=3):
    sleep_base = 1
    for i in range(max_retries):
        try:
            return table_obj.create(payload)
        except Exception as e:
            text = str(e)
            # handle 429 explicitly (rate limit)
            if "429" in text:
                logger.warning("Rate limit hit: %s; sleeping before retry", text)
                time.sleep(2)
            else:
                logger.warning(
                    "Airtable create failed (%d/%d): %s", i + 1, max_retries, e
                )
            # exponential backoff (with small base)
            time.sleep(sleep_base * (2**i))
    logger.error("Giving up on Airtable create: %s", payload)
    return None


def get_or_create_councillor(table_councillors, name):
    """Ensure councillor record exists, return record ID."""
    if not name:
        return None
    # check in-memory cache first
    if name in councillor_cache:
        return councillor_cache[name]

    try:
        # sanitize name in formula (escape single quotes)
        safe_name = name.replace("'", "''")
        existing = table_councillors.all(formula=f"{{Councillor}}='{safe_name}'")
        if existing:
            councillor_cache[name] = existing[0]["id"]
            return councillor_cache[name]
        new_rec = table_councillors.create({"Councillor": name})
        logger.info("Created councillor record: %s", name)
        councillor_cache[name] = new_rec["id"]
        return councillor_cache[name]
    except Exception as e:
        logger.warning("Failed councillor lookup/create for %s: %s", name, e)
        return None


def upload_to_airtable(meeting, motions):
    """Upload one meeting’s motions and votes to Airtable."""
    # support dry-run flag when caller passes it (keeps signature backward compatible)
    dry_run = False
    if isinstance(meeting, dict) and "_dry_run" in meeting:
        dry_run = bool(meeting.pop("_dry_run"))
    motions = deduplicate_motions(motions)
    if Table is None or not AIRTABLE_TOKEN or not BASE_ID:
        logger.info("Airtable not configured or pyairtable missing; skipping upload")
        return

    table_meetings = Table(AIRTABLE_TOKEN, BASE_ID, MEETINGS_TABLE)
    table_motions = Table(AIRTABLE_TOKEN, BASE_ID, MOTIONS_TABLE)
    table_votes = Table(AIRTABLE_TOKEN, BASE_ID, VOTES_TABLE)
    table_councillors = Table(AIRTABLE_TOKEN, BASE_ID, COUNCILLORS_TABLE)

    # Idempotency: avoid duplicate meeting uploads
    try:
        safe_id = str(meeting.get("ID", "")).replace("'", "''")
        existing = table_meetings.all(formula=f"{{Meeting ID}}='{safe_id}'")
    except Exception as e:
        logger.warning("Failed to query Airtable: %s", e)
        existing = None

    if existing:
        m_record = existing[0]
        logger.info("Meeting already exists: %s", meeting.get("MeetingName"))
    else:
        m_payload = {
            "Meeting ID": meeting.get("ID"),
            "Committee": meeting.get("MeetingName"),
            "Date": meeting.get("StartDate"),
            "Source": meeting.get("Url"),
        }
        m_record = safe_airtable_create(table_meetings, m_payload)
        if m_record is None:
            return

    # Upload motions and votes
    for motion in motions:
        motion_record = safe_airtable_create(
            table_motions,
            {
                "Meeting": [m_record["id"]],
                "Decision": motion["title"],
                "Outcome": normalize_outcome(motion["result"]),
                "Committee": meeting["MeetingName"],
                "Source": meeting["Url"],
                "For Count": len(motion["for_names"]),
                "Against Count": len(motion["against_names"]),
            },
        )
        if not motion_record:
            continue

        # create votes but avoid duplicates: check existing votes for the motion+person
        def vote_exists(table_votes_obj, motion_id, councillor_id):
            try:
                # formula: check for Motion link and Councillor Record link equality
                # escape single quotes
                safe_motion = str(motion_id).replace("'", "''")
                safe_councillor = str(councillor_id).replace("'", "''")
                formula = f"AND({{Motion}}='{safe_motion}',{{Councillor Record}}='{safe_councillor}')"
                existing_votes = table_votes_obj.all(formula=formula)
                return bool(existing_votes)
            except Exception as e:
                logger.warning("Failed to check existing votes: %s", e)
                return False

        for name in motion["for_names"]:
            councillor_id = get_or_create_councillor(table_councillors, name)
            if not councillor_id:
                continue
            if vote_exists(table_votes, motion_record["id"], councillor_id):
                logger.debug(
                    "Vote already exists for %s on %s", name, motion_record["id"]
                )
                continue
            if not dry_run:
                safe_airtable_create(
                    table_votes,
                    {
                        "Motion": [motion_record["id"]],
                        "Councillor Record": [councillor_id],
                        "Vote": "Yes",
                    },
                )

        for name in motion["against_names"]:
            councillor_id = get_or_create_councillor(table_councillors, name)
            if not councillor_id:
                continue
            if vote_exists(table_votes, motion_record["id"], councillor_id):
                logger.debug(
                    "Vote already exists for %s on %s", name, motion_record["id"]
                )
                continue
            if not dry_run:
                safe_airtable_create(
                    table_votes,
                    {
                        "Motion": [motion_record["id"]],
                        "Councillor Record": [councillor_id],
                        "Vote": "No",
                    },
                )


# === MAIN EXECUTION ===
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Scrape Ottawa Council vote records")
    parser.add_argument(
        "--since",
        help="Scrape meetings starting from this date (YYYY-MM-DD). Overrides rolling window",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse and show findings but do not write to Airtable",
    )
    args = parser.parse_args()

    if args.since:
        try:
            start_date = datetime.strptime(args.since, "%Y-%m-%d")
        except Exception as e:
            logger.error("Invalid --since date: %s", e)
            raise
    else:
        start_date = datetime.now() - timedelta(days=ROLLING_WINDOW_DAYS)

    end_date = datetime.now()
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    logger.info("Scraping meetings from %s to %s", start_str, end_str)
    meetings = get_meetings(start_str, end_str)
    logger.info("Found %d meetings in range.", len(meetings))

    for meeting in meetings:
        html_links = [
            BASE_URL + d["Url"]
            for d in meeting.get("MeetingDocumentLink", [])
            if d.get("Type") == "PostMinutes"
            and d.get("Format") == "HTML"
            and "English" in d.get("Url", "")
        ]
        for url in html_links:
            logger.info("Processing minutes: %s", url)
            r = safe_request("get", url)
            if not r:
                logger.warning("Skipping %s (fetch failed)", url)
                continue
            motions = parse_votes(r.text)
            if not motions:
                logger.info("No divided votes found for %s", meeting.get("MeetingName"))
                continue
            logger.info(
                "%s: %d motions found", meeting.get("MeetingName"), len(motions)
            )
            # pass dry-run flag via meeting dict so upload_to_airtable can skip writes
            if args.dry_run:
                meeting["_dry_run"] = True
            upload_to_airtable(meeting, motions)

    logger.info("Scraping completed successfully.")
