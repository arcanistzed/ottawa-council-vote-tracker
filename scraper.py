#!/usr/bin/env python3
"""
Ottawa Council Vote Tracker Scraper
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
from dateutil import parser as dateutil_parser, tz as dateutil_tz
from zoneinfo import ZoneInfo
from pyairtable import Api
import urllib3

# Disable SSL warnings since eScribe uses non-standard certs
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# === CONFIGURATION ===
AIRTABLE_TOKEN = os.environ.get("AIRTABLE_TOKEN")
BASE_ID = os.environ.get("AIRTABLE_BASE_ID")

BASE_URL = "https://pub-ottawa.escribemeetings.com/"
MEETINGS_TABLE = "Meetings"
MOTIONS_TABLE = "Motions"
VOTES_TABLE = "Votes"
COUNCILLORS_TABLE = "Councillors"
ROLLING_WINDOW_DAYS = 14
PREFERRED_TZ = "America/Toronto"

# Cache to avoid redundant councillor lookups
councillor_cache = {}

# Logging setup
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# === HTTP HELPERS ===
def safe_request(method, url, json=None, max_retries=3, timeout=30):
    for i in range(max_retries):
        try:
            r = httpx.request(method, url, json=json, timeout=timeout, verify=False)
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
    if not r:
        return []
    try:
        data = r.json()
        if isinstance(data, dict) and "d" in data:
            return data["d"] or []
        return data if isinstance(data, list) else []
    except Exception as e:
        logger.warning("Invalid JSON from eScribe API: %s", e)
        return []


# === PARSING ===
def parse_votes(html):
    """Extract motions and votes from meeting minutes HTML."""
    soup = BeautifulSoup(html, "html.parser")
    motions = [parse_motion_item(item) for item in soup.select(".AgendaItemContainer")]
    return motions


def parse_motion_item(item):
    """Extract one motion’s title, result, and votes."""
    title_el = item.select_one(".AgendaItemTitle a")
    result_el = item.select_one(".MotionResult")
    voters_table = item.select_one("table.MotionVoters")

    for_names, against_names = [], []
    if voters_table:
        for_names, against_names = parse_voters_table(voters_table)

    if for_names or against_names:
        logger.info(
            "Parsed divided motion: %s | For=%d | Against=%d",
            title_el.text.strip() if title_el else "Untitled",
            len(for_names),
            len(against_names),
        )

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

        text = re.sub(r"\band\b", ",", names_el.get_text(" ", strip=True), flags=re.I)
        raw_names = [n.strip().strip(",") for n in re.split(r"[;,]", text) if n.strip()]
        names = [re.sub(r"\s+", " ", n).strip() for n in raw_names]

        if "for" in label.text.lower():
            for_names.extend(names)
        elif "against" in label.text.lower():
            against_names.extend(names)

    return for_names, against_names


def normalize_outcome(text):
    """Normalize motion result text."""
    if not text:
        return None
    t = text.lower()
    if any(x in t for x in ["carried", "passed", "adopted"]):
        return "Passed"
    if any(x in t for x in ["lost", "failed", "not carried"]):
        return "Failed"
    return text.strip()


def deduplicate_motions(motions):
    """Remove duplicate (title, result) combinations."""
    seen, out = set(), []
    for m in motions:
        key = (m.get("title"), m.get("result"))
        if key not in seen:
            seen.add(key)
            out.append(m)
    return out


def format_airtable_date(raw_date):
    """Convert eScribe date to ISO8601 for Airtable."""
    if not raw_date:
        return None
    try:
        dt = dateutil_parser.parse(raw_date)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=dateutil_tz.gettz(PREFERRED_TZ))
        return dt.isoformat()
    except Exception:
        logger.warning("Could not parse date: %s", raw_date)
        return None


def format_airtable_date_dateonly(raw_date):
    """Return YYYY-MM-DD string for date-only fallback."""
    try:
        dt = dateutil_parser.parse(raw_date)
        return dt.date().isoformat()
    except Exception:
        return None


# === AIRTABLE HELPERS ===
def safe_airtable_create(table_obj, payload, max_retries=3):
    for i in range(max_retries):
        try:
            return table_obj.create(payload)
        except Exception as e:
            text = str(e)
            if "422" in text:
                logger.error(
                    "Airtable rejected payload (422) → Table: %s | Payload: %s",
                    getattr(table_obj, "name", "<table>"),
                    payload,
                )
                break
            if "429" in text:
                time.sleep(2)
            time.sleep(1.5 * (2**i))
    return None


def get_or_create_councillor(table_councillors, name):
    """Ensure councillor record exists, return record ID."""
    if not name:
        return None
    if name in councillor_cache:
        return councillor_cache[name]
    try:
        safe_name = name.replace("'", "''")
        existing = table_councillors.all(formula=f"{{Councillor}}='{safe_name}'")
        if existing:
            councillor_cache[name] = existing[0]["id"]
            return councillor_cache[name]
        rec = table_councillors.create({"Councillor": name})
        councillor_cache[name] = rec["id"]
        logger.info("Created councillor: %s", name)
        return rec["id"]
    except Exception as e:
        logger.warning("Councillor lookup/create failed for %s: %s", name, e)
        return None


def upload_to_airtable(meeting, motions):
    """Upload one meeting’s motions and votes to Airtable."""
    dry_run = meeting.pop("_dry_run", False)
    motions = deduplicate_motions(motions)
    if not (Api and AIRTABLE_TOKEN and BASE_ID):
        logger.info("Airtable not configured; skipping upload")
        return

    api = Api(AIRTABLE_TOKEN)
    t_meetings = api.table(BASE_ID, MEETINGS_TABLE)
    t_motions = api.table(BASE_ID, MOTIONS_TABLE)
    t_votes = api.table(BASE_ID, VOTES_TABLE)
    t_councillors = api.table(BASE_ID, COUNCILLORS_TABLE)

    # Check if meeting already exists
    safe_id = str(meeting.get("ID", "")).replace("'", "''")
    existing = t_meetings.all(formula=f"{{Meeting ID}}='{safe_id}'")
    if existing:
        m_record = existing[0]
    else:
        m_payload = {
            "Meeting ID": meeting.get("ID"),
            "Committee": meeting.get("MeetingName"),
            "Date": format_airtable_date(meeting.get("StartDate")),
            "Source": meeting.get("Url"),
        }
        m_record = safe_airtable_create(t_meetings, m_payload)
        if not m_record:
            raw_date = format_airtable_date_dateonly(meeting.get("StartDate"))
            if raw_date:
                m_payload["Date"] = raw_date
                m_record = safe_airtable_create(t_meetings, m_payload)
            if not m_record:
                return

    for motion in motions:
        if not motion["for_names"] and not motion["against_names"]:
            continue
        motion_rec = safe_airtable_create(
            t_motions,
            {
                "Meeting": [m_record["id"]],
                "Decision": motion["title"],
                "Outcome": normalize_outcome(motion["result"]),
                "For Count": len(motion["for_names"]),
                "Against Count": len(motion["against_names"]),
            },
        )
        if not motion_rec:
            continue

        for name, vote in [
            *[(n, "Yes") for n in motion["for_names"]],
            *[(n, "No") for n in motion["against_names"]],
        ]:
            councillor_id = get_or_create_councillor(t_councillors, name)
            if not councillor_id:
                continue
            if not dry_run:
                safe_airtable_create(
                    t_votes,
                    {
                        "Motion": [motion_rec["id"]],
                        "Councillor": [councillor_id],
                        "Vote": vote,
                    },
                )


# === MAIN EXECUTION ===
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Scrape Ottawa Council vote records")
    parser.add_argument("--since", help="Scrape meetings starting from YYYY-MM-DD")
    parser.add_argument(
        "--dry-run", action="store_true", help="Parse and log findings, no upload"
    )
    args = parser.parse_args()

    if args.since:
        start_date = datetime.strptime(args.since, "%Y-%m-%d")
    else:
        start_date = datetime.now() - timedelta(days=ROLLING_WINDOW_DAYS)

    end_date = datetime.now()
    start_str, end_str = start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")

    logger.info("Scraping meetings from %s to %s", start_str, end_str)
    meetings = get_meetings(start_str, end_str)
    logger.info("Found %d meetings", len(meetings))

    for meeting in meetings:
        html_links = [
            BASE_URL + d["Url"]
            for d in meeting.get("MeetingDocumentLink", [])
            if d.get("Type") == "PostMinutes"
            and d.get("Format") == "HTML"
            and "English" in d.get("Url", "")
        ]
        for url in html_links:
            r = safe_request("get", url)
            if not r:
                continue
            motions = parse_votes(r.text)
            logger.info(
                "%s: %d motions parsed", meeting.get("MeetingName"), len(motions)
            )
            if not motions:
                continue
            if args.dry_run:
                meeting["_dry_run"] = True
            upload_to_airtable(meeting, motions)

    logger.info("✅ Scraping completed successfully.")
