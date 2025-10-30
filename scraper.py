#!/usr/bin/env python3
"""
Ottawa Council Vote Tracker Scraper
Scrapes voting records from the Ottawa City Council eScribe system
and stores divided vote records in Airtable.
"""

import logging
import os
import random
import re
import sys
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List

import httpx
from bs4 import BeautifulSoup
from dateutil import parser as date_parser, tz as date_tz
from pyairtable import Api

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
VERIFY_SSL = False  # eScribe’s certificate is invalid

councillor_cache: Dict[str, str] = {}

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


# === HTTP HELPERS ===
def safe_request(method, url, json=None, max_retries=3, timeout=30):
    for i in range(max_retries):
        try:
            r = httpx.request(
                method, url, json=json, timeout=timeout, verify=VERIFY_SSL
            )
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
        return (data.get("d") if isinstance(data, dict) else data) or []
    except Exception as e:
        logger.warning("Invalid JSON from eScribe API: %s", e)
        return []


# === PARSING ===
def parse_votes(html: str) -> List[Dict[str, Any]]:
    """Extract motions and votes from a meeting minutes HTML."""
    soup = BeautifulSoup(html, "html.parser")
    motions = []
    for item in soup.select(".AgendaItemContainer"):
        title = item.select_one(".AgendaItemTitle a")
        result = item.select_one(".MotionResult")
        table = item.select_one("table.MotionVoters")
        if not table:
            continue

        for_names, against_names = [], []
        for row in table.select("tr"):
            label, names_el = row.select_one(".VoterVote"), row.select_one(
                ".VotesUsers"
            )
            if not label or not names_el:
                continue

            names = re.sub(
                r"\band\b", ",", names_el.get_text(" ", strip=True), flags=re.I
            )
            names = [
                re.sub(r"\s+", " ", n.strip(" ,\u00a0"))
                for n in re.split(r"[;,]", names)
                if n.strip()
            ]

            if "for" in label.text.lower():
                for_names += names
            elif "against" in label.text.lower():
                against_names += names

        if for_names or against_names:
            motions.append(
                {
                    "title": title.text.strip() if title else None,
                    "result": result.text.strip() if result else None,
                    "for_names": for_names,
                    "against_names": against_names,
                }
            )
    return motions


# === AIRTABLE HELPERS ===
def safe_airtable_create(table, payload, max_retries=3):
    for i in range(max_retries):
        try:
            return table.create(payload)
        except Exception as e:
            msg = str(e)
            if "422" in msg:
                logger.error(
                    "422: invalid payload → %s | %s",
                    getattr(table, "name", "<table>"),
                    payload,
                )
                break
            time.sleep(min(2**i, 8) + random.random())
    return None


def clear_airtable(api: Api, force: bool = False):
    """Delete all records from all known tables in the configured base."""
    if not (AIRTABLE_TOKEN and BASE_ID):
        logger.error("Airtable not configured.")
        return

    tables = [MEETINGS_TABLE, MOTIONS_TABLE, VOTES_TABLE, COUNCILLORS_TABLE]
    if not force:
        logger.warning(
            "You are about to permanently delete ALL records in base %s", BASE_ID
        )
        confirm = input("Type DELETE to confirm: ").strip()
        if confirm != "DELETE":
            logger.info("Aborting clear operation.")
            return

    for name in tables:
        t = api.table(BASE_ID, name)
        try:
            recs = t.all()
            if not recs:
                logger.info("No records in %s.", name)
                continue
            ids = [r["id"] for r in recs if "id" in r]
            logger.info("Deleting %d records from %s...", len(ids), name)
            for i in range(0, len(ids), 10):
                batch = ids[i : i + 10]
                try:
                    t.batch_delete(batch)
                    time.sleep(0.2)
                except Exception as e:
                    logger.warning("Batch delete failed for %s: %s", name, e)
            logger.info("Cleared table %s", name)
        except Exception as e:
            logger.error("Failed to clear %s: %s", name, e)


def get_or_create_councillor(t_councillors, name: str) -> str | None:
    """Ensure councillor record exists, using normalized last name as key."""
    if not name:
        return None
    last = name.strip().split()[-1].lower()

    if not councillor_cache:
        for rec in t_councillors.all():
            val = rec["fields"].get("Councillor", "").strip()
            if val:
                councillor_cache[val.split()[-1].lower()] = rec["id"]

    if last in councillor_cache:
        return councillor_cache[last]

    rec = safe_airtable_create(t_councillors, {"Councillor": name.strip()})
    if rec:
        councillor_cache[last] = rec["id"]
        logger.info("Created councillor: %s", name.strip())
        return rec["id"]
    return None


def upload_to_airtable(meeting, motions, dry_run=False):
    motions = [m for m in motions if m["for_names"] or m["against_names"]]
    logger.info(
        "%s: %d divided motions found", meeting.get("MeetingName"), len(motions)
    )
    if not motions:
        logger.info("No divided votes for %s; skipping.", meeting.get("MeetingName"))
        return

    api = Api(AIRTABLE_TOKEN)
    t_meetings = api.table(BASE_ID, MEETINGS_TABLE)
    t_motions = api.table(BASE_ID, MOTIONS_TABLE)
    t_votes = api.table(BASE_ID, VOTES_TABLE)
    t_councillors = api.table(BASE_ID, COUNCILLORS_TABLE)

    mid = meeting.get("ID", "").replace("'", "''")
    existing = t_meetings.all(formula=f"{{Meeting ID}}='{mid}'")
    if existing:
        m_rec = existing[0]
    else:
        dt = date_parser.parse(meeting.get("StartDate")).astimezone(
            date_tz.gettz(PREFERRED_TZ)
        )
        m_rec = safe_airtable_create(
            t_meetings,
            {
                "Meeting ID": meeting.get("ID"),
                "Committee": meeting.get("MeetingName"),
                "Date": dt.isoformat(),
                "Source": f"{BASE_URL.rstrip('/')}/Meeting.aspx?Id={meeting.get('ID')}&Agenda=PostMinutes",
            },
        )
    if not m_rec:
        return

    for motion in motions:
        res = (motion.get("result") or "").lower()
        if any(k in res for k in ["carried", "passed", "adopted"]):
            outcome = "Passed"
        elif any(k in res for k in ["lost", "failed", "not carried"]):
            outcome = "Failed"
        else:
            outcome = motion.get("result")

        mot = safe_airtable_create(
            t_motions,
            {
                "Meeting": [m_rec["id"]],
                "Decision": motion["title"],
                "Outcome": outcome,
                "For Count": len(motion["for_names"]),
                "Against Count": len(motion["against_names"]),
            },
        )
        if not mot:
            continue

        for name, vote in [(n, "Yes") for n in motion["for_names"]] + [
            (n, "No") for n in motion["against_names"]
        ]:
            cid = get_or_create_councillor(t_councillors, name)
            if cid and not dry_run:
                safe_airtable_create(
                    t_votes, {"Motion": [mot["id"]], "Councillor": [cid], "Vote": vote}
                )


# === MAIN ===
if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser(description="Scrape Ottawa Council vote records")
    p.add_argument("--since", help="Scrape meetings starting from YYYY-MM-DD")
    p.add_argument("--dry-run", action="store_true", help="Parse only, no upload")
    p.add_argument("--clear", action="store_true", help="Delete all Airtable records")
    p.add_argument("--yes", action="store_true", help="Skip confirmation prompts")
    a = p.parse_args()

    if a.clear:
        api = Api(AIRTABLE_TOKEN)
        clear_airtable(api, force=a.yes)
        sys.exit(0)

    if not a.dry_run and (not AIRTABLE_TOKEN or not BASE_ID):
        logger.error("Missing AIRTABLE_TOKEN or AIRTABLE_BASE_ID.")
        sys.exit(1)

    start = (
        datetime.strptime(a.since, "%Y-%m-%d")
        if a.since
        else datetime.now() - timedelta(days=ROLLING_WINDOW_DAYS)
    )
    end = datetime.now()
    logger.info("Scraping meetings from %s to %s", start.date(), end.date())

    for m in get_meetings(start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")):
        for doc in m.get("MeetingDocumentLink", []):
            if (
                doc.get("Type") == "PostMinutes"
                and doc.get("Format") == "HTML"
                and "English" in doc.get("Url", "")
            ):
                r = safe_request("get", BASE_URL + doc["Url"])
                if r:
                    motions = parse_votes(r.text)
                    logger.info(
                        "%s: %d motions parsed", m.get("MeetingName"), len(motions)
                    )
                    upload_to_airtable(m, motions, dry_run=a.dry_run)

    logger.info("Scraping completed successfully.")
