<!-- Copied/merged guidance for AI coding agents working on this repo. Keep concise and actionable. -->

# Guidance for AI code contributors

This repository is a small, single-purpose scraper that extracts Ottawa City Council voting records from the public eScribe site and writes them to Airtable.

Use the concrete pointers below to be productive quickly.

- Project entry point: `scraper.py` — one executable script that performs the full workflow: fetch meetings -> find PostMinutes HTML -> parse motions/votes -> upload to Airtable.
- Dependencies are pinned in `requirements.txt` (beautifulsoup4, httpx, lxml, pyairtable). Use a virtualenv and `pip install -r requirements.txt` before running.

## Key architecture and data flow

- `get_meetings(start, end)` posts to `https://pub-ottawa.escribemeetings.com/MeetingsCalendarView.aspx/GetCalendarMeetings` and returns JSON meeting objects.

- For each meeting, the script looks for `MeetingDocumentLink` entries with `Type == "PostMinutes"`, `Format == "HTML"`, and English links. It downloads those HTML pages.

- `parse_votes(html)` uses BeautifulSoup and the following CSS structure to extract motions and votes:

  - `.AgendaItemContainer` — container per agenda item
  - `.AgendaItemTitle a` — motion title (may be missing)
  - `.MotionResult` — vote result text
  - `.MotionVoters` table rows with `.VoterVote` (label like "For"/"Against") and `.VotesUsers` (comma-separated names)

- `upload_to_airtable(meeting, motions)` uses `pyairtable.Table` to create records in three tables: `Meetings`, `Motions`, `Votes`. Fields written (examples):

  - Meetings: `Meeting ID`, `Meeting Name`, `Date`, `URL`
  - Motions: `Meeting` (link to Meeting record), `Motion Title`, `Result`, `For Count`, `Against Count`
  - Votes: `Motion` (link), `Councillor`, `Vote` (string: "For"/"Against")

Environment and runtime notes

- Required environment variables:
  - `AIRTABLE_TOKEN` (must be set for Airtable writes)
  - `AIRTABLE_BASE_ID` (defaults to `appXXXXXXXXXXXXXX` placeholder in code — replace with your base id)
  - `START_DATE` and `END_DATE` optional (YYYY-MM-DD). If not set, script uses the first of current month through today.
- Run locally:
  - Create and activate a venv, install requirements, then: `python3 scraper.py` (with env vars exported in shell).

Project-specific conventions and caveats (discoverable in code)

- Single-script design: there is no webserver or package structure. Changes should preserve simple CLI behavior or explicitly refactor into modules with tests.
- Airtable writes are optimistic and always create new records; there is no deduplication or idempotency. If you add features that re-run the scraper, include dedupe logic (e.g., lookup by `Meeting ID` before create).
- HTML parsing relies on the eScribe CSS classes named above. If scraping breaks, first inspect the downloaded HTML (print or save) and check for changed class names or layout.
- Error handling is minimal: network failures or missing env vars will raise exceptions. When adding robustness, prefer explicit, narrow exception handling and retry (for httpx) rather than broad try/except.

Debugging and developer workflow

- Install deps: `python3 -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt`.
- Quick run (example):
  - `AIRTABLE_TOKEN=... AIRTABLE_BASE_ID=appABC START_DATE=2025-10-01 END_DATE=2025-10-26 python3 scraper.py`
- To debug parsing, add temporary prints or write HTML to a local file before `parse_votes()` and open in a browser.

Files to inspect when changing behavior

- `scraper.py` — canonical logic to read/modify.
- `requirements.txt` — pinned dependencies for reproducibility.

What not to change without CI/tests

- Avoid large refactors that change data shapes (Airtable field names or record linking) without adding migration/detection code.

If something is missing in this guidance or you need examples, ask for the parts to expand (tests, CI, idempotency). After edits, run the script locally with a small date range to verify scraping and Airtable interactions.
