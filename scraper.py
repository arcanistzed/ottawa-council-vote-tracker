#!/usr/bin/env python3
"""
Ottawa Council Vote Tracker Scraper

This script scrapes voting records from the Ottawa City Council eScribe system
and stores them in Airtable for tracking and analysis.
"""

from pyairtable import Table
import httpx
from bs4 import BeautifulSoup
import os
from datetime import datetime

AIRTABLE_TOKEN = os.environ.get("AIRTABLE_TOKEN")
BASE_ID = os.environ.get("AIRTABLE_BASE_ID", "appXXXXXXXXXXXXXX")
MEETINGS_TABLE = "Meetings"
MOTIONS_TABLE = "Motions"
VOTES_TABLE = "Votes"

BASE_URL = "https://pub-ottawa.escribemeetings.com/"

def get_meetings(start, end):
    """
    Fetch meetings from the Ottawa eScribe calendar API.
    
    Args:
        start: Start date in YYYY-MM-DD format
        end: End date in YYYY-MM-DD format
        
    Returns:
        List of meeting data from the API
    """
    r = httpx.post(
        f"{BASE_URL}MeetingsCalendarView.aspx/GetCalendarMeetings",
        headers={"Content-Type": "application/json"},
        json={"calendarStartDate": f"{start}T00:00:00-04:00",
              "calendarEndDate": f"{end}T00:00:00-04:00"}
    )
    return r.json()

def parse_votes(html):
    """
    Parse voting data from meeting minutes HTML.
    
    Args:
        html: HTML content of meeting minutes
        
    Returns:
        List of motion dictionaries with voting details
    """
    soup = BeautifulSoup(html, "html.parser")
    motions = []
    for item in soup.select(".AgendaItemContainer"):
        title = item.select_one(".AgendaItemTitle a")
        result = item.select_one(".MotionResult")
        table = item.select_one(".MotionVoters")

        for_names, against_names = [], []
        if table:
            for row in table.select("tr"):
                label = row.select_one(".VoterVote")
                names = row.select_one(".VotesUsers")
                if not label or not names: continue
                if label.text.startswith("For"):
                    for_names = [n.strip().strip(",") for n in names.text.split(",") if n.strip()]
                elif label.text.startswith("Against"):
                    against_names = [n.strip().strip(",") for n in names.text.split(",") if n.strip()]

        motions.append({
            "title": title.text.strip() if title else None,
            "result": result.text.strip() if result else None,
            "for_names": for_names,
            "against_names": against_names,
        })
    return motions

def upload_to_airtable(meeting, motions):
    """
    Upload meeting and motion data to Airtable.
    
    Args:
        meeting: Meeting data dictionary
        motions: List of motion dictionaries
    """
    table_meetings = Table(AIRTABLE_TOKEN, BASE_ID, MEETINGS_TABLE)
    table_motions = Table(AIRTABLE_TOKEN, BASE_ID, MOTIONS_TABLE)
    table_votes = Table(AIRTABLE_TOKEN, BASE_ID, VOTES_TABLE)
    
    m_record = table_meetings.create({
        "Meeting ID": meeting["ID"],
        "Meeting Name": meeting["MeetingName"],
        "Date": meeting["StartDate"],
        "URL": meeting["Url"]
    })

    for motion in motions:
        motion_record = table_motions.create({
            "Meeting": [m_record["id"]],
            "Motion Title": motion["title"],
            "Result": motion["result"],
            "For Count": len(motion["for_names"]),
            "Against Count": len(motion["against_names"]),
        })

        for name in motion["for_names"]:
            table_votes.create({"Motion": [motion_record["id"]], "Councillor": name, "Vote": "For"})
        for name in motion["against_names"]:
            table_votes.create({"Motion": [motion_record["id"]], "Councillor": name, "Vote": "Against"})

if __name__ == "__main__":
    # Get date range from environment or use current month
    start_date = os.environ.get("START_DATE", datetime.now().strftime("%Y-%m-01"))
    end_date = os.environ.get("END_DATE", datetime.now().strftime("%Y-%m-%d"))
    
    print(f"Scraping meetings from {start_date} to {end_date}")
    
    meetings = get_meetings(start_date, end_date)
    for meeting in meetings:
        html_links = [BASE_URL + d["Url"]
                      for d in meeting["MeetingDocumentLink"]
                      if d["Type"] == "PostMinutes" and d["Format"] == "HTML" and "English" in d["Url"]]
        for url in html_links:
            print(f"Processing: {url}")
            html = httpx.get(url).text
            motions = parse_votes(html)
            if motions:
                print(f"Found {len(motions)} motions, uploading to Airtable...")
                upload_to_airtable(meeting, motions)
    
    print("Scraping completed successfully!")
