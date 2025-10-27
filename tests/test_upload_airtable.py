import pytest
from pathlib import Path

import sys
from pathlib import Path as _Path

# ensure repo root is on sys.path so tests can import scraper.py directly
sys.path.insert(0, str(_Path(__file__).resolve().parents[1]))

import scraper


def make_tables(meetings_all):
    class FakeTable:
        def __init__(self, name):
            self.name = name
            self.created = []
            self._meetings_all = meetings_all if name == "Meetings" else []

        def create(self, payload):
            # return a dict with an id to mimic pyairtable
            rec = {"id": f"{self.name}_rec_{len(self.created)+1}"}
            self.created.append(payload)
            return rec

        def all(self, formula=None):
            return self._meetings_all

    return (
        FakeTable("Meetings"),
        FakeTable("Motions"),
        FakeTable("Votes"),
        FakeTable("Councillor"),
    )


def test_upload_skips_existing_meeting(monkeypatch):
    # prepare
    html = Path("tests/fixtures/sample_minutes.html").read_text(encoding="utf-8")
    motions = scraper.parse_votes(html)

    meetings_table, motions_table, votes_table, councillors_table = make_tables(
        [{"id": "m_exist"}]
    )

    # New pyairtable.Api interface: Api(token).table(base, name)
    class FakeApi:
        def __init__(self, token):
            self.token = token

        def table(self, base, name):
            if name == "Meetings":
                return meetings_table
            if name == "Motions":
                return motions_table
            if name == "Votes":
                return votes_table
            if name == scraper.COUNCILLORS_TABLE or name == "Councillor":
                return councillors_table

    monkeypatch.setattr(scraper, "Api", FakeApi)
    # ensure env tokens present so upload runs
    scraper.AIRTABLE_TOKEN = "x"
    scraper.BASE_ID = "b"

    meeting = {
        "ID": "M1",
        "MeetingName": "Test",
        "StartDate": "2025-10-01",
        "Url": "http://x",
    }
    scraper.upload_to_airtable(meeting, motions)

    # meeting should not have been created
    assert len(meetings_table.created) == 0
    # one motion created
    assert len(motions_table.created) == 1
    # votes created equal to parsed counts
    expected_votes = len(motions[0]["for_names"]) + len(motions[0]["against_names"])
    assert len(votes_table.created) == expected_votes


def test_upload_creates_meeting_when_missing(monkeypatch):
    html = Path("tests/fixtures/sample_minutes.html").read_text(encoding="utf-8")
    motions = scraper.parse_votes(html)

    meetings_table, motions_table, votes_table, councillors_table = make_tables([])

    class FakeApi:
        def __init__(self, token):
            self.token = token

        def table(self, base, name):
            if name == "Meetings":
                return meetings_table
            if name == "Motions":
                return motions_table
            if name == "Votes":
                return votes_table
            if name == scraper.COUNCILLORS_TABLE or name == "Councillor":
                return councillors_table

    monkeypatch.setattr(scraper, "Api", FakeApi)
    scraper.AIRTABLE_TOKEN = "x"
    scraper.BASE_ID = "b"

    meeting = {
        "ID": "M2",
        "MeetingName": "Test2",
        "StartDate": "2025-10-02",
        "Url": "http://x",
    }
    scraper.upload_to_airtable(meeting, motions)

    # meeting should have been created once
    assert len(meetings_table.created) == 1
    # motion and votes created
    assert len(motions_table.created) == 1
    expected_votes = len(motions[0]["for_names"]) + len(motions[0]["against_names"])
    assert len(votes_table.created) == expected_votes
