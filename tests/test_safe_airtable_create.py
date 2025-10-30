import pytest

import scraper


class FlakyTable:
    def __init__(self, fail_times=2):
        self.calls = 0
        self.fail_times = fail_times

    def create(self, payload):
        self.calls += 1
        if self.calls <= self.fail_times:
            raise RuntimeError("transient error")
        return {"id": "rec_ok"}


def test_safe_airtable_create_retries_and_succeeds():
    t = FlakyTable(fail_times=2)
    rec = scraper.safe_airtable_create(t, {"foo": "bar"}, max_retries=4)
    assert rec is not None and rec.get("id") == "rec_ok"


def test_safe_airtable_create_gives_up():
    t = FlakyTable(fail_times=5)
    rec = scraper.safe_airtable_create(t, {"foo": "bar"}, max_retries=3)
    assert rec is None

def test_safe_airtable_create_retries(monkeypatch):
    calls = []
    def flaky_create(payload):
        calls.append(payload)
        if len(calls) < 3:
            raise RuntimeError("temporary failure")
        return {"id": "rec_ok"}

    class FakeTable:
        def create(self, payload): return flaky_create(payload)

    rec = scraper.safe_airtable_create(FakeTable(), {"foo": "bar"}, max_retries=5)
    assert rec["id"] == "rec_ok"
    assert len(calls) == 3  # retried twice, then succeeded
