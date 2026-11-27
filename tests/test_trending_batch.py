"""A trending fetch that fails for any geo saves no batch. On 11-16 every geo failed and
batch 22 was saved with 0 queries; the 12:00 and 18:00 runs then measured nothing."""
from typing import ClassVar

import pytest
from Metric.Query.SerpApi import SerpApiError
from Metric.RawDataReader import DatabaseRawDataReader as reader_module
from Metric.RawDataReader.DatabaseRawDataReader import DatabaseRawDataReader


class FakeSearch:
    """Stands in for serpapi.GoogleSearch; `answers[geo]` is the dict SerpApi returns."""
    answers: ClassVar[dict] = {}
    calls: ClassVar[list] = []

    def __init__(self, params):
        self.params = params

    def get_dict(self):
        FakeSearch.calls.append(self.params["geo"])
        return FakeSearch.answers[self.params["geo"]]


@pytest.fixture
def reader(monkeypatch):
    monkeypatch.setenv("SERPAPI_KEY", "k")
    monkeypatch.setattr(reader_module, "GoogleSearch", FakeSearch)
    monkeypatch.setattr(reader_module.time, "sleep", lambda s: None)
    FakeSearch.calls = []
    r = DatabaseRawDataReader(db=None, modelFactory=None)
    r.saved = []
    monkeypatch.setattr(r, "_save_to_database", r.saved.append)
    return r


def trending(*keywords):
    return {"trending_searches": [
        {"query": k, "search_volume": 100, "start_timestamp": 1} for k in keywords]}


def test_every_geo_answers_and_the_batch_is_saved(reader):
    FakeSearch.answers = {c["geo"]: trending(f"kw-{c['geo']}", "shared") for c in reader.countries}
    data = reader._fetch_from_api_process_and_store()
    assert len(reader.saved) == 1
    assert len(data) == len(reader.countries) + 1
    shared = next(d for d in data if d["keyword"] == "shared")
    assert len(shared["geo"]) == len(reader.countries)


def test_a_geo_that_keeps_failing_saves_no_batch(reader):
    FakeSearch.answers = {c["geo"]: trending("kw") for c in reader.countries}
    FakeSearch.answers["JP"] = {"error": "Invalid API key."}
    with pytest.raises(SerpApiError, match="JP"):
        reader._fetch_from_api_process_and_store()
    assert reader.saved == []
    assert FakeSearch.calls.count("JP") == 4  # 1 try + 3 retries


def test_a_geo_with_no_trends_is_not_a_failure(reader):
    FakeSearch.answers = {c["geo"]: trending("kw") for c in reader.countries}
    FakeSearch.answers["TW"] = {"trending_searches": []}
    reader._fetch_from_api_process_and_store()
    assert len(reader.saved) == 1


def test_a_missing_key_fails_before_any_request(reader, monkeypatch):
    monkeypatch.delenv("SERPAPI_KEY")
    with pytest.raises(SerpApiError, match="not set"):
        reader._fetch_from_api_process_and_store()
    assert FakeSearch.calls == []
    assert reader.saved == []
