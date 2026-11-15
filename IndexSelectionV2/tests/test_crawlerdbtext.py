from __future__ import annotations

import re
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from conftest import replace, small_cfg

from isel.crawlerdbtext import CRAWLERDB_TEXT_SCHEMA, pull_crawlerdb_text
from isel.io import open_duckdb, write_parquet_atomic
from isel.pg import ReadOnlySession, assert_read_only_sql
from isel.recrawl import TEXT_SCHEMA
from isel.textindex import TextIndexError, latest_docs
from isel.urlnorm import DomainKeyer, ShardRouter, md5_number_lower

SHARDS = 4
CHALLENGE_SHA1 = "c" * 40
SPLIT_HOST = "news.split.com"
REAL = "https://real.example/page"
CHALLENGED = "https://www.cosmo.example/horoscope-leo"
EMPTY = "https://waf.example/empty"
VINEGARY = "https://www.merriam-webster.com/dictionary/vinegary"
OFF_LIST = "https://off.example/"
LISTED = [REAL, CHALLENGED, EMPTY, VINEGARY]


class CrawlerDb:
    """A stand-in for a crawlerdb connection: `tables` maps a shard table to its rows by URL."""

    def __init__(self, tables: dict[str, dict[str, tuple[str | None, str | None]]]) -> None:
        self.tables = tables
        self.executed: list[tuple[str, list[str]]] = []

    def cursor(self):
        db = self

        class Cursor:
            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False

            def execute(self, text, params=None):
                (urls,) = params
                db.executed.append((text, urls))
                table = db.tables.get(re.search(r"FROM (\w+)", text)[1], {})
                self._rows = [(u, *table[u]) for u in urls if u in table]

            def fetchall(self):
                return self._rows

        return Cursor()


def _router() -> ShardRouter:
    return ShardRouter(DomainKeyer([SPLIT_HOST], offline=True), SHARDS, {})


def _table(url: str, candidate: int = 0) -> str:
    return f"url_state_current_{_router().candidate_shards(url)[candidate]:03d}"


def _doc_id(url: str) -> int:
    return md5_number_lower(url)


def _page(url: str, title: str | None, body: str, payload_sha1: str = "x") -> dict:
    return {"doc_id": _doc_id(url), "batch_id": 1, "url": url, "final_url": None,
            "fetched_at": datetime(2026, 10, 20, tzinfo=UTC), "http_status": 200, "lang": "en",
            "title": title, "description": None, "body": body, "text_len": len(body),
            "payload_sha1": payload_sha1}


def _list(path: Path, urls: list[str]) -> str:
    write_parquet_atomic(pa.table({"doc_id": pa.array([_doc_id(u) for u in urls], pa.uint64()),
                                   "rank": pa.array(range(len(urls)), pa.int64()),
                                   "url": urls}), path)
    return str(path)


@pytest.fixture
def cfg(tmp_path):
    """REAL has a page, CHALLENGED a challenge page, EMPTY an empty one and VINEGARY none."""
    write_parquet_atomic(pa.Table.from_pylist([
        _page(REAL, "Real page", "real text"), _page(EMPTY, None, ""),
        _page(CHALLENGED, "Client Challenge", "Enable JavaScript.", CHALLENGE_SHA1),
        _page(OFF_LIST, None, "")], schema=TEXT_SCHEMA), tmp_path / "text" / "job-1.parquet")
    cfg = replace(small_cfg(tmp_path / "work"), "recrawl", challenge_sha1=[CHALLENGE_SHA1])
    return replace(cfg, "text_index", prior_file=_list(tmp_path / "list" / "s.parquet", LISTED),
                   crawlerdb_text_root=str(tmp_path / "crawlerdb_text"))


def _pull(cfg, db: CrawlerDb, tmp_path: Path, **kw) -> dict:
    return pull_crawlerdb_text(cfg, ReadOnlySession(db), _router(), tmp_path / "text", **kw)


def _dataset(cfg) -> dict[int, tuple[str | None, str | None]]:
    files = sorted(Path(cfg.text_index.crawlerdb_text_root).glob("*.parquet"))
    table = pa.concat_tables([pq.read_table(f) for f in files])
    assert table.schema.equals(CRAWLERDB_TEXT_SCHEMA)
    assert table.num_rows == len(set(table["doc_id"].to_pylist()))
    return {r["doc_id"]: (r["title"], r["anchor"]) for r in table.to_pylist()}


def _crawlerdb(rows: dict[str, tuple[str | None, str | None]]) -> CrawlerDb:
    """A crawlerdb that holds (title, anchor text) for each URL, in the URL's first shard."""
    tables: dict[str, dict] = {}
    for url, text in rows.items():
        tables.setdefault(_table(url), {})[url] = text
    return CrawlerDb(tables)


def _everything(*urls: str) -> CrawlerDb:
    return _crawlerdb({url: (f"title of {url}", f"anchor of {url}") for url in urls})


def test_the_docs_without_text_are_looked_up_and_never_a_doc_with_text(cfg, tmp_path):
    db = _everything(*LISTED, OFF_LIST)
    report = _pull(replace(cfg, "text_index", crawlerdb_text_chunk=1), db, tmp_path)
    # One URL per query here, so the queries show the order: best list rank first.
    assert [urls for _, urls in db.executed] == [[CHALLENGED], [EMPTY], [VINEGARY]]
    assert _dataset(cfg) == {_doc_id(u): (f"title of {u}", f"anchor of {u}")
                             for u in (CHALLENGED, EMPTY, VINEGARY)}
    assert {k: report[k] for k in ("docs_wanted", "docs_found", "with_title", "with_anchor",
                                   "queries", "stopped")} == {
        "docs_wanted": 3, "docs_found": 3, "with_title": 3, "with_anchor": 3, "queries": 3,
        "stopped": False}
    # What the build leaves without text is what was asked for.
    *_, reader = latest_docs(open_duckdb(cfg, tmp_path / "tmp"), tmp_path / "text", 10,
                             cfg.text_index.prior_file, [CHALLENGE_SHA1])
    textless = {r["doc_id"] for r in reader.read_all().to_pylist()
                if not (r["title"] or r["description"] or r["body"])}
    assert textless == set(_dataset(cfg))


def test_every_statement_is_one_read_of_a_shard_table_by_url(cfg, tmp_path):
    db = _everything(*LISTED)
    _pull(cfg, db, tmp_path)
    assert db.executed
    for text, urls in db.executed:
        assert_read_only_sql(text)
        assert re.fullmatch(r"SELECT url, title, anchor_text FROM url_state_current_\d{3} "
                            r"WHERE url = ANY\(%s\)", text)
        assert f"url_state_current_{_router().candidate_shards(urls[0])[0]:03d}" in text


def test_empty_text_is_left_out_and_the_anchor_is_cut(cfg, tmp_path):
    db = _crawlerdb({CHALLENGED: ("  ", "x" * 500), EMPTY: ("", None),
                     VINEGARY: (" VINEGARY Definition ", "")})
    report = _pull(replace(cfg, "text_index", crawlerdb_anchor_chars=7), db, tmp_path)
    assert _dataset(cfg) == {_doc_id(CHALLENGED): (None, "xxxxxxx"),
                             _doc_id(VINEGARY): ("VINEGARY Definition", None)}
    assert (report["docs_wanted"], report["docs_found"], report["with_title"],
            report["with_anchor"]) == (3, 2, 1, 1)


def test_a_url_is_looked_up_in_both_of_its_candidate_shards(cfg, tmp_path):
    migrated, waiting, both, missing = (f"https://{SPLIT_HOST}/{name}"
                                        for name in ("migrated", "waiting", "both", "missing"))
    assert len(_router().candidate_shards(migrated)) == 2
    cfg = replace(cfg, "text_index", prior_file=_list(
        tmp_path / "list" / "split.parquet", [migrated, waiting, both, missing]))
    db = CrawlerDb({_table(migrated, 0): {migrated: ("first", None), both: ("one", None)},
                    _table(migrated, 1): {waiting: ("second", None), both: ("two", None)}})
    report = _pull(cfg, db, tmp_path)
    found = _dataset(cfg)
    assert found[_doc_id(migrated)] == ("first", None)
    assert found[_doc_id(waiting)] == ("second", None)
    assert found[_doc_id(both)] in (("one", None), ("two", None))
    assert _doc_id(missing) not in found
    assert (report["docs_wanted"], report["docs_found"], report["queries"]) == (4, 3, 2)


def test_a_rerun_replaces_the_dataset(cfg, tmp_path):
    small = replace(cfg, "text_index", crawlerdb_text_file_rows=1, crawlerdb_text_chunk=1)
    first = _pull(small, _everything(*LISTED), tmp_path)
    root = Path(cfg.text_index.crawlerdb_text_root)
    before = {f.name: pq.read_table(f) for f in root.iterdir()}
    assert first["files"] == len(before) == 3
    again = _pull(small, _everything(*LISTED), tmp_path)
    assert {f.name: pq.read_table(f) for f in root.iterdir()} == before
    assert {**again, "seconds": 0} == {**first, "seconds": 0}
    # crawlerdb lost two of the pages: their rows go, and no older file stays.
    _pull(small, _everything(VINEGARY), tmp_path)
    assert [f.name for f in root.iterdir()] == ["part-00000.parquet"]
    assert set(_dataset(cfg)) == {_doc_id(VINEGARY)}
    assert not list(root.parent.glob("crawlerdb_text.*"))
    # And none at all: the dataset is empty, which a build reads as no dataset.
    assert _pull(small, CrawlerDb({}), tmp_path)["files"] == 0
    assert list(root.iterdir()) == []


def test_a_run_past_its_time_stops_and_leaves_what_it_found(cfg, tmp_path):
    seconds = iter(range(0, 10_000, 40))
    db = _everything(*LISTED)
    timed = replace(cfg, "text_index", crawlerdb_text_chunk=1, crawlerdb_text_max_minutes=1.0)
    report = _pull(timed, db, tmp_path, clock=lambda: next(seconds))
    # The clock reads 0 at the start, 40 before the first lookup and 80 before the second.
    assert [urls for _, urls in db.executed] == [[CHALLENGED]]
    assert (report["stopped"], report["docs_wanted"], report["docs_found"]) == (True, 3, 1)
    assert set(_dataset(cfg)) == {_doc_id(CHALLENGED)}
    *counts, reader = latest_docs(open_duckdb(cfg, tmp_path / "tmp"), tmp_path / "text", 10,
                                  cfg.text_index.prior_file, [CHALLENGE_SHA1],
                                  Path(cfg.text_index.crawlerdb_text_root), 100)
    assert counts[-1] == 1 and reader.read_all().num_rows == len(LISTED)


def test_a_failed_run_keeps_the_previous_dataset(cfg, tmp_path):
    _pull(cfg, _everything(*LISTED), tmp_path)
    before = _dataset(cfg)

    class Down(CrawlerDb):
        def cursor(self):
            raise ConnectionError("crawlerdb is down")

    with pytest.raises(ConnectionError):
        _pull(cfg, Down({}), tmp_path)
    assert _dataset(cfg) == before


@pytest.mark.parametrize(("option", "message"), [
    ("prior_file", "needs the list"), ("crawlerdb_text_root", "crawlerdb_text_root is not set")])
def test_the_lookup_needs_a_list_and_a_root(cfg, tmp_path, option, message):
    db = _everything(*LISTED)
    with pytest.raises(TextIndexError, match=message):
        _pull(replace(cfg, "text_index", **{option: ""}), db, tmp_path)
    assert db.executed == []
