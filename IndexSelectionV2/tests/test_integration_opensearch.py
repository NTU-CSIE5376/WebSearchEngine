"""The text index against a real OpenSearch: `docker compose -f deploy/compose.opensearch.yaml
build` first, then `pytest -m opensearch`."""

from __future__ import annotations

import json
import socket
import subprocess
import time
import urllib.error
import urllib.request
import uuid
from datetime import UTC, datetime

import pyarrow as pa
import pytest
from conftest import ROOT, replace, small_cfg

from isel.golden import GOLDEN_SCHEMA, golden_path
from isel.io import write_parquet_atomic
from isel.recrawl import TEXT_SCHEMA
from isel.textindex import OpenSearch, build, evaluate_golden, search
from isel.urlnorm import canonicalize, md5_number_lower

pytestmark = pytest.mark.opensearch

IMAGE = "isel-opensearch:3.9.0"
DOCS = [  # url, lang, title, description, body
    ("https://news.example.com/olympics", "en", "Olympic opening ceremony",
     "Tokyo hosts the games", "The opening ceremony of the Olympic Games took place in Tokyo."),
    ("https://sport.example.de/mueller", "de", "Müller trifft für Bayern", None,
     "Thomas Müller erzielte zwei Tore in der Bundesliga."),
    ("https://news.example.jp/kaikaishiki", "ja", "東京オリンピックの開会式", None,
     "東京オリンピックの開会式が国立競技場で行われた。"),
    ("https://weather.example.tw/typhoon", "zh", "交通部中央氣象署", "颱風警報",
     "交通部中央氣象署發布颱風警報，提醒民眾注意安全。"),
    ("https://news.example.in/pradhan", "hi", "धर्मेंद्र प्रधान", None,
     "शिक्षा मंत्री धर्मेंद्र प्रधान ने नई नीति की घोषणा की।"),
]
TWINS = [(f"https://twin{n}.example.org/", "en", "Identical twin page", None,
          "Every twin page carries this same text.") for n in range(4)]


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="module")
def node():
    name = f"isel-os-it-{uuid.uuid4().hex[:8]}"
    port = _free_port()
    subprocess.run(["docker", "run", "-d", "--rm", "--name", name,
                    "-e", "discovery.type=single-node", "-e", "DISABLE_SECURITY_PLUGIN=true",
                    "-e", "DISABLE_INSTALL_DEMO_CONFIG=true",
                    "-e", "OPENSEARCH_JAVA_OPTS=-Xms1g -Xmx1g",
                    "-p", f"127.0.0.1:{port}:9200", IMAGE], check=True, capture_output=True)
    url = f"http://127.0.0.1:{port}"
    try:
        deadline = time.monotonic() + 180
        while True:
            try:
                with urllib.request.urlopen(
                        f"{url}/_cluster/health?wait_for_status=yellow&timeout=5s",
                        timeout=10) as resp:
                    if json.load(resp)["status"] in ("yellow", "green"):
                        break
            except (OSError, urllib.error.URLError, ValueError):
                pass
            if time.monotonic() > deadline:
                raise TimeoutError("OpenSearch did not start")
            time.sleep(2)
        yield url
    finally:
        subprocess.run(["docker", "stop", name], check=False, capture_output=True)


def _text(docs, fetched_at: datetime) -> pa.Table:
    return pa.Table.from_pylist([{
        "doc_id": md5_number_lower(canonicalize(u)), "batch_id": 1, "url": u, "final_url": u,
        "fetched_at": fetched_at, "http_status": 200, "lang": lang, "title": title,
        "description": desc, "body": body, "text_len": len(body), "payload_sha1": "x"}
        for u, lang, title, desc, body in docs], schema=TEXT_SCHEMA)


def _tokens(client: OpenSearch, index: str, text: str, analyzer: str = "text") -> list[str]:
    reply = client.call("POST", f"/{index}/_analyze", {"analyzer": analyzer, "text": text})
    return [t["token"] for t in reply["tokens"]]


def test_build_search_and_eval(node, tmp_path):
    root = tmp_path / "text"
    stale = [(DOCS[0][0], "en", "stale", None, "stale body")]
    write_parquet_atomic(_text(stale, datetime(2026, 10, 1, tzinfo=UTC)),
                         root / "fetch_date=20261001" / "job-1.parquet")
    twin_ids = sorted(md5_number_lower(canonicalize(u)) for u, *_ in TWINS)
    # Loaded in descending doc id order, so load order alone would rank them backwards.
    twins = sorted(TWINS, key=lambda d: -md5_number_lower(canonicalize(d[0])))
    write_parquet_atomic(_text(DOCS + twins, datetime(2026, 10, 20, tzinfo=UTC)),
                         root / "fetch_date=20261020" / "job-2.parquet")
    cfg = replace(small_cfg(tmp_path / "work"), "text_index", url=node, lease_wait_s=90.0,
                  settings_file=str(ROOT / "config/text_index/icu.json"))
    client = OpenSearch(node, 60.0, 2, 1.0)
    first = build(cfg, client, root, datetime(2026, 10, 23, 1, tzinfo=UTC))
    index = first["index"]
    assert first["docs"] == len(DOCS) + len(TWINS) and first["segments"] == 1
    assert first["recovery_source_pruned"] is True
    assert sorted(client.call("GET", "/_alias/isel-text")) == [index]
    flat = client.call("GET", f"/{index}/_settings?flat_settings=true")[index]["settings"]
    assert flat["index.blocks.write"] == "true" and flat["index.codec"] == "best_compression"
    fields = client.call("GET", f"/{index}/_mapping")[index]["mappings"]["properties"]
    assert fields["body"]["index_options"] == "freqs"      # ranks the same, 45 % smaller

    assert len(_tokens(client, index, "東京オリンピックの開会式")) > 2
    # One token: the vowel signs stay with the word. icu_folding drops the virama, so
    # "प्रधान" is indexed as "परधान", the same on the query side.
    assert _tokens(client, index, "प्रधान") == ["परधान"]
    assert _tokens(client, index, "Müller") == ["muller"]
    # Percent escapes vanish whole, so an encoded path leaves no hex fragments.
    assert _tokens(client, index, "https://ja.example.org/wiki/%E6%9D%B1%E4%BA%AC_2020-Games",
                   "url") == ["https", "ja", "example", "org", "wiki", "2020", "games"]

    t = cfg.text_index
    for url, _, title, _, _ in DOCS:
        ids, _ = search(client, t, title)
        assert ids[0] == str(md5_number_lower(canonicalize(url))), title
    ids, _ = search(client, t, "olympics")      # only in the URL; the text says "Olympic"
    assert ids == [str(md5_number_lower(canonicalize(DOCS[0][0])))]
    ids, _ = search(client, t, TWINS[0][2])     # equal scores: the doc id breaks the tie
    assert ids == [str(i) for i in twin_ids]

    golden = [{"batch_id": 20, "batch_created_at": datetime(2026, 10, 16, tzinfo=UTC),
               "query_id": n, "keyword": title, "frequency": 1, "tags": ["head"], "rank": 1,
               "raw_url": url, "canonical_url": canonicalize(url), "host": None}
              for n, (url, _, title, _, _) in enumerate(DOCS)]
    golden.append({**golden[0], "query_id": 99, "keyword": "nothing like this",
                   "tags": ["random"], "raw_url": "https://gone.example/",
                   "canonical_url": "https://gone.example/"})
    write_parquet_atomic(pa.Table.from_pylist(golden, schema=GOLDEN_SCHEMA),
                         golden_path(cfg.work_dir))
    out = evaluate_golden(replace(cfg, "text_index", warmup_queries=0), client, 20)
    assert out["head"]["recall_pairs"] == 1.0 and out["head"]["ndcg"] == 1.0
    assert out["all"]["presence"] == pytest.approx(5 / 6)
    assert out["random"]["recall_pairs"] == 0.0

    fast = replace(cfg, "text_index", lease_wait_s=1.0)
    second = build(fast, client, root, datetime(2026, 10, 23, 2, tzinfo=UTC))
    third = build(fast, client, root, datetime(2026, 10, 23, 3, tzinfo=UTC))
    assert second["previous"] == [index] and third["deleted"] == [index]
    assert sorted(client.call("GET", "/_alias/isel-text")) == [third["index"]]
