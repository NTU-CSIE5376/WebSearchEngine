"""
CrawlerAllMetricMeasure 的 golden URL 比對測試

不連資料庫：metricDB 用假的 session，三個掃描 crawlerdb / selectdb 的方法換成
在記憶體裡做「字串完全相等」比對，和 SQL 的 url IN (...) / url = ANY(...) 行為相同。

執行 (repo 根目錄)：python3 -m unittest discover -s tests -t . -v
"""
import io
import unittest
from contextlib import contextmanager, redirect_stdout
from types import SimpleNamespace
from unittest import mock

import tldextract
from w3lib.url import canonicalize_url

from Database.CrawlerModels import Base as CrawlerBase
from Database.MetricModels import Base as MetricBase
from Database.ModelFactory.AppModelFactory import AppModelFactory
from Metric.Measure.CrawlerAllMetricMeasure import CrawlerAllMetricMeasure

# 動態 model 掛在同一個 declarative base 上，重複建立會衝突，所以共用一個 factory
MODEL_FACTORY = AppModelFactory(CrawlerBase, MetricBase)


class FakeResult:
    def __init__(self, rows):
        self._rows = rows

    def scalars(self):
        return self

    def all(self):
        return self._rows


class FakeMetricSession:
    def __init__(self, golden_rows):
        self.golden_rows = golden_rows
        self.updates = []

    def execute(self, stmt):
        # select 是讀 golden URL；insert 是寫 coverage 表，這裡不檢查
        if stmt.is_select:
            return FakeResult(self.golden_rows)
        return None

    def bulk_update_mappings(self, model, mappings):
        self.updates.extend(mappings)

    def commit(self):
        pass


class FakeDB:
    def __init__(self, session):
        self._session = session

    @contextmanager
    def session(self):
        yield self._session


def run_measure(golden, crawler_rows, selected_urls):
    """
    golden: [(metric_url.id, SerpApi 原始 URL)]
    crawler_rows: {crawlerdb 裡的 url: (crawled, shard_id)}
    selected_urls: selectdb.selected_urls_current 裡的 url
    回傳 ({metric_url.id: 更新內容}, 統計, 掃描時傳入的 url tuple)
    """
    metric_session = FakeMetricSession([SimpleNamespace(id=i, url=u) for i, u in golden])
    measure = CrawlerAllMetricMeasure(
        MODEL_FACTORY,
        crawlerDB=None,
        metricDB=FakeDB(metric_session),
        selectDB=object(),
        batch_id=1,
        tag="head",
    )
    # 用內建的 suffix list，避免測試時連網下載
    measure.extractor = tldextract.TLDExtract(cache_dir=False, suffix_list_urls=())

    scanned = []

    def scan_url_shard(shard_ids, url_tuple):
        scanned.append(url_tuple)
        return [
            (url, crawled, False, shard_id)
            for url, (crawled, shard_id) in crawler_rows.items()
            if url in url_tuple and shard_id in shard_ids
        ]

    stats = {}
    with mock.patch.object(measure, "_scan_url_shard", side_effect=scan_url_shard), \
         mock.patch.object(measure, "_scan_domain_shard", return_value={}), \
         mock.patch.object(measure, "_load_indexed_urls",
                           side_effect=lambda url_tuple: set(selected_urls) & set(url_tuple)), \
         mock.patch.object(measure, "_print_report", side_effect=stats.update), \
         redirect_stdout(io.StringIO()):
        measure.test()

    updates = {u["id"]: u for u in metric_session.updates}
    return updates, stats, scanned


class TestGoldenUrlCanonicalization(unittest.TestCase):

    def test_raw_golden_url_matches_canonical_crawler_row(self):
        # crawler 和 IndexSelection 存的是 canonicalize_url 之後的 URL
        cases = {
            "query parameter order": "https://example.com/p?b=2&a=1",
            "%27 vs '": "https://example.com/it%27s?q=a'b",
            "escaping of , and = in query values": "https://example.com/s?q=a,b&r=x=y",
        }
        for name, raw in cases.items():
            with self.subTest(name):
                canonical = canonicalize_url(raw)
                self.assertNotEqual(raw, canonical)

                updates, _, scanned = run_measure(
                    golden=[(1, raw)],
                    crawler_rows={canonical: (True, 5)},
                    selected_urls={canonical},
                )

                self.assertTrue(all(url_tuple == (canonical,) for url_tuple in scanned))
                self.assertEqual(updates[1], {
                    "id": 1,
                    "is_discovered": True,
                    "is_crawled": True,
                    "is_indexed": True,
                    "shard_id": 5,
                })

    def test_raw_variants_share_one_status(self):
        # 兩個原始寫法 canonicalize 後相同：只算一個 URL，兩個 metric_url id 都更新
        canonical = "https://example.com/p?a=1&b=2"
        updates, stats, _ = run_measure(
            golden=[(1, "https://example.com/p?b=2&a=1"), (2, canonical)],
            crawler_rows={canonical: (True, 200)},
            selected_urls=set(),
        )

        self.assertEqual(set(updates), {1, 2})
        for m_id in (1, 2):
            self.assertTrue(updates[m_id]["is_discovered"])
            self.assertTrue(updates[m_id]["is_crawled"])
            self.assertFalse(updates[m_id]["is_indexed"])
            self.assertEqual(updates[m_id]["shard_id"], 200)

        self.assertEqual(stats["Total"], {"total": 1, "disc": 1, "crawl": 1, "idx": 0})
        self.assertEqual(stats["B"], {"total": 1, "disc": 1, "crawl": 1, "idx": 0})
        self.assertEqual(stats["A"]["total"], 0)


if __name__ == "__main__":
    unittest.main()
