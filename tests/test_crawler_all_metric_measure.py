from datetime import date

import pytest
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import declarative_base

from Database.ModelFactory.AppModelFactory import AppModelFactory
from Metric.Measure.CrawlerAllMetricMeasure import coverage_upsert, normalize_golden_url


@pytest.mark.parametrize(
    "raw, stored",
    [
        # Query parameters are sorted.
        (
            "https://play.google.com/store/apps/details?id=com.moonpay&hl=en_US",
            "https://play.google.com/store/apps/details?hl=en_US&id=com.moonpay",
        ),
        # %27 in the path is decoded to '.
        (
            "https://en.wikipedia.org/wiki/Ardal_O%27Hanlon",
            "https://en.wikipedia.org/wiki/Ardal_O'Hanlon",
        ),
        # , and = in query values are escaped.
        (
            "https://ncr.indianrailways.gov.in/view_section.jsp?lang=1&id=0,1,396",
            "https://ncr.indianrailways.gov.in/view_section.jsp?id=0%2C1%2C396&lang=1",
        ),
        (
            "/goto?url=CAESYAHuR6pN==",
            "/goto?url=CAESYAHuR6pN%3D%3D",
        ),
        # Non-ASCII escapes are kept, in upper case.
        (
            "https://www.swissinfo.ch/spa/vac%c3%ado",
            "https://www.swissinfo.ch/spa/vac%C3%ADo",
        ),
    ],
)
def test_matches_crawler_form(raw, stored):
    assert normalize_golden_url(raw) == stored


def test_already_canonical_url_is_unchanged():
    url = "https://en.wikipedia.org/wiki/Ardal_O'Hanlon"
    assert normalize_golden_url(url) == url


def test_different_raw_forms_map_to_one_url():
    a = "https://play.google.com/store/apps/details?id=com.moonpay&hl=en_US"
    b = "https://play.google.com/store/apps/details?hl=en_US&id=com.moonpay"
    assert normalize_golden_url(a) == normalize_golden_url(b)


def _compiled_upsert():
    model = AppModelFactory(declarative_base(), declarative_base()).create_metric_coverage_model(
        "HeadSet", "Total")
    row = {"stat_date": date(2026, 11, 16), "total": 4, "discovered_num": 4,
           "discovered_rate": 1.0, "crawled_num": 3, "crawled_rate": 0.75,
           "indexed_num": 3, "indexed_rate": 0.75}
    return coverage_upsert(model, row).compile(dialect=postgresql.dialect())


def test_new_coverage_row_starts_without_a_rank():
    compiled = _compiled_upsert()
    assert compiled.params["ranked_num"] is None
    assert compiled.params["ranked_rate"] is None


def test_rerun_keeps_the_rank_and_updates_the_rest():
    sql = str(_compiled_upsert())
    assert sql.startswith("INSERT INTO metric_headset_total")
    _, update = sql.split("ON CONFLICT (stat_date) DO UPDATE SET")
    assert "indexed_num" in update and "crawled_rate" in update
    assert "ranked_num" not in update and "ranked_rate" not in update
