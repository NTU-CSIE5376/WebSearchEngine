import pytest

from Metric.Measure.CrawlerAllMetricMeasure import normalize_golden_url


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
