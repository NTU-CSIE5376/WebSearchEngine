"""Text extraction from WARC response records, as the recrawl archive reads them."""

from __future__ import annotations

import io

import pytest
from warcio.statusandheaders import StatusAndHeaders
from warcio.warcwriter import WARCWriter

from isel.extract import ExtractError, extract, read_response

ARTICLE = """<html><head><title>  颱風 假   </title>
<meta name="Description" content=" 明天 停班停課 "></head><body>
<nav><a href="/">首頁</a><a href="/news">新聞</a></nav>
<main><h1>颱風假公布</h1><p>台北市和新北市宣布明天停止上班上課，氣象署表示颱風將在今晚登陸，
民眾應注意安全並做好防颱準備。</p></main><footer>Copyright 2026</footer></body></html>"""


def _warc(tmp_path, *records):
    """Writes (uri, status, content_type, body) records; returns path and offsets."""
    path = tmp_path / "x.warc.gz"
    spans = []
    with path.open("wb") as fh:
        writer = WARCWriter(fh, gzip=True)
        writer.write_record(writer.create_warcinfo_record("x.warc.gz", {"software": "t"}))
        for uri, status, ctype, body in records:
            http = StatusAndHeaders(f"{status} OK", [("Content-Type", ctype)],
                                    protocol="HTTP/1.1")
            offset = fh.tell()
            writer.write_record(writer.create_warc_record(
                uri, "response", payload=io.BytesIO(body), http_headers=http))
            spans.append((offset, fh.tell() - offset))
    return path, spans


def test_reads_each_record_by_offset_and_extracts_main_content(tmp_path):
    big5 = ("<html><head><title>測試</title></head><body><p>繁體中文的內容，用來確認編碼。"
            "颱風假的消息今天下午公布，台北市與新北市宣布明天停止上班上課，"
            "氣象署表示颱風將在今晚登陸東部，民眾應注意強風豪雨並做好防颱準備。</p></body></html>")
    path, spans = _warc(
        tmp_path,
        ("https://a.example/1", 200, "text/html; charset=utf-8", ARTICLE.encode()),
        ("https://a.example/2", 200, "text/html; charset=big5", big5.encode("big5")),
        ("https://a.example/3", 200, "text/html; charset=no-such-charset",
         b"<html><head><title>Plain</title></head><body><p>Hello world</p></body></html>"))
    payload, charset = read_response(path, *spans[0], "https://a.example/1")
    assert charset == "utf-8"
    doc = extract(payload, charset, max_body_chars=10_000)
    assert doc.title == "颱風 假" and doc.description == "明天 停班停課"
    assert "颱風假公布" in doc.body and "首頁" not in doc.body and "Copyright" not in doc.body
    assert doc.lang == "zh"
    doc = extract(*read_response(path, *spans[1], "https://a.example/2"), max_body_chars=10_000)
    assert doc.title == "測試" and "繁體中文的內容" in doc.body and doc.lang == "zh"
    doc = extract(*read_response(path, *spans[2], "https://a.example/3"), max_body_chars=10_000)
    assert (doc.title, doc.description, doc.body) == ("Plain", None, "Hello world")


def test_a_page_that_is_all_boilerplate_keeps_its_full_text_and_the_body_is_capped():
    html = b"<html><body><nav>Only navigation links here</nav></body></html>"
    assert extract(html, None, 10_000).body == "Only navigation links here"
    long = b"<html><body><main><p>" + b"word " * 1000 + b"</p></main></body></html>"
    assert len(extract(long, "utf-8", 50).body) == 50


def test_read_response_refuses_bytes_that_are_not_one_response_record(tmp_path):
    uri = "https://a.example/1"
    path, spans = _warc(tmp_path, (uri, 200, "text/html", b"<p>x</p>"),
                        ("https://a.example/2", 200, "text/html", b"<p>y</p>"))
    offset, length = spans[0]
    with pytest.raises(ExtractError, match="run past the end"):
        read_response(path, offset, length + 10_000, uri)
    with pytest.raises(ExtractError, match="no response record"):
        read_response(path, 0, offset, uri)                 # the warcinfo record
    with pytest.raises(ExtractError, match="no response record"):
        read_response(path, 0, length, uri)                 # warcinfo, then a cut-off record
    with pytest.raises(ExtractError, match="no response record"):
        read_response(path, *spans[1], uri)                 # another page's record
