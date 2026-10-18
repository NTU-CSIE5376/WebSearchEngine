"""Text extraction from raw HTML bytes, as the recrawl archive reads them."""

from __future__ import annotations

import pytest

from isel.extract import extract

ARTICLE = """<html><head><title>  颱風 假   </title>
<meta name="Description" content=" 明天 停班停課 "></head><body>
<nav><a href="/">首頁</a><a href="/news">新聞</a></nav>
<main><h1>颱風假公布</h1><p>台北市和新北市宣布明天停止上班上課，氣象署表示颱風將在今晚登陸，
民眾應注意安全並做好防颱準備。</p></main><footer>Copyright 2026</footer></body></html>"""
NEWS = ("繁體中文的內容，用來確認編碼。颱風假的消息今天下午公布，台北市與新北市宣布明天停止上班上課，"
        "氣象署表示颱風將在今晚登陸東部，民眾應注意強風豪雨並做好防颱準備。")


def test_extracts_main_content_title_description_and_language():
    doc = extract(ARTICLE.encode(), None, max_body_chars=10_000)
    assert doc.title == "颱風 假" and doc.description == "明天 停班停課"
    assert "颱風假公布" in doc.body and "首頁" not in doc.body and "Copyright" not in doc.body
    assert doc.lang == "zh"


@pytest.mark.parametrize("encoding", ["big5", "gbk", "utf-8"])
@pytest.mark.parametrize("meta", [True, False])
def test_detects_the_encoding_without_headers(encoding, meta):
    head = f'<meta charset="{encoding}">' if meta else ""
    html = f"<html><head>{head}<title>測試</title></head><body><p>{NEWS}</p></body></html>"
    doc = extract(html.encode(encoding), None, max_body_chars=10_000)
    assert doc.title == "測試" and "繁體中文的內容" in doc.body and doc.lang == "zh"


def test_a_declared_charset_wins_and_an_unknown_one_is_ignored():
    html = f"<html><body><p>{NEWS}</p></body></html>".encode("big5")
    assert "颱風假" in extract(html, "big5", 10_000).body
    plain = b"<html><head><title>Plain</title></head><body><p>Hello world</p></body></html>"
    doc = extract(plain, "no-such-charset", 10_000)
    assert (doc.title, doc.description, doc.body) == ("Plain", None, "Hello world")


def test_a_page_that_is_all_boilerplate_keeps_its_full_text_and_the_body_is_capped():
    html = b"<html><body><nav>Only navigation links here</nav></body></html>"
    assert extract(html, None, 10_000).body == "Only navigation links here"
    long = b"<html><body><main><p>" + b"word " * 1000 + b"</p></main></body></html>"
    assert len(extract(long, "utf-8", 50).body) == 50
