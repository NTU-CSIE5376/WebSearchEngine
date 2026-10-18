"""Text for the inverted index: one HTML page in, one `TextDoc` out.

The crawler stores each page's raw bytes without its headers, so the encoding
is detected from the bytes (or a `<meta charset>`) unless a caller declares
one. Extraction keeps what an index needs: title, meta description, main
content and language. Raw HTML is not kept.
"""

from __future__ import annotations

import codecs
from dataclasses import dataclass

from resiliparse.extract.html2text import extract_plain_text
from resiliparse.parse.encoding import bytes_to_str, detect_encoding
from resiliparse.parse.html import HTMLTree
from resiliparse.parse.lang import detect_fast


@dataclass(frozen=True)
class TextDoc:
    title: str | None
    description: str | None
    body: str
    lang: str | None


def _encoding(payload: bytes, declared: str | None) -> str:
    if declared:
        try:
            return codecs.lookup(declared).name
        except LookupError:
            pass
    return detect_encoding(payload)


def _clean(text: str | None) -> str | None:
    text = " ".join(text.split()) if text else ""
    return text or None


def extract(payload: bytes, charset: str | None, max_body_chars: int) -> TextDoc:
    """Title, description, main-content text and language of an HTML payload."""
    tree = HTMLTree.parse(bytes_to_str(payload, _encoding(payload, charset)))
    meta = tree.head.query_selector('meta[name="description" i]') if tree.head else None
    # Main-content mode drops boilerplate; a page that is all boilerplate keeps its full text.
    body = (extract_plain_text(tree, main_content=True) or extract_plain_text(tree))
    body = body[:max_body_chars]
    title = _clean(tree.title)
    lang, _ = detect_fast(body or title or "")
    return TextDoc(title=title, description=_clean(meta.getattr("content") if meta else None),
                   body=body, lang=None if lang == "unknown" else lang)
