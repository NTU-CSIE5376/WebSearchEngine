"""Text for the inverted index: one WARC response record in, one `TextDoc` out.

The crawler writes each WARC record as its own gzip member (proposal C6), and the
fetch log gives the member's offset and length, so a record is read by seeking
to it. Extraction keeps what an index needs: title, meta description, main
content and language. Raw HTML is not kept; the WARC is short-lived staging.
"""

from __future__ import annotations

import codecs
import io
from dataclasses import dataclass
from pathlib import Path

from fastwarc.warc import ArchiveIterator, WarcRecordType
from resiliparse.extract.html2text import extract_plain_text
from resiliparse.parse.encoding import bytes_to_str, detect_encoding
from resiliparse.parse.html import HTMLTree
from resiliparse.parse.lang import detect_fast


class ExtractError(RuntimeError):
    """Raised when the bytes at an offset are not one WARC response record."""


@dataclass(frozen=True)
class TextDoc:
    title: str | None
    description: str | None
    body: str
    lang: str | None


def read_response(path: Path, offset: int, length: int,
                  target_uri: str) -> tuple[bytes, str | None]:
    """Returns the HTTP payload and declared charset of the response record at `offset`.

    The record must start at `offset` and be the response for `target_uri`: a
    wrong offset would otherwise yield a neighbouring or truncated record.
    """
    with path.open("rb") as fh:
        fh.seek(offset)
        member = fh.read(length)
    if len(member) != length:
        raise ExtractError(f"{path.name}: {length} bytes at {offset} run past the end")
    for rec in ArchiveIterator(io.BytesIO(member), parse_http=True):
        if (rec.stream_pos != 0 or rec.record_type != WarcRecordType.response
                or rec.headers.get("WARC-Target-URI") != target_uri):
            break
        return rec.reader.read(), rec.http_charset
    raise ExtractError(f"{path.name}: no response record for {target_uri} at {offset}")


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
