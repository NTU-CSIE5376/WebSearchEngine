"""JSON logging plus a per-run append-only event log."""

from __future__ import annotations

import json
import logging
import sys
import threading
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pythonjsonlogger.json import JsonFormatter

logger = logging.getLogger("idxsel")


def setup_logging(level: str) -> None:
    """Sends JSON log lines to stderr; stdout carries only command results."""
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(JsonFormatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
    root = logging.getLogger("idxsel")
    root.handlers[:] = [handler]
    root.setLevel(level)
    root.propagate = False


class EventLog:
    """Appends structured events to a JSONL file and mirrors them to the logger."""

    def __init__(self, path: Path | None) -> None:
        self._path = path
        self._lock = threading.Lock()
        if path is not None:
            path.parent.mkdir(parents=True, exist_ok=True)

    def emit(self, event: str, **fields: Any) -> None:
        record = {"ts": datetime.now(UTC).isoformat(), "event": event, **fields}
        logger.info(event, extra={"fields": fields})
        if self._path is None:
            return
        line = json.dumps(record, default=str, ensure_ascii=False)
        with self._lock, self._path.open("a", encoding="utf-8") as fh:
            fh.write(line + "\n")
