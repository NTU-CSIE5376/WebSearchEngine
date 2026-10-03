from __future__ import annotations

import dataclasses
from pathlib import Path
from typing import Any

from idxsel.config import Config, load_config

ROOT = Path(__file__).resolve().parents[1]
EXAMPLE = ROOT / "config" / "idxsel.example.toml"


def example_config() -> Config:
    return load_config([EXAMPLE])


def with_changes(obj: Any, **changes: Any) -> Any:
    """Returns a copy of a frozen dataclass tree; keys use `__` for nesting."""
    direct = {k: v for k, v in changes.items() if "__" not in k}
    nested: dict[str, dict[str, Any]] = {}
    for key, value in changes.items():
        if "__" in key:
            head, rest = key.split("__", 1)
            nested.setdefault(head, {})[rest] = value
    for head, sub in nested.items():
        direct[head] = with_changes(getattr(obj, head), **sub)
    return dataclasses.replace(obj, **direct)
