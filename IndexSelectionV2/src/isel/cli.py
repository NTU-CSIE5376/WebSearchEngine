"""Command line: `isel --config base.toml [--config overlay.toml] <command> [options]`.

Exit codes: 0 success, 2 config error.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from typing import Any

from isel.config import Config, ConfigError, load_config


def _ids(text: str | None) -> list[int] | None:
    """Parses `0-3,7,10-12` into a sorted id list."""
    if not text:
        return None
    out: set[int] = set()
    for part in text.split(","):
        lo, _, hi = part.partition("-")
        out.update(range(int(lo), int(hi or lo) + 1))
    return sorted(out)


def _print(obj: Any) -> None:
    print(json.dumps(obj, indent=2, sort_keys=True, default=str))


def cmd_pull_golden(cfg: Config, args: argparse.Namespace) -> Any:
    from isel.golden import pull_golden
    from isel.pg import open_metricdb

    session = open_metricdb(cfg)
    try:
        return pull_golden(session, cfg.work_dir / "golden", cfg.golden.tags)
    finally:
        session.close()


def cmd_pull_dims(cfg: Config, args: argparse.Namespace) -> Any:
    from isel.pg import open_crawlerdb
    from isel.pull import pull_dims

    session = open_crawlerdb(cfg)
    try:
        return pull_dims(cfg, session, cfg.work_dir / "dims")
    finally:
        session.close()


def cmd_pull_full(cfg: Config, args: argparse.Namespace) -> Any:
    import shutil
    from datetime import UTC, datetime

    from isel.pg import next_allowed_start, next_forbidden_window, open_crawlerdb
    from isel.pull import estimate_full, pull_full

    out = cfg.work_dir / "full" / "export"
    out.mkdir(parents=True, exist_ok=True)
    sessions = []
    try:
        for _ in range(1 if args.dry_run else cfg.full.connections):
            sessions.append(open_crawlerdb(cfg))
        if args.dry_run:
            session = sessions[0]
            estimate = estimate_full(cfg, session, _ids(args.shards))
            now = datetime.now(UTC)
            return {**estimate, "io_waiters_now": session.io_waiters(),
                    "work_dir_free_gb": round(shutil.disk_usage(out).free / 1e9, 1),
                    "forbidden_window_next": [t.isoformat()
                                              for t in next_forbidden_window(now, cfg.guard)],
                    "next_chunk_start": next_allowed_start(
                        now, estimate["chunk_timeout_s"], cfg.guard).isoformat()}
        return pull_full(cfg, sessions, out, _ids(args.shards))
    finally:
        for session in sessions:
            session.close()


def cmd_check_config(cfg: Config, args: argparse.Namespace) -> Any:
    out: dict[str, Any] = {"config_sha256": cfg.sha256()}
    return out


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="isel")
    parser.add_argument("--config", action="append", required=True,
                        help="TOML config; repeat to layer overlays")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("check-config")
    sub.add_parser("pull-golden")
    sub.add_parser("pull-dims")
    p = sub.add_parser("pull-full", help="export every crawled row in ctid block ranges")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--shards")
    return parser


COMMANDS = {
    "check-config": cmd_check_config,
    "pull-golden": cmd_pull_golden,
    "pull-dims": cmd_pull_dims,
    "pull-full": cmd_pull_full,
}


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(level=logging.INFO, stream=sys.stderr,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    try:
        cfg = load_config(args.config)
    except ConfigError as exc:
        print(f"config error: {exc}", file=sys.stderr)
        return 2
    result = COMMANDS[args.command](cfg, args)
    if result is not None:
        _print(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
