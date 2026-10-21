"""Command line: `isel --config base.toml [--config overlay.toml] <command> [options]`.

Exit codes: 0 success, 2 config error.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from collections import Counter
from typing import Any

import pyarrow.parquet as pq

from isel.config import Config, ConfigError, load_config
from isel.io import atomic_write_json, open_duckdb, sql_str


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


def _router(cfg: Config):
    from isel.urlnorm import DomainKeyer, ShardRouter

    split = cfg.work_dir / "dims" / "split_hosts.parquet"
    if not split.exists():
        raise SystemExit("dims/split_hosts.parquet is missing; run pull-dims first")
    hosts = pq.read_table(split, columns=["host"]).column("host").to_pylist()
    keyer = DomainKeyer(hosts, offline=cfg.routing.tldextract_offline)
    return ShardRouter(keyer, cfg.crawlerdb.num_shards, cfg.routing.overrides)


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


def cmd_validate_routing(cfg: Config, args: argparse.Namespace) -> Any:
    """Row-weighted share of sample rows whose shard the router predicts."""
    from isel.urlnorm import HOST_RE

    router = _router(cfg)
    con = open_duckdb(cfg, cfg.work_dir / "duckdb_tmp")
    glob = sql_str(str(cfg.work_dir / "sample" / "shard=*.parquet"))
    pairs = con.execute(
        f"SELECT lower(regexp_extract(url, {sql_str(HOST_RE)}, 1)) AS host, shard, count(*) "
        f"FROM read_parquet({glob}) GROUP BY ALL").fetchall()
    total = primary = candidate = 0
    misses: Counter[str] = Counter()
    for host, shard, n in pairs:
        url = f"https://{host}/"
        total += n
        if router.shard_for_key(router.keyer.key_for_host(host)) == shard:
            primary += n
        if shard in router.candidate_shards(url):
            candidate += n
        else:
            misses[host] += n
    return {"rows": total, "primary_rate": primary / total if total else None,
            "candidate_rate": candidate / total if total else None,
            "top_misses": misses.most_common(20)}


def cmd_build_dataset(cfg: Config, args: argparse.Namespace) -> Any:
    """Writes per-batch pool sizes, label counts, label audit and history flags.

    `canonical_only_positives` are positives whose crawler URL matches only the
    canonical spelling; the official metric compares the raw spelling, so it can
    never count them as indexed.
    """
    from isel.features import open_store

    store = open_store(cfg, cfg.work_dir)
    summary = {}
    for b in store.batches:
        (positives, head, rand, canonical_only) = store.con.execute(
            f"SELECT count(*), count(*) FILTER (WHERE l.in_head), "
            f"count(*) FILTER (WHERE l.in_random), count(*) FILTER (WHERE r.url IS NULL) "
            f"FROM crawl c JOIN label_urls l ON l.url = c.url AND l.batch_id = {b.batch_id} "
            f"LEFT JOIN (SELECT DISTINCT raw_url AS url FROM gold "
            f"WHERE batch_id = {b.batch_id}) r ON r.url = c.url "
            f"WHERE c.last_fetch_ok < '{b.as_of.isoformat()}'::TIMESTAMPTZ").fetchone()
        (golden_urls,) = store.con.execute(
            f"SELECT count(DISTINCT raw_url) FROM gold WHERE batch_id = {b.batch_id}").fetchone()
        summary[str(b.batch_id)] = {
            "created_at": b.created_at, "as_of": b.as_of, "golden_urls": golden_urls,
            "pool_rows_est": store.pool_size(b.batch_id) / store.rate,
            "positive_rows": positives, "head_rows": head, "random_rows": rand,
            "canonical_only_positives": canonical_only,
            "has_history": store.has_history(b.batch_id),
            **store.label_audit(b.batch_id),
        }
    atomic_write_json(cfg.work_dir / "datasets" / "summary.json", summary)
    return summary


def cmd_derive_sample(cfg: Config, args: argparse.Namespace) -> Any:
    from isel.derive import derive_sample

    return derive_sample(cfg, cfg.work_dir)


def cmd_derive_positives(cfg: Config, args: argparse.Namespace) -> Any:
    from isel.derive import derive_positives

    return derive_positives(cfg, cfg.work_dir, _ids(args.batches))


def cmd_check_config(cfg: Config, args: argparse.Namespace) -> Any:
    out: dict[str, Any] = {"config_sha256": cfg.sha256()}
    from isel.features import resolve_set

    out["feature_sets"] = {n: len(resolve_set(cfg, n)) for n in cfg.features.sets}
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
    sub.add_parser("derive-sample", help="hash-sample the full export into sample/")
    p = sub.add_parser("derive-positives", help="golden URLs' rows from the full export")
    p.add_argument("--batches")
    sub.add_parser("validate-routing")
    sub.add_parser("build-dataset")
    return parser


COMMANDS = {
    "check-config": cmd_check_config,
    "pull-golden": cmd_pull_golden,
    "pull-dims": cmd_pull_dims,
    "pull-full": cmd_pull_full,
    "derive-sample": cmd_derive_sample,
    "derive-positives": cmd_derive_positives,
    "validate-routing": cmd_validate_routing,
    "build-dataset": cmd_build_dataset,
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
