"""Command line: `isel --config base.toml [--config overlay.toml] <command> [options]`.

Exit codes: 0 success, 2 config error.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from collections import Counter
from pathlib import Path
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


def _names(text: str | None, default: list[str]) -> list[str]:
    return [n for n in text.split(",") if n] if text else list(default)


def _baselines(text: str | None, cfg: Config) -> list[str]:
    """Baseline names from `--baselines` (default `eval.baselines`); unknown names exit."""
    from isel.config import KNOWN_BASELINES

    names = _names(text, cfg.eval.baselines)
    unknown = sorted(set(names) - set(KNOWN_BASELINES))
    if unknown:
        raise SystemExit(f"unknown baselines {unknown}; known: {', '.join(KNOWN_BASELINES)}")
    return names


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
            "created_at": b.created_at, "as_of": b.as_of, "official": b.official,
            "available_at": b.available_at, "golden_urls": golden_urls,
            "pool_rows_est": store.pool_size(b.batch_id) / store.rate,
            "positive_rows": positives, "head_rows": head, "random_rows": rand,
            "canonical_only_positives": canonical_only,
            "has_history": store.has_history(b.batch_id),
            **(store.label_audit(b.batch_id) if b.official else {}),
        }
    atomic_write_json(cfg.work_dir / "datasets" / "summary.json", summary)
    return summary


def cmd_backtest(cfg: Config, args: argparse.Namespace) -> Any:
    from isel.backtest import run_backtest

    out = run_backtest(cfg, cfg.work_dir, _names(args.feature_sets, [cfg.model.feature_set]),
                       _baselines(args.baselines, cfg))
    return {"results": str(out / "results.json"), "report": str(out / "report.md")}


def cmd_report(cfg: Config, args: argparse.Namespace) -> Any:
    from isel.evaluate import render_report
    from isel.io import read_json

    results = read_json(Path(args.results))
    if results is None:
        raise SystemExit(f"{args.results} not found")
    sys.stdout.write(render_report(results))
    return None


def cmd_train(cfg: Config, args: argparse.Namespace) -> Any:
    from isel.backtest import train_frozen

    return {"model_dir": str(train_frozen(cfg, cfg.work_dir,
                                          _baselines(args.baselines, cfg)))}


def cmd_forward_eval(cfg: Config, args: argparse.Namespace) -> Any:
    from isel.backtest import forward_eval

    out = forward_eval(cfg, cfg.work_dir, [Path(d) for d in args.model_dir], args.batch,
                       _baselines(args.baselines, cfg))
    return {"results": str(out / "results.json"), "report": str(out / "report.md")}


def cmd_derive_sample(cfg: Config, args: argparse.Namespace) -> Any:
    from isel.derive import derive_sample

    return derive_sample(cfg, cfg.work_dir)


def cmd_derive_positives(cfg: Config, args: argparse.Namespace) -> Any:
    from isel.derive import derive_positives

    return derive_positives(cfg, cfg.work_dir, _ids(args.batches))


def cmd_select_sample(cfg: Config, args: argparse.Namespace) -> Any:
    from isel.backtest import select_sample

    meta = select_sample(cfg, cfg.work_dir, Path(args.model_dir), args.k or cfg.eval.k_headline)
    return {k: v for k, v in meta.items() if k != "files"}


def cmd_score_full(cfg: Config, args: argparse.Namespace) -> Any:
    from isel.score import score_full

    return score_full(cfg, cfg.work_dir, Path(args.model_dir), args.k or cfg.eval.k_headline)


def cmd_select_full(cfg: Config, args: argparse.Namespace) -> Any:
    from isel.score import select_full

    meta = select_full(cfg, cfg.work_dir, args.k or cfg.eval.k_headline)
    return {k: v for k, v in meta.items() if k != "files"}


def _with_admin(cfg: Config, action: Any) -> Any:
    """Runs `action(conn)` on an admin connection to our selectdb."""
    from isel.selectdb import connect

    conn = connect(cfg, cfg.selectdb.admin_dsn_env, read_only=False)
    try:
        return action(conn)
    finally:
        conn.close()


def cmd_migrate_selectdb(cfg: Config, args: argparse.Namespace) -> Any:
    from isel.selectdb import init_role, migrate, render_sql

    if args.sql:
        render_sql(args.stamp_baseline, sys.stdout)
        return None
    result = _with_admin(cfg, lambda conn: migrate(conn, args.stamp_baseline))
    _with_admin(cfg, lambda conn: init_role(cfg, conn))
    return {**result, "reader_role": cfg.selectdb.reader_role}


def cmd_publish(cfg: Config, args: argparse.Namespace) -> Any:
    from isel.publish import load_selection

    return _with_admin(cfg, lambda conn: load_selection(cfg, conn, Path(args.run_dir),
                                                         cfg.work_dir))


def cmd_swap(cfg: Config, args: argparse.Namespace) -> Any:
    from isel.publish import swap

    return _with_admin(cfg, lambda conn: swap(cfg, conn, args.db_run_id))


def cmd_rollback(cfg: Config, args: argparse.Namespace) -> Any:
    from isel.publish import rollback

    return _with_admin(cfg, lambda conn: rollback(cfg, conn))


def cmd_verify_live(cfg: Config, args: argparse.Namespace) -> Any:
    from isel.publish import verify_live

    return verify_live(cfg, cfg.work_dir, args.batch)


def _recrawl(cfg: Config, action: Any, read_only: bool = False) -> Any:
    """Runs `action(conn)` as admin on a selectdb at the newest schema revision.

    The reader role cannot see `recrawl`, so read-only work uses the admin role
    in a read-only session.
    """
    from isel.selectdb import assert_ready, connect

    conn = connect(cfg, cfg.selectdb.admin_dsn_env, read_only=read_only)
    try:
        assert_ready(cfg, conn)
        return action(conn)
    finally:
        conn.close()


def cmd_recrawl_sync(cfg: Config, args: argparse.Namespace) -> Any:
    from isel.recrawl import sync

    return _recrawl(cfg, lambda conn: sync(cfg, conn))


def cmd_recrawl_plan(cfg: Config, args: argparse.Namespace) -> Any:
    from isel.recrawl import capacity_report, run_planner

    if args.dry_run:
        return _recrawl(cfg, lambda conn: capacity_report(cfg, conn), read_only=True)
    return _recrawl(cfg, lambda conn: run_planner(cfg, conn, args.once))


def cmd_recrawl_archive(cfg: Config, args: argparse.Namespace) -> Any:
    from isel.recrawl import run_archiver

    return _recrawl(cfg, lambda conn: run_archiver(cfg, conn, args.once))


def cmd_validate_manifest(cfg: Config, args: argparse.Namespace) -> Any:
    from isel.manifest import validate_run

    meta = validate_run(Path(args.run_dir))
    return {"run_id": meta["run_id"], "rows": meta["rows"], "ok": True}


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
    p = sub.add_parser("backtest")
    p.add_argument("--feature-sets")
    p.add_argument("--baselines")
    p = sub.add_parser("report")
    p.add_argument("--results", required=True)
    p = sub.add_parser("train")
    p.add_argument("--baselines")
    p = sub.add_parser("forward-eval")
    p.add_argument("--model-dir", required=True, action="append",
                   help="repeat to compare models")
    p.add_argument("--batch", type=int, required=True)
    p.add_argument("--baselines")
    p = sub.add_parser("select-sample")
    p.add_argument("--model-dir", required=True)
    p.add_argument("--k", type=int)
    p = sub.add_parser("score-full", help="score the full export with a frozen model")
    p.add_argument("--model-dir", required=True)
    p.add_argument("--k", type=int)
    p = sub.add_parser("select-full", help="write the exact top-K list")
    p.add_argument("--k", type=int)
    p = sub.add_parser("migrate-selectdb",
                       help="bring selectdb to the newest schema revision; set the reader role")
    p.add_argument("--sql", action="store_true", help="print the DDL instead of running it")
    p.add_argument("--stamp-baseline", action="store_true",
                   help="record revision 0001 on a selectdb made before Alembic, then upgrade")
    p = sub.add_parser("publish", help="load a select-full run into a new selectdb table")
    p.add_argument("--run-dir", required=True)
    p = sub.add_parser("swap", help="make a loaded run live")
    p.add_argument("--db-run-id", type=int, required=True)
    sub.add_parser("rollback", help="make the previous list live again")
    p = sub.add_parser("verify-live", help="indexed rate of a batch under the live list")
    p.add_argument("--batch", type=int, required=True)
    sub.add_parser("recrawl-sync", help="load the live run into the recrawl frontier")
    p = sub.add_parser("recrawl-plan", help="send due batches to the crawler API (loop)")
    p.add_argument("--once", action="store_true", help="run one cycle and exit")
    p.add_argument("--dry-run", action="store_true",
                   help="print the per-host capacity report; send nothing")
    p = sub.add_parser("recrawl-archive", help="apply crawler fetch logs; store page text (loop)")
    p.add_argument("--once", action="store_true", help="run one cycle and exit")
    p = sub.add_parser("validate-manifest")
    p.add_argument("--run-dir", required=True)
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
    "backtest": cmd_backtest,
    "report": cmd_report,
    "train": cmd_train,
    "forward-eval": cmd_forward_eval,
    "select-sample": cmd_select_sample,
    "score-full": cmd_score_full,
    "select-full": cmd_select_full,
    "migrate-selectdb": cmd_migrate_selectdb,
    "publish": cmd_publish,
    "swap": cmd_swap,
    "rollback": cmd_rollback,
    "verify-live": cmd_verify_live,
    "recrawl-sync": cmd_recrawl_sync,
    "recrawl-plan": cmd_recrawl_plan,
    "recrawl-archive": cmd_recrawl_archive,
    "validate-manifest": cmd_validate_manifest,
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
