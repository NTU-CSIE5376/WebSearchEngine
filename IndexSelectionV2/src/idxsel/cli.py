"""Command line: `idxsel --config FILE [--config OVERRIDE] <command> [options]`.

Commands that change the work directory take the work lock and exit with
code 4 while another idxsel process holds it.
"""

from __future__ import annotations

import argparse
import contextlib
import json
import sys
from typing import Any

from idxsel.config import load_config
from idxsel.guards import GuardAbort, PreflightFailed
from idxsel.load import init_selectdb, rollback
from idxsel.log import setup_logging
from idxsel.manifest import WorkDirBusy, work_lock
from idxsel.pg import SessionHolder, require_env
from idxsel.pipeline import (
    Runtime,
    backtest,
    daemon,
    deadline_before_build,
    gc_storage,
    parse_shards,
    run_once,
    stage_eval,
    stage_export,
    stage_features,
    stage_golden,
    stage_load,
    stage_select,
    stage_swap,
    stage_verify_live,
    status,
)

# Read-only checks, selectdb-only commands (rollback is the emergency path and
# must not queue behind a long export) and the daemon, which locks per tick.
_UNLOCKED = frozenset({"init-selectdb", "preflight", "probe", "verify-live", "rollback",
                       "status", "daemon"})


def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="idxsel", description=__doc__)
    p.add_argument("--config", action="append", required=True,
                   help="TOML config; repeat to layer overrides")
    sub = p.add_subparsers(dest="command", required=True)
    sub.add_parser("init-selectdb", help="create tables and the reader role in our selectdb")
    pre = sub.add_parser("preflight", help="check whether a crawlerdb export may start now")
    pre.add_argument("--no-db", action="store_true", help="skip crawlerdb statistics checks")
    sub.add_parser("probe", help="print current runtime guard findings")
    exp = sub.add_parser("export", help="export from crawlerdb")
    exp.add_argument("--mode", choices=["full", "delta"], required=True)
    exp.add_argument("--shards", help="e.g. 0-3,7 (default: all)")
    exp.add_argument("--dry-run", action="store_true", help="size from the catalog only")
    for name in ("golden", "features", "select", "load", "eval", "rollback", "status", "gc"):
        sub.add_parser(name)
    sw = sub.add_parser("swap")
    sw.add_argument("--run-id", type=int, help="model_runs id (default: latest loaded run)")
    sub.add_parser("verify-live", help="check the live list as the metric's reader role")
    run = sub.add_parser("run", help="one production cycle end to end")
    run.add_argument("--once", action="store_true", required=True)
    run.add_argument("--full", action="store_true", help="force a full export")
    run.add_argument("--shards", help="e.g. 0-3,7 (default: all)")
    sub.add_parser("backtest", help="golden -> select -> eval on the latest feature set; "
                                    "reads neither crawlerdb nor selectdb")
    dm = sub.add_parser("daemon", help="follow the schedule forever")
    dm.add_argument("--max-ticks", type=int)
    return p


def _dispatch(rt: Runtime, args: argparse.Namespace) -> Any:
    cmd = args.command
    cfg = rt.cfg
    if cmd == "init-selectdb":
        admin = rt.admin_conn()
        try:
            init_selectdb(admin, cfg.selectdb.reader_role,
                          require_env(cfg.selectdb.reader_password_env))
        finally:
            admin.close()
        return {"initialized": True}
    if cmd in ("preflight", "probe"):
        holder = (None if getattr(args, "no_db", False)
                  else SessionHolder(lambda: rt.crawler_session("guard")))
        try:
            guard = rt.guard(holder)
            if cmd == "probe":
                f = guard.check()
                return {"verdict": f.verdict.name, "reason": f.reason}
            guard.preflight("manual")
            return {"preflight": "ok"}
        finally:
            if holder is not None:
                holder.close()
    if cmd == "export":
        # A manual export stops when the build window opens and refuses inside it.
        deadline = None if args.dry_run else deadline_before_build(rt, "export")
        return stage_export(rt, args.mode, parse_shards(args.shards, cfg.crawlerdb.num_shards)
                            if args.shards else None, dry_run=args.dry_run, deadline=deadline)
    if cmd == "run":
        return run_once(rt, full=args.full,
                        shards=parse_shards(args.shards, cfg.crawlerdb.num_shards)
                        if args.shards else None)
    if cmd == "daemon":
        daemon(rt, max_ticks=args.max_ticks)
        return {"daemon": "stopped"}
    if cmd == "swap":
        return stage_swap(rt, args.run_id)
    if cmd == "rollback":
        admin = rt.admin_conn()
        try:
            return rollback(cfg, admin, rt.events)
        finally:
            admin.close()
    simple = {"golden": stage_golden, "features": stage_features, "select": stage_select,
              "load": stage_load, "eval": stage_eval, "verify-live": stage_verify_live,
              "status": status, "gc": gc_storage, "backtest": backtest}
    return simple[cmd](rt)


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    cfg = load_config(args.config)
    setup_logging(cfg.logging.level)
    rt = Runtime(cfg)
    locked = args.command not in _UNLOCKED
    try:
        with (work_lock(rt.layout.lock_file, args.command) if locked
              else contextlib.nullcontext()):
            result = _dispatch(rt, args)
    except WorkDirBusy as exc:
        print(json.dumps({"error": "WorkDirBusy", "reason": str(exc)}), file=sys.stderr)
        return 4
    except (PreflightFailed, GuardAbort) as exc:
        print(json.dumps({"error": type(exc).__name__, "reason": str(exc)}), file=sys.stderr)
        return 3
    print(json.dumps(result, default=str, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
