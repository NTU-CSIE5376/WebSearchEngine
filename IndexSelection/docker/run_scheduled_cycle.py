import argparse
import json
import subprocess
from datetime import date, datetime, timedelta, timezone
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run the insel cadence: biweekly 32M refresh plus pre-golden reset."
    )
    parser.add_argument("--anchor-date", required=True, help="UTC date when the golden set becomes available (YYYY-MM-DD).")
    parser.add_argument("--cadence-days", type=int, default=14, help="Golden-set cadence in days.")
    parser.add_argument("--reset-days-before", type=int, default=7, help="Reset this many days before the next golden-set date.")
    parser.add_argument("--today-utc", default="", help="Optional override UTC date (YYYY-MM-DD). Defaults to today in UTC.")
    parser.add_argument("--refresh-command", type=Path, default=BASE_DIR / "run_refresh.sh", help="Command executed on refresh days.")
    parser.add_argument("--reset-command", type=Path, default=BASE_DIR / "run_reset.sh", help="Command executed on reset days.")
    parser.add_argument("--dry-run", action="store_true", help="Print the computed action without executing it.")
    return parser.parse_args()


def parse_utc_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def next_phase_date(today: date, phase: int, current_phase: int, cadence_days: int) -> date:
    days_until = (phase - current_phase) % cadence_days
    return today + timedelta(days=days_until)


def main():
    args = parse_args()
    if args.cadence_days <= 1:
        raise ValueError("--cadence-days must be > 1")
    if args.reset_days_before <= 0 or args.reset_days_before >= args.cadence_days:
        raise ValueError("--reset-days-before must be between 1 and cadence_days - 1")

    anchor_date = parse_utc_date(args.anchor_date)
    today_utc = parse_utc_date(args.today_utc) if args.today_utc else datetime.now(timezone.utc).date()

    refresh_phase = 0
    reset_phase = (args.cadence_days - args.reset_days_before) % args.cadence_days
    phase = (today_utc - anchor_date).days % args.cadence_days

    if phase == refresh_phase:
        action = "refresh"
        command = [str(args.refresh_command)]
    elif phase == reset_phase:
        action = "reset"
        command = [str(args.reset_command)]
    else:
        action = "noop"
        command = []

    payload = {
        "today_utc": today_utc.isoformat(),
        "anchor_date": anchor_date.isoformat(),
        "cadence_days": int(args.cadence_days),
        "reset_days_before": int(args.reset_days_before),
        "phase": int(phase),
        "refresh_phase": int(refresh_phase),
        "reset_phase": int(reset_phase),
        "action": action,
        "next_refresh_date": next_phase_date(today_utc, refresh_phase, phase, args.cadence_days).isoformat(),
        "next_reset_date": next_phase_date(today_utc, reset_phase, phase, args.cadence_days).isoformat(),
        "command": command,
        "dry_run": bool(args.dry_run),
    }

    if action != "noop" and not args.dry_run:
        completed = subprocess.run(command, check=False)
        payload["command_exit_code"] = int(completed.returncode)
        print(json.dumps(payload, indent=2), flush=True)
        raise SystemExit(completed.returncode)

    print(json.dumps(payload, indent=2), flush=True)


if __name__ == "__main__":
    main()
