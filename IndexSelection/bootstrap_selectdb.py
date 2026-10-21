import argparse

from live_db_common import DEFAULT_DB_RETRY_ATTEMPTS
from selectdb_common import ensure_selectdb_schema


def parse_args():
    parser = argparse.ArgumentParser(description="Create the selectdb schema used by IndexSelection_v1.")
    parser.add_argument("--select-db-url", required=True, help="Target selectdb URL.")
    parser.add_argument(
        "--db-retry-attempts",
        type=int,
        default=DEFAULT_DB_RETRY_ATTEMPTS,
        help="Retry attempts when the selectdb connection drops during schema creation.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    if args.db_retry_attempts <= 0:
        raise ValueError("--db-retry-attempts must be > 0")
    ensure_selectdb_schema(args.select_db_url, db_retry_attempts=args.db_retry_attempts)
    print("selectdb_schema_ready")


if __name__ == "__main__":
    main()
