import json
from pathlib import Path

from psycopg2.extras import Json

from live_db_common import DEFAULT_DB_RETRY_ATTEMPTS, close_pg_quietly, connect_pg, is_retryable_pg_error, sleep_before_retry


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS public.model_runs (
    id BIGSERIAL PRIMARY KEY,
    run_label TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    cutoff_datetime TIMESTAMPTZ,
    model_path TEXT,
    train_metrics JSONB NOT NULL DEFAULT '{}'::jsonb,
    eval_metrics JSONB NOT NULL DEFAULT '{}'::jsonb,
    selection_metrics JSONB NOT NULL DEFAULT '{}'::jsonb,
    source_paths JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS public.selected_urls_current (
    url TEXT PRIMARY KEY,
    score DOUBLE PRECISION NOT NULL,
    first_seen TIMESTAMPTZ,
    run_id BIGINT NOT NULL REFERENCES public.model_runs(id) ON DELETE CASCADE,
    selected_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_selected_urls_current_run_id
    ON public.selected_urls_current (run_id);

CREATE INDEX IF NOT EXISTS idx_selected_urls_current_score_desc
    ON public.selected_urls_current (score DESC);
"""


def ensure_selectdb_schema(
    select_db_url: str,
    *,
    db_retry_attempts: int = DEFAULT_DB_RETRY_ATTEMPTS,
) -> None:
    for attempt in range(1, db_retry_attempts + 1):
        conn = None
        try:
            conn = connect_pg(
                select_db_url,
                readonly=False,
                application_name="indexselection_v1_selectdb_schema",
            )
            with conn.cursor() as cur:
                cur.execute(SCHEMA_SQL)
            conn.commit()
            return
        except Exception as exc:
            if conn is not None:
                conn.rollback()
            if not is_retryable_pg_error(exc) or attempt >= db_retry_attempts:
                raise
            print(f"[selectdb_schema] retry attempt={attempt + 1} error={exc}", flush=True)
            sleep_before_retry(attempt)
        finally:
            close_pg_quietly(conn)


def load_json_file(path: Path | None) -> dict:
    if path is None:
        return {}
    candidate = Path(path)
    if not candidate.is_file():
        return {}
    with candidate.open("r", encoding="utf-8") as f:
        data = json.load(f)
    return data if isinstance(data, dict) else {}


def insert_model_run(
    conn,
    *,
    run_label: str,
    cutoff_datetime: str | None,
    model_path: str,
    train_metrics: dict,
    eval_metrics: dict,
    selection_metrics: dict,
    source_paths: dict,
) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO public.model_runs (
                run_label,
                cutoff_datetime,
                model_path,
                train_metrics,
                eval_metrics,
                selection_metrics,
                source_paths
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (run_label) DO UPDATE
            SET cutoff_datetime = EXCLUDED.cutoff_datetime,
                model_path = EXCLUDED.model_path,
                train_metrics = EXCLUDED.train_metrics,
                eval_metrics = EXCLUDED.eval_metrics,
                selection_metrics = EXCLUDED.selection_metrics,
                source_paths = EXCLUDED.source_paths
            RETURNING id
            """,
            (
                run_label,
                cutoff_datetime,
                model_path,
                Json(train_metrics),
                Json(eval_metrics),
                Json(selection_metrics),
                Json(source_paths),
            ),
        )
        row = cur.fetchone()
    return int(row[0])
