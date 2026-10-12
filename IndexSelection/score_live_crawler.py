import os

# Apply conservative native-thread defaults before importing train_model, which
# pulls in numpy and later loads Polars/LightGBM inside worker processes.
os.environ.setdefault("POLARS_MAX_THREADS", "1")
os.environ.setdefault("RAYON_NUM_THREADS", "1")
os.environ.setdefault("OMP_NUM_THREADS", "1")
os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")
os.environ.setdefault("MKL_NUM_THREADS", "1")

import argparse
from dataclasses import dataclass
import json
import multiprocessing as mp
import subprocess
import sys
import tempfile
import threading
import traceback
from concurrent.futures import ALL_COMPLETED, FIRST_COMPLETED, ProcessPoolExecutor, ThreadPoolExecutor, wait
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter

from live_db_common import (
    DEFAULT_DB_RETRY_ATTEMPTS,
    DEFAULT_LIVE_WORKER_COUNT,
    estimate_crawler_shard_rows,
    parse_db_datetime,
    sleep_before_retry,
)
from train_model import build_feature_matrix, write_score_shard
from training_common import (
    ARTIFACT_DIR,
    DEFAULT_BINARY_MODEL_PATH,
    DEFAULT_DOMAIN_FREQ_PATH,
    URL_FEATURE_COLUMNS,
    ensure_parent,
    read_domain_freq,
    write_json,
)


DEFAULT_SCORE_SHARD_DIR = ARTIFACT_DIR / "live_crawler_score_shards"
DEFAULT_SELECTION_OUTPUT_PATH = ARTIFACT_DIR / "live_crawler_top_selection.tsv"
DEFAULT_SELECTION_SUMMARY_PATH = ARTIFACT_DIR / "live_crawler_top_selection_summary.json"
_WORKER_CONTEXT = {}
_POOL_START_METHOD = "spawn"
_SPAWN_CONTEXT = mp.get_context(_POOL_START_METHOD)
_CHUNK_MAX_TASKS_PER_CHILD = 50
_SHARD_MAX_TASKS_PER_CHILD = 1
_CURRENT_FEATURE_COLUMNS = tuple(URL_FEATURE_COLUMNS)
_CURRENT_FEATURE_INDEX = {name: idx for idx, name in enumerate(_CURRENT_FEATURE_COLUMNS)}


@dataclass
class _SortedRun:
    path: Path
    row_count: int
    min_score: float | None


def _make_process_pool(*, max_workers: int, max_tasks_per_child: int | None) -> ProcessPoolExecutor:
    return ProcessPoolExecutor(
        max_workers=max_workers,
        mp_context=_SPAWN_CONTEXT,
        max_tasks_per_child=max_tasks_per_child,
    )


def _cpu_parallelism_budget() -> int:
    process_cpu_count = getattr(os, "process_cpu_count", None)
    detected = process_cpu_count() if callable(process_cpu_count) else None
    if detected is None:
        detected = os.cpu_count()
    return max(1, int(detected or 1))


def _resolve_merge_threads(requested_threads: int) -> int:
    if requested_threads > 0:
        return requested_threads
    return max(1, min(4, os.cpu_count() or 1))


def _merge_tsv_in_subprocess(
    *,
    run_paths: list[Path],
    output_path: Path,
    top_k: int,
    input_row_count: int | None,
    merge_threads: int,
    max_urls_per_domain: int | None = None,
) -> int:
    helper_path = Path(__file__).with_name("polars_merge_helper.py")
    output_parent = output_path.resolve().parent
    output_parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        dir=output_parent,
        prefix="merge_inputs_",
        suffix=".txt",
        delete=False,
    ) as manifest_file:
        manifest_path = Path(manifest_file.name)
        for run_path in run_paths:
            manifest_file.write(str(run_path.resolve()) + "\n")

    cmd = [
        sys.executable,
        str(helper_path),
        "--input-manifest",
        str(manifest_path),
        "--output-path",
        str(output_path.resolve()),
        "--top-k",
        str(top_k),
        "--output-format",
        "tsv",
    ]
    if input_row_count is not None:
        cmd.extend(["--input-row-count", str(input_row_count)])
    if max_urls_per_domain is not None:
        cmd.extend(["--max-urls-per-domain", str(max_urls_per_domain)])

    child_env = os.environ.copy()
    thread_text = str(max(1, int(merge_threads)))
    child_env["INDEXSELECTION_POLARS_THREADS"] = thread_text
    child_env["POLARS_MAX_THREADS"] = thread_text
    child_env["RAYON_NUM_THREADS"] = thread_text

    try:
        completed = subprocess.run(
            cmd,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=child_env,
        )
    except subprocess.CalledProcessError as exc:
        stderr_text = (exc.stderr or "").strip()
        stdout_text = (exc.stdout or "").strip()
        detail = stderr_text or stdout_text or f"exit_code={exc.returncode}"
        raise RuntimeError(f"final merge subprocess failed: {detail}") from exc
    finally:
        try:
            manifest_path.unlink()
        except FileNotFoundError:
            pass

    stdout_text = (completed.stdout or "").strip()
    if not stdout_text:
        raise RuntimeError("final merge subprocess returned no output")
    json_line = stdout_text.splitlines()[-1]
    try:
        payload = json.loads(json_line)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"final merge subprocess returned invalid JSON: {stdout_text}") from exc
    return int(payload["selected_rows"])


def parse_args():
    parser = argparse.ArgumentParser(
        description="Score crawler shard data via connectorx streaming and keep the top-K URLs without materializing the full scored corpus."
    )
    parser.add_argument("--crawler-db-url", default="", help="crawlerdb URL read by connectorx.")
    parser.add_argument("--model-path", type=Path, default=DEFAULT_BINARY_MODEL_PATH, help="Trained LightGBM model path.")
    parser.add_argument(
        "--domain-freq-path",
        type=Path,
        default=DEFAULT_DOMAIN_FREQ_PATH,
        help="Train domain-frequency TSV emitted during feature extraction, with separate label=1/label=0 counts.",
    )
    parser.add_argument(
        "--selection-top-k",
        type=int,
        default=32_000_000,
        help="Number of highest-scoring URLs to keep.",
    )
    parser.add_argument(
        "--max-urls-per-domain",
        type=int,
        default=None,
        help="Maximum URLs to keep per domain. If set, caps each domain at this many URLs.",
    )
    parser.add_argument(
        "--selection-output-path",
        type=Path,
        default=DEFAULT_SELECTION_OUTPUT_PATH,
        help="Output TSV containing the selected URLs.",
    )
    parser.add_argument(
        "--summary-path",
        type=Path,
        default=DEFAULT_SELECTION_SUMMARY_PATH,
        help="Output JSON summary path.",
    )
    parser.add_argument(
        "--score-shard-dir",
        type=Path,
        default=DEFAULT_SCORE_SHARD_DIR,
        help="Directory used to store bounded top-K score shards and merge checkpoints.",
    )
    parser.add_argument("--score-batch-size", type=int, default=250_000, help="Number of URLs scored per batch.")
    parser.add_argument("--shard-start", type=int, default=0, help="First shard suffix to scan.")
    parser.add_argument("--shard-end", type=int, default=255, help="Last shard suffix to scan.")
    parser.add_argument(
        "--worker-count",
        type=int,
        default=DEFAULT_LIVE_WORKER_COUNT,
        help="Number of parallel shard scorers. Set to 1 for sequential scoring with intra-shard chunk parallelism.",
    )
    parser.add_argument(
        "--chunk-worker-count",
        type=int,
        default=32,
        help="Parallel score workers inside each shard for batch chunk scoring. Use 1 to disable intra-shard parallelism.",
    )
    parser.add_argument(
        "--db-retry-attempts",
        type=int,
        default=DEFAULT_DB_RETRY_ATTEMPTS,
        help="Retry attempts when connectorx shard reads fail.",
    )
    parser.add_argument(
        "--first-seen-before",
        default="",
        help="Optional UTC cutoff; when set, only score rows with first_seen earlier than this timestamp.",
    )
    parser.add_argument(
        "--progress-log-path",
        type=Path,
        default=None,
        help="Optional JSONL progress log path. Defaults next to the summary JSON.",
    )
    parser.add_argument(
        "--progress-every-rows",
        type=int,
        default=1_000_000,
        help="Print a shard progress line every N scored URLs. Use 0 to disable periodic worker progress logs.",
    )
    parser.add_argument(
        "--verify-row-counts",
        action="store_true",
        help="Fail if the scored row count does not match the expected connectorx row counts.",
    )
    parser.add_argument(
        "--intra-shard-workers",
        type=int,
        default=0,
        help="Extra thread-pool workers inside each shard for parallel chunk scoring. Used for large shards. 0 = disabled.",
    )
    parser.add_argument(
        "--merge-threads",
        type=int,
        default=0,
        help="Polars threads for the final global merge only. 0 = auto (up to 4).",
    )
    parser.add_argument(
        "--db-stream-method",
        type=str,
        choices=["connectorx"],
        default="connectorx",
        help="Database streaming method. Only connectorx is supported.",
    )
    return parser.parse_args()


def _cleanup_score_shards(paths: list[Path]) -> None:
    for path in paths:
        try:
            path.unlink()
        except FileNotFoundError:
            pass


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _append_progress_event(path: Path, payload: dict) -> None:
    ensure_parent(path)
    line = json.dumps(payload, ensure_ascii=False) + "\n"
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
    try:
        os.write(fd, line.encode("utf-8"))
    finally:
        os.close(fd)


def _get_worker_context(model_path: str, domain_freq_path: str):
    key = (model_path, domain_freq_path)
    ctx = _WORKER_CONTEXT.get(key)
    if ctx is not None:
        return ctx

    import lightgbm as lgb

    domain_freq = read_domain_freq(Path(domain_freq_path))
    booster = lgb.Booster(model_file=str(model_path))
    model_feature_spec = _resolve_model_feature_spec(booster)
    ctx = {
        "domain_freq": domain_freq,
        "booster": booster,
        "best_iteration": booster.best_iteration or 0,
        "model_feature_spec": model_feature_spec,
    }
    _WORKER_CONTEXT[key] = ctx
    return ctx


def _resolve_model_feature_spec(booster) -> dict:
    model_feature_names = list(booster.feature_name() or [])
    expected_feature_count = int(booster.num_feature())

    if not model_feature_names:
        if expected_feature_count == len(_CURRENT_FEATURE_COLUMNS):
            return {
                "expected_feature_count": expected_feature_count,
                "is_identity": True,
                "source_indices": (),
            }
        raise ValueError(
            "model did not expose feature names and expects "
            f"{expected_feature_count} features; current scorer builds {len(_CURRENT_FEATURE_COLUMNS)}"
        )

    if len(model_feature_names) != expected_feature_count:
        raise ValueError(
            "model feature metadata is inconsistent: "
            f"num_feature={expected_feature_count} feature_names={len(model_feature_names)}"
        )

    if model_feature_names == list(_CURRENT_FEATURE_COLUMNS):
        return {
            "expected_feature_count": expected_feature_count,
            "is_identity": True,
            "source_indices": (),
        }

    unknown_features = [name for name in model_feature_names if name not in _CURRENT_FEATURE_INDEX]
    if unknown_features:
        raise ValueError(
            "model uses unsupported feature names: "
            + ", ".join(sorted(set(unknown_features)))
        )

    source_indices = tuple(
        _CURRENT_FEATURE_INDEX[name]
        for name in model_feature_names
    )
    return {
        "expected_feature_count": expected_feature_count,
        "is_identity": False,
        "source_indices": source_indices,
    }


def _align_feature_matrix_for_model(x_batch, model_feature_spec: dict):
    if model_feature_spec["is_identity"]:
        return x_batch

    import numpy as np

    expected_feature_count = int(model_feature_spec["expected_feature_count"])
    if x_batch.shape[1] != len(_CURRENT_FEATURE_COLUMNS):
        raise ValueError(
            "unexpected scorer feature width before model alignment: "
            f"got {x_batch.shape[1]} expected {len(_CURRENT_FEATURE_COLUMNS)}"
        )

    aligned = np.empty((x_batch.shape[0], expected_feature_count), dtype=x_batch.dtype)
    for aligned_idx, source_idx in enumerate(model_feature_spec["source_indices"]):
        aligned[:, aligned_idx] = x_batch[:, source_idx]
    return aligned


def _score_chunk_to_shard(
    *,
    booster,
    best_iteration: int,
    domain_freq: dict[int, dict[str, int]],
    model_feature_spec: dict,
    shard_dir: Path,
    shard: int,
    chunk_idx: int,
    urls: list[str],
    first_seen_values: list[str],
    selection_top_k: int,
    min_score_threshold: float | None = None,
) -> tuple[Path | None, int, float | None]:
    x_batch = build_feature_matrix(
        urls,
        domain_freq,
        progress_label=f"score_s{shard:03d}_c{chunk_idx:05d}",
    )
    x_batch = _align_feature_matrix_for_model(x_batch, model_feature_spec)
    scores = booster.predict(x_batch, num_iteration=best_iteration if best_iteration > 0 else None)
    import numpy as np

    if min_score_threshold is not None:
        keep_idx = np.flatnonzero(scores >= min_score_threshold)
        if keep_idx.size == 0:
            return None, 0, None
        if keep_idx.size != len(urls):
            scores = scores[keep_idx]
            urls = [urls[int(idx)] for idx in keep_idx]
            first_seen_values = [first_seen_values[int(idx)] for idx in keep_idx]
    kept_rows = len(urls)
    if kept_rows == 0:
        return None, 0, None

    min_kept_score = None
    if kept_rows > selection_top_k:
        min_kept_score = float(np.partition(scores, -selection_top_k)[-selection_top_k])
        kept_rows = selection_top_k
    elif kept_rows == selection_top_k:
        min_kept_score = float(np.min(scores))

    chunk_path = write_score_shard(
        shard_dir,
        chunk_idx,
        urls,
        first_seen_values,
        scores,
        file_name=f"shard_{shard:03d}_batch_{chunk_idx:05d}.parquet",
        top_k=selection_top_k,
    )
    return chunk_path, kept_rows, min_kept_score


def _score_dump_chunk_worker(
    *,
    shard_dir: Path,
    shard: int,
    chunk_idx: int,
    urls: list[str],
    first_seen_values: list[str],
    model_path: str,
    domain_freq_path: str,
    selection_top_k: int,
    min_score_threshold: float | None = None,
) -> tuple[str | None, int, int, float | None]:
    ctx = _get_worker_context(model_path, domain_freq_path)
    input_rows = len(urls)
    chunk_path, kept_rows, min_kept_score = _score_chunk_to_shard(
        booster=ctx["booster"],
        best_iteration=int(ctx["best_iteration"]),
        domain_freq=ctx["domain_freq"],
        model_feature_spec=ctx["model_feature_spec"],
        shard_dir=shard_dir,
        shard=shard,
        chunk_idx=chunk_idx,
        urls=urls,
        first_seen_values=first_seen_values,
        selection_top_k=selection_top_k,
        min_score_threshold=min_score_threshold,
    )
    return str(chunk_path) if chunk_path is not None else None, input_rows, kept_rows, min_kept_score


def _cleanup_shard_chunk_files(shard_dir: Path, shard: int) -> None:
    _cleanup_score_shards(list(shard_dir.glob(f"shard_{shard:03d}_batch_*.parquet")))


def _wrap_worker_base_exception(exc: BaseException, *, context: str) -> RuntimeError:
    detail = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    return RuntimeError(f"{context}\n{detail}".rstrip())


def _score_row_source_shard(
    *,
    row_iter,
    source_meta: dict,
    shard_dir: Path,
    shard: int,
    score_batch_size: int,
    model_path: str,
    domain_freq_path: str,
    selection_top_k: int,
    chunk_worker_count: int,
    progress_every_rows: int,
    verify_row_counts: bool,
    chunk_executor=None,
    intra_shard_workers: int = 0,
    on_rows_read=None,
) -> dict:
    table_name = f"url_state_current_{shard:03d}"
    started_at = perf_counter()
    _cleanup_shard_chunk_files(shard_dir, shard)
    source_label = source_meta.get("db_stream_method", "dump") if source_meta.get("source_streamed") else "dump"
    rows_read = 0
    rows_scored = 0
    batch_urls: list[str] = []
    batch_first_seen: list[str] = []
    chunk_idx = 0
    score_chunks = 0
    pending_chunk_runs: list[_SortedRun] = []
    score_run_rows = 0
    shard_min_score_threshold = None
    next_progress_rows = progress_every_rows if progress_every_rows > 0 else None

    def collect_chunk_result(
        chunk_path_text: str | None,
        row_count: int,
        kept_rows: int,
        min_kept_score: float | None,
    ) -> None:
        nonlocal rows_scored, score_chunks, shard_min_score_threshold
        rows_scored += int(row_count)
        score_chunks += 1
        if chunk_path_text and kept_rows > 0:
            pending_chunk_runs.append(_SortedRun(Path(chunk_path_text), int(kept_rows), min_kept_score))
        if min_kept_score is not None:
            if shard_min_score_threshold is None or min_kept_score > shard_min_score_threshold:
                shard_min_score_threshold = float(min_kept_score)

    effective_chunk_workers = max(1, int(chunk_worker_count))
    max_in_flight_chunks = effective_chunk_workers * 2
    pending_futures: dict = {}
    active_chunk_executor = chunk_executor
    owns_chunk_executor = active_chunk_executor is None and effective_chunk_workers > 1
    if owns_chunk_executor:
        active_chunk_executor = _make_process_pool(
            max_workers=effective_chunk_workers,
            max_tasks_per_child=_CHUNK_MAX_TASKS_PER_CHILD,
        )

    def finalize_score_runs() -> list[_SortedRun]:
        nonlocal score_run_rows
        if not pending_chunk_runs:
            return []
        score_run_rows = sum(run.row_count for run in pending_chunk_runs)
        return list(pending_chunk_runs)

    def dispatch_chunk(chunk_urls: list[str], chunk_first_seen: list[str]) -> None:
        nonlocal chunk_idx
        if not chunk_urls:
            return
        chunk_idx_value = chunk_idx
        chunk_idx += 1
        min_score_threshold = shard_min_score_threshold
        if active_chunk_executor is None:
            chunk_path_text, row_count, kept_rows, min_kept_score = _score_dump_chunk_worker(
                shard_dir=shard_dir,
                shard=shard,
                chunk_idx=chunk_idx_value,
                urls=chunk_urls,
                first_seen_values=chunk_first_seen,
                model_path=model_path,
                domain_freq_path=domain_freq_path,
                selection_top_k=selection_top_k,
                min_score_threshold=min_score_threshold,
            )
            collect_chunk_result(chunk_path_text, row_count, kept_rows, min_kept_score)
            return
        future = active_chunk_executor.submit(
            _score_dump_chunk_worker,
            shard_dir=shard_dir,
            shard=shard,
            chunk_idx=chunk_idx_value,
            urls=chunk_urls,
            first_seen_values=chunk_first_seen,
            model_path=model_path,
            domain_freq_path=domain_freq_path,
            selection_top_k=selection_top_k,
            min_score_threshold=min_score_threshold,
        )
        pending_futures[future] = chunk_idx_value

    def submit_batch() -> None:
        nonlocal batch_urls, batch_first_seen
        if not batch_urls:
            return
        chunk_urls = batch_urls
        chunk_first_seen = batch_first_seen
        batch_urls = []
        batch_first_seen = []
        dispatch_chunk(chunk_urls, chunk_first_seen)

    def drain_completed(*, wait_for_all: bool) -> None:
        if not pending_futures:
            return
        done, _pending = wait(
            tuple(pending_futures.keys()),
            return_when=FIRST_COMPLETED if not wait_for_all else ALL_COMPLETED,
        )
        for future in done:
            pending_futures.pop(future, None)
            chunk_path_text, row_count, kept_rows, min_kept_score = future.result()
            collect_chunk_result(chunk_path_text, row_count, kept_rows, min_kept_score)

    use_intra_thread_pool = intra_shard_workers > 0
    intra_pool = ThreadPoolExecutor(max_workers=intra_shard_workers) if use_intra_thread_pool else None
    batch_queue: list = []
    queue_lock = threading.Lock() if use_intra_thread_pool else None
    batch_submitted = threading.Event() if use_intra_thread_pool else None

    def submit_batch_from_queue() -> None:
        while True:
            with queue_lock:
                if not batch_queue:
                    if batch_submitted.is_set():
                        return
            with queue_lock:
                if not batch_queue:
                    item = None
                else:
                    item = batch_queue.pop(0)
            if item is None:
                batch_submitted.wait(timeout=0.01)
                continue
            cu, fs = item
            dispatch_chunk(cu, fs)

    try:
        if use_intra_thread_pool:
            queue_worker = intra_pool.submit(submit_batch_from_queue)
            try:
                for url, first_seen_iso in row_iter:
                    rows_read += 1
                    batch_urls.append(url)
                    batch_first_seen.append(first_seen_iso)
                    if on_rows_read is not None and rows_read % 1_000_000 == 0:
                        on_rows_read(url)
                    if next_progress_rows is not None and rows_read >= next_progress_rows:
                        print(
                            f"[score_progress] table={table_name} scored_rows={rows_read} expected_rows={rows_read if verify_row_counts else 'unknown'}",
                            flush=True,
                        )
                        next_progress_rows += progress_every_rows
                    if len(batch_urls) >= score_batch_size:
                        with queue_lock:
                            batch_queue.append((batch_urls, batch_first_seen))
                            batch_urls = []
                            batch_first_seen = []
                        batch_submitted.set()
                        batch_submitted.clear()
                        if active_chunk_executor is not None and len(pending_futures) >= max_in_flight_chunks:
                            drain_completed(wait_for_all=False)

                with queue_lock:
                    if batch_urls:
                        batch_queue.append((batch_urls, batch_first_seen))
                        batch_urls = []
                        batch_first_seen = []
                    batch_submitted.set()

                queue_worker.result()
                drain_completed(wait_for_all=True)
            finally:
                batch_submitted.set()
                if intra_pool is not None:
                    intra_pool.shutdown(wait=True, cancel_futures=True)
        else:
            for url, first_seen_iso in row_iter:
                rows_read += 1
                batch_urls.append(url)
                batch_first_seen.append(first_seen_iso)
                if on_rows_read is not None and rows_read % 1_000_000 == 0:
                    on_rows_read(url)
                if next_progress_rows is not None and rows_read >= next_progress_rows:
                    print(
                        f"[score_progress] table={table_name} scored_rows={rows_read} expected_rows={rows_read if verify_row_counts else 'unknown'}",
                        flush=True,
                    )
                    next_progress_rows += progress_every_rows
                if len(batch_urls) >= score_batch_size:
                    submit_batch()
                    if active_chunk_executor is not None and len(pending_futures) >= max_in_flight_chunks:
                        drain_completed(wait_for_all=False)

            submit_batch()
            drain_completed(wait_for_all=True)

        final_runs = finalize_score_runs()
        selected_rows = score_run_rows
    except BaseException as exc:
        if owns_chunk_executor and active_chunk_executor is not None:
            active_chunk_executor.shutdown(wait=True, cancel_futures=True)
            active_chunk_executor = None
        _cleanup_shard_chunk_files(shard_dir, shard)
        raise _wrap_worker_base_exception(exc, context=f"scoring shard {shard:03d} failed") from None
    finally:
        if owns_chunk_executor and active_chunk_executor is not None:
            active_chunk_executor.shutdown(wait=True, cancel_futures=True)

    if selected_rows == 0 or not final_runs:
        expected_candidates = rows_read if verify_row_counts else None
        verified_all_scored = None if not verify_row_counts else (expected_candidates == rows_scored)
        print(f"[score] table={table_name} candidates=0 score_chunks=0 source={source_label}", flush=True)
        return {
            "shard": shard,
            "total_candidates": int(rows_scored),
            "expected_candidates": int(expected_candidates or 0) if verify_row_counts else None,
            "verified_all_scored": verified_all_scored,
            "score_chunks": 0,
            "batch_score_shards": 0,
            "source_path": source_meta.get("source_path"),
            "source_bytes": int(source_meta.get("source_bytes", 0)),
            "source_deleted": bool(source_meta.get("source_deleted", False)),
            "dump_path": source_meta.get("dump_path"),
            "dump_deleted": bool(source_meta.get("dump_deleted", False)),
            "source_streamed": bool(source_meta.get("source_streamed", False)),
            "kept_rows": 0,
            "score_run_paths": [],
            "elapsed_seconds": round(perf_counter() - started_at, 3),
        }

    expected_candidates = rows_read if verify_row_counts else None
    verified_all_scored = None if not verify_row_counts else (expected_candidates == rows_scored)
    print(
        f"[score] table={table_name} candidates={rows_scored} score_chunks={score_chunks} forwarded_score_rows={selected_rows} verified={verified_all_scored if verified_all_scored is not None else 'na'} source={source_label}",
        flush=True,
    )
    return {
        "shard": shard,
        "total_candidates": int(rows_scored),
        "expected_candidates": int(expected_candidates or 0) if verify_row_counts else None,
        "verified_all_scored": verified_all_scored,
        "score_chunks": score_chunks,
        "batch_score_shards": score_chunks,
        "source_path": source_meta.get("source_path"),
        "source_bytes": int(source_meta.get("source_bytes", 0)),
        "source_deleted": bool(source_meta.get("source_deleted", False)),
        "dump_path": source_meta.get("dump_path"),
        "dump_deleted": bool(source_meta.get("dump_deleted", False)),
        "source_streamed": bool(source_meta.get("source_streamed", False)),
        "kept_rows": selected_rows,
        "score_run_paths": [str(run.path) for run in final_runs],
        "elapsed_seconds": round(perf_counter() - started_at, 3),
    }


def _iter_connectorx_rows(crawler_db_url: str, shard: int, cutoff_dt) -> tuple[callable, dict]:
    import polars as pl
    table_name = f"url_state_current_{shard:03d}"
    needs_cutoff = cutoff_dt is not None

    source_meta = {
        "source_path": None,
        "source_bytes": 0,
        "source_deleted": True,
        "dump_path": None,
        "dump_deleted": False,
        "source_streamed": True,
        "db_stream_method": "connectorx",
    }

    def row_iter():
        max_rows_per_batch = 10_000_000

        clean_uri = crawler_db_url.replace("postgresql+psycopg2://", "postgresql://")

        query_count = f"SELECT COUNT(*) as cnt FROM public.{table_name}"
        if needs_cutoff:
            cutoff_str = cutoff_dt.strftime("%Y-%m-%d %H:%M:%S%z")
            query_count += f" WHERE first_seen < '{cutoff_str}'"

        total_count_df = pl.read_database_uri(query=query_count, uri=clean_uri, engine="connectorx")
        total_rows = total_count_df.item(0, 0)
        del total_count_df
        print(f"[connectorx] shard={shard:03d} total_rows={total_rows}", flush=True)

        num_chunks = total_rows // max_rows_per_batch + 1
        print(f"[connectorx] shard={shard:03d} using {num_chunks} chunks", flush=True)

        for chunk_start in range(num_chunks):
            query = f"SELECT url, first_seen FROM public.{table_name}"
            if needs_cutoff:
                query += f" WHERE first_seen < '{cutoff_str}' AND ABS(hashtext(url)::bigint) % {num_chunks} = {chunk_start}"
            else:
                query += f" WHERE ABS(hashtext(url)::bigint) % {num_chunks} = {chunk_start}"

            print(f"[connectorx] shard={shard:03d} chunk={chunk_start}/{num_chunks}", flush=True)

            for attempt in range(1, 31):
                try:
                    df = pl.read_database_uri(query=query, uri=clean_uri, engine="connectorx")
                    break
                except Exception as e:
                    error_msg = str(e)
                    is_retryable = "Connection refused" in error_msg or "connection" in error_msg.lower() or "timeout" in error_msg.lower() or "refused" in error_msg.lower() or "r2d2" in error_msg.lower()
                    if not is_retryable or attempt >= 30:
                        raise
                    print(f"[connectorx] shard={shard:03d} chunk={chunk_start} retry={attempt} error={error_msg[:100]}", flush=True)
                    sleep_before_retry(attempt, max_delay=30.0)

            urls = df["url"].to_list()
            first_seens = df["first_seen"].to_list()
            valid_count = 0
            for i in range(len(urls)):
                url = urls[i]
                first_seen = first_seens[i]
                if url is None or not str(url).strip():
                    continue
                clean_url = str(url).strip()
                if first_seen is None:
                    continue
                if hasattr(first_seen, "isoformat"):
                    fs_str = first_seen.isoformat()
                else:
                    fs_str = str(first_seen)
                    if fs_str == "\\N":
                        continue
                yield clean_url, fs_str
                valid_count += 1

            del df
            del urls
            del first_seens
            print(f"[connectorx] shard={shard:03d} chunk={chunk_start} yielded {valid_count} rows", flush=True)

    return row_iter(), source_meta


def _score_connectorx_shard(
    *,
    crawler_db_url: str,
    shard_dir: Path,
    shard: int,
    cutoff_dt,
    score_batch_size: int,
    model_path: str,
    domain_freq_path: str,
    selection_top_k: int,
    chunk_worker_count: int,
    progress_every_rows: int,
    verify_row_counts: bool,
    chunk_executor=None,
    intra_shard_workers: int = 0,
) -> dict:
    row_iter, source_meta = _iter_connectorx_rows(
        crawler_db_url=crawler_db_url,
        shard=shard,
        cutoff_dt=cutoff_dt,
    )

    return _score_row_source_shard(
        row_iter=row_iter,
        source_meta=source_meta,
        shard_dir=shard_dir,
        shard=shard,
        score_batch_size=score_batch_size,
        model_path=model_path,
        domain_freq_path=domain_freq_path,
        selection_top_k=selection_top_k,
        chunk_worker_count=chunk_worker_count,
        progress_every_rows=progress_every_rows,
        verify_row_counts=verify_row_counts,
        chunk_executor=chunk_executor,
        intra_shard_workers=intra_shard_workers,
    )


def main():
    args = parse_args()
    source_mode = args.db_stream_method
    if not (args.crawler_db_url or "").strip():
        raise ValueError("--crawler-db-url is required")
    if args.selection_top_k <= 0:
        raise ValueError("--selection-top-k must be > 0")
    if args.score_batch_size <= 0:
        raise ValueError("--score-batch-size must be > 0")
    if args.worker_count <= 0:
        raise ValueError("--worker-count must be > 0")
    if args.chunk_worker_count <= 0:
        raise ValueError("--chunk-worker-count must be > 0")
    if args.db_retry_attempts <= 0:
        raise ValueError("--db-retry-attempts must be > 0")
    if args.progress_every_rows < 0:
        raise ValueError("--progress-every-rows must be >= 0")
    if args.merge_threads < 0:
        raise ValueError("--merge-threads must be >= 0")

    cutoff_dt = parse_db_datetime(args.first_seen_before) if args.first_seen_before else None
    run_label = "all_rows" if cutoff_dt is None else cutoff_dt.strftime("%Y%m%dT%H%M%SZ")
    shard_dir = args.score_shard_dir / run_label
    shard_dir.mkdir(parents=True, exist_ok=True)
    effective_merge_threads = _resolve_merge_threads(args.merge_threads)
    shards = list(range(args.shard_start, args.shard_end + 1))
    if not shards:
        raise ValueError("--shard-start must be <= --shard-end")

    print("[shard_order] fetching row estimates from database...", flush=True)
    shard_estimates = estimate_crawler_shard_rows(
        args.crawler_db_url,
        shard_start=args.shard_start,
        shard_end=args.shard_end,
        db_retry_attempts=args.db_retry_attempts,
    )
    estimate_map = {int(shard): int(row_count) for shard, row_count in shard_estimates}
    shards.sort(key=lambda shard: estimate_map.get(shard, 0), reverse=True)
    total_est_rows = sum(estimate_map.get(shard, 0) for shard in shards)
    print(
        f"[shard_order] sorted by estimated rows (desc), total_est_rows={total_est_rows:,}",
        flush=True,
    )
    for shard in shards[:5]:
        print(f"  shard {shard:03d}: ~{estimate_map.get(shard, 0):,} rows", flush=True)
    if len(shards) > 5:
        print("  ...", flush=True)
        for shard in shards[-3:]:
            print(f"  shard {shard:03d}: ~{estimate_map.get(shard, 0):,} rows", flush=True)
    requested_worker_count = int(args.worker_count)
    requested_chunk_worker_count = int(args.chunk_worker_count)
    intra_shard_workers = max(0, int(args.intra_shard_workers))
    cpu_parallelism_budget = _cpu_parallelism_budget()

    if requested_worker_count > 1 and len(shards) > 1:
        effective_worker_count = min(requested_worker_count, len(shards), cpu_parallelism_budget)
        remaining_cpu_budget = max(0, cpu_parallelism_budget - effective_worker_count)
        per_shard_chunk_capacity = remaining_cpu_budget // effective_worker_count if effective_worker_count > 0 else 0
        effective_chunk_worker_count = 1
        parallelism_mode = "shard_parallel"
        uses_shard_process_pool = True
        nested_process_pools_avoided = True

        if requested_chunk_worker_count > 1 and per_shard_chunk_capacity > 1:
            effective_chunk_worker_count = min(requested_chunk_worker_count, per_shard_chunk_capacity)
            parallelism_mode = "shard_parallel+chunk_parallel"
            nested_process_pools_avoided = False
            print(
                f"[parallelism] requested_shard_workers={requested_worker_count} effective_shard_workers={effective_worker_count} requested_chunk_workers={requested_chunk_worker_count} effective_chunk_workers_per_shard={effective_chunk_worker_count} cpu_budget={cpu_parallelism_budget} mode=shard_parallel+chunk_parallel",
                flush=True,
            )
        elif intra_shard_workers > 0:
            nested_process_pools_avoided = requested_chunk_worker_count > 1
            parallelism_mode = "shard_parallel+intra_thread_pool"
            print(
                f"[parallelism] requested_shard_workers={requested_worker_count} effective_shard_workers={effective_worker_count} intra_shard_threads={intra_shard_workers} cpu_budget={cpu_parallelism_budget} mode=shard_parallel+intra_thread_pool",
                flush=True,
            )
        else:
            if requested_chunk_worker_count > 1:
                print(
                    f"[parallelism] hybrid_chunk_parallelism_disabled requested_chunk_workers={requested_chunk_worker_count} cpu_budget={cpu_parallelism_budget} shard_workers={effective_worker_count}",
                    flush=True,
                )
            print(
                f"[parallelism] requested_shard_workers={requested_worker_count} effective_shard_workers={effective_worker_count} chunk_workers=1 cpu_budget={cpu_parallelism_budget} mode=shard_parallel",
                flush=True,
            )
    else:
        effective_worker_count = 1
        effective_chunk_worker_count = min(requested_chunk_worker_count, cpu_parallelism_budget)
        parallelism_mode = "chunk_parallel_only"
        uses_shard_process_pool = False
        nested_process_pools_avoided = requested_worker_count > 1
        print(
            f"[parallelism] requested_shard_workers={requested_worker_count} effective_shard_workers=1 chunk_workers={effective_chunk_worker_count} cpu_budget={cpu_parallelism_budget} mode=chunk_parallel_only",
            flush=True,
        )
    reuses_chunk_executor = (not uses_shard_process_pool) and effective_chunk_worker_count > 1

    progress_log_path = args.progress_log_path or args.summary_path.with_name(f"{args.summary_path.stem}_progress.jsonl")
    progress_log_path = progress_log_path.resolve()
    if progress_log_path.exists():
        progress_log_path.unlink()
    _append_progress_event(
        progress_log_path,
        {
            "event": "run_started",
            "timestamp": _utc_now_iso(),
            "run_label": run_label,
            "source_mode": source_mode,
            "shard_start": int(args.shard_start),
            "shard_end": int(args.shard_end),
            "total_shards": int(len(shards)),
            "requested_worker_count": int(requested_worker_count),
            "requested_chunk_worker_count": int(requested_chunk_worker_count),
            "worker_count": int(effective_worker_count),
            "chunk_worker_count": int(effective_chunk_worker_count),
            "cpu_parallelism_budget": int(cpu_parallelism_budget),
            "uses_shard_process_pool": bool(uses_shard_process_pool),
            "nested_process_pools_avoided": bool(nested_process_pools_avoided),
            "parallelism_mode": parallelism_mode,
            "process_start_method": _POOL_START_METHOD,
            "chunk_max_tasks_per_child": int(_CHUNK_MAX_TASKS_PER_CHILD),
            "shard_max_tasks_per_child": int(_SHARD_MAX_TASKS_PER_CHILD),
            "reuses_chunk_executor": bool(reuses_chunk_executor),
            "intra_shard_workers": int(intra_shard_workers),
            "selection_top_k": int(args.selection_top_k),
            "score_batch_size": int(args.score_batch_size),
            "merge_threads": int(effective_merge_threads),
            "verify_row_counts": bool(args.verify_row_counts),
            "progress_every_rows": int(args.progress_every_rows),
        },
    )

    shard_results = []
    total_candidates = 0
    total_expected_candidates = 0
    total_score_chunks = 0
    verified_shards = 0
    score_runs: list[_SortedRun] = []
    scoring_started_logged = False

    def make_shard_result(result: dict) -> dict:
        return {
            "shard": int(result["shard"]),
            "total_candidates": int(result["total_candidates"]),
            "expected_candidates": int(result["expected_candidates"]) if result["expected_candidates"] is not None else None,
            "verified_all_scored": result["verified_all_scored"],
            "score_chunks": int(result["score_chunks"]),
            "batch_score_shards": int(result["score_chunks"]),
            "source_streamed": bool(result.get("source_streamed", False)),
            "source_path": result.get("source_path"),
            "source_bytes": int(result.get("source_bytes", 0)),
            "source_deleted": bool(result.get("source_deleted", False)),
            "kept_rows": int(result["kept_rows"]),
            "elapsed_seconds": float(result["elapsed_seconds"]),
        }

    def handle_scored_result(result: dict) -> None:
        nonlocal total_candidates, total_expected_candidates, total_score_chunks, verified_shards
        shard_result = make_shard_result(result)
        shard_results.append(shard_result)

        total_candidates += int(result["total_candidates"])
        if result["expected_candidates"] is not None:
            total_expected_candidates += int(result["expected_candidates"])
        total_score_chunks += int(result["score_chunks"])
        if result["verified_all_scored"] is True:
            verified_shards += 1

        for score_run_path_text in result.get("score_run_paths", []):
            score_runs.append(_SortedRun(Path(score_run_path_text), 0, None))

        progress_event = {
            "event": "shard_scored",
            "timestamp": _utc_now_iso(),
            "shard": int(result["shard"]),
            "completed_shards": len(shard_results),
            "total_shards": len(shards),
            "completed_fraction": len(shard_results) / len(shards),
            "scored_rows": int(result["total_candidates"]),
            "expected_rows": int(result["expected_candidates"]) if result["expected_candidates"] is not None else None,
            "verified_all_scored": result["verified_all_scored"],
            "score_chunks": int(result["score_chunks"]),
            "batch_score_shards": int(result["score_chunks"]),
            "source_streamed": bool(result.get("source_streamed", False)),
            "source_path": result.get("source_path"),
            "source_bytes": int(result.get("source_bytes", 0)),
            "source_deleted": bool(result.get("source_deleted", False)),
            "kept_rows": int(result["kept_rows"]),
            "elapsed_seconds": float(result["elapsed_seconds"]),
            "cumulative_scored_rows": total_candidates,
            "cumulative_selected_rows": 0,
        }

        nonlocal scoring_started_logged
        if not scoring_started_logged:
            _append_progress_event(
                progress_log_path,
                {
                    "event": "scoring_started",
                    "timestamp": _utc_now_iso(),
                    "first_shard": int(result["shard"]),
                },
            )
            scoring_started_logged = True

        if args.verify_row_counts and result["verified_all_scored"] is not True:
            progress_event["event"] = "shard_verification_failed"
            _append_progress_event(progress_log_path, progress_event)
            _cleanup_score_shards([Path(path_text) for path_text in result.get("score_run_paths", [])])
            raise ValueError(
                f"Shard {int(result['shard']):03d} verification failed: scored_rows={int(result['total_candidates'])} expected_rows={int(result['expected_candidates'])}"
            )

        _append_progress_event(progress_log_path, progress_event)

    shared_chunk_executor = (
        _make_process_pool(
            max_workers=effective_chunk_worker_count,
            max_tasks_per_child=_CHUNK_MAX_TASKS_PER_CHILD,
        )
        if reuses_chunk_executor
        else None
    )

    print(
        f"[worker_lifecycle] process_start_method={_POOL_START_METHOD} chunk_max_tasks_per_child={_CHUNK_MAX_TASKS_PER_CHILD} shard_max_tasks_per_child={_SHARD_MAX_TASKS_PER_CHILD}",
        flush=True,
    )

    print(f"[db_stream] using connectorx mode for {len(shards)} shards", flush=True)
    try:
        if uses_shard_process_pool:
            shard_executor = _make_process_pool(
                max_workers=effective_worker_count,
                max_tasks_per_child=_SHARD_MAX_TASKS_PER_CHILD,
            )
            pending_score_futures = {
                shard_executor.submit(
                    _score_connectorx_shard,
                    crawler_db_url=args.crawler_db_url,
                    shard_dir=shard_dir,
                    shard=shard,
                    cutoff_dt=cutoff_dt,
                    score_batch_size=args.score_batch_size,
                    model_path=str(args.model_path),
                    domain_freq_path=str(args.domain_freq_path),
                    selection_top_k=args.selection_top_k,
                    chunk_worker_count=effective_chunk_worker_count,
                    progress_every_rows=args.progress_every_rows,
                    verify_row_counts=args.verify_row_counts,
                ): shard
                for shard in shards
            }
            try:
                while pending_score_futures:
                    done, _ = wait(pending_score_futures.keys(), return_when=ALL_COMPLETED)
                    for future in done:
                        shard = pending_score_futures.pop(future)
                        try:
                            result = future.result()
                            handle_scored_result(result)
                        except Exception as exc:
                            print(f"[connectorx] shard={shard:03d} failed: {exc}, will retry", flush=True)
                            retry_future = shard_executor.submit(
                                _score_connectorx_shard,
                                crawler_db_url=args.crawler_db_url,
                                shard_dir=shard_dir,
                                shard=shard,
                                cutoff_dt=cutoff_dt,
                                score_batch_size=args.score_batch_size,
                                model_path=str(args.model_path),
                                domain_freq_path=str(args.domain_freq_path),
                                selection_top_k=args.selection_top_k,
                                chunk_worker_count=effective_chunk_worker_count,
                                progress_every_rows=args.progress_every_rows,
                                verify_row_counts=args.verify_row_counts,
                            )
                            pending_score_futures[retry_future] = shard
            finally:
                shard_executor.shutdown(wait=True, cancel_futures=True)
                print(f"[connectorx] finished processing {len(shards)} shards, shard_results={len(shard_results)}", flush=True)
        else:
            with ThreadPoolExecutor(max_workers=1) as shard_prefetch_pool:
                future_shard = None
                shard_idx = 0
                while shard_idx < len(shards):
                    shard = shards[shard_idx]
                    if future_shard is None:
                        result = _score_connectorx_shard(
                            crawler_db_url=args.crawler_db_url,
                            shard_dir=shard_dir,
                            shard=shard,
                            cutoff_dt=cutoff_dt,
                            score_batch_size=args.score_batch_size,
                            model_path=str(args.model_path),
                            domain_freq_path=str(args.domain_freq_path),
                            selection_top_k=args.selection_top_k,
                            chunk_worker_count=effective_chunk_worker_count,
                            progress_every_rows=args.progress_every_rows,
                            verify_row_counts=args.verify_row_counts,
                            chunk_executor=shared_chunk_executor,
                        )
                    else:
                        result = future_shard.result()
                    handle_scored_result(result)
                    shard_idx += 1
                    if shard_idx < len(shards):
                        next_shard = shards[shard_idx]
                        future_shard = shard_prefetch_pool.submit(
                            _score_connectorx_shard,
                            crawler_db_url=args.crawler_db_url,
                            shard_dir=shard_dir,
                            shard=next_shard,
                            cutoff_dt=cutoff_dt,
                            score_batch_size=args.score_batch_size,
                            model_path=str(args.model_path),
                            domain_freq_path=str(args.domain_freq_path),
                            selection_top_k=args.selection_top_k,
                            chunk_worker_count=effective_chunk_worker_count,
                            progress_every_rows=args.progress_every_rows,
                            verify_row_counts=args.verify_row_counts,
                            chunk_executor=shared_chunk_executor,
                        )
    finally:
        if shared_chunk_executor is not None:
            shared_chunk_executor.shutdown(wait=True, cancel_futures=True)

    if not score_runs:
        raise ValueError("No crawlerdb rows were scanned for scoring")

    _append_progress_event(
        progress_log_path,
        {
            "event": "merge_started",
            "timestamp": _utc_now_iso(),
            "total_score_run_files": len(score_runs),
            "merge_threads": int(effective_merge_threads),
        },
    )

    score_runs.sort(key=lambda run: run.path.name)
    selected_rows = _merge_tsv_in_subprocess(
        run_paths=[run.path for run in score_runs],
        output_path=args.selection_output_path,
        top_k=args.selection_top_k,
        input_row_count=sum(item["kept_rows"] for item in shard_results),
        merge_threads=effective_merge_threads,
        max_urls_per_domain=args.max_urls_per_domain,
    )

    _append_progress_event(
        progress_log_path,
        {
            "event": "merge_completed",
            "timestamp": _utc_now_iso(),
            "merged_rows": int(selected_rows),
            "merge_threads": int(effective_merge_threads),
        },
    )

    _cleanup_score_shards([run.path for run in score_runs])

    _append_progress_event(
        progress_log_path,
        {
            "event": "selection_written",
            "timestamp": _utc_now_iso(),
            "selection_path": str(args.selection_output_path),
            "selection_rows": int(selected_rows),
            "selection_top_k": int(args.selection_top_k),
        },
    )

    summary = {
        "source_mode": source_mode,
        "crawler_db_url": args.crawler_db_url or None,
        "model_path": str(args.model_path),
        "domain_freq_path": str(args.domain_freq_path),
        "selection_output_path": str(args.selection_output_path),
        "score_shard_dir": str(shard_dir),
        "score_chunks": int(total_score_chunks),
        "score_shards": int(total_score_chunks),
        "selection_top_k": int(args.selection_top_k),
        "selection_rows_written": int(selected_rows),
        "selection_total_candidates": int(total_candidates),
        "selection_expected_total_candidates": int(total_expected_candidates) if args.verify_row_counts else None,
        "first_seen_before": cutoff_dt.isoformat() if cutoff_dt else None,
        "shard_start": int(args.shard_start),
        "shard_end": int(args.shard_end),
        "requested_worker_count": int(requested_worker_count),
        "requested_chunk_worker_count": int(requested_chunk_worker_count),
        "worker_count": int(effective_worker_count),
        "chunk_worker_count": int(effective_chunk_worker_count),
        "cpu_parallelism_budget": int(cpu_parallelism_budget),
        "process_start_method": _POOL_START_METHOD,
        "chunk_max_tasks_per_child": int(_CHUNK_MAX_TASKS_PER_CHILD),
        "shard_max_tasks_per_child": int(_SHARD_MAX_TASKS_PER_CHILD),
        "merge_threads": int(effective_merge_threads),
        "uses_shard_process_pool": bool(uses_shard_process_pool),
        "reuses_chunk_executor": bool(reuses_chunk_executor),
        "nested_process_pools_avoided": bool(nested_process_pools_avoided),
        "parallelism_mode": parallelism_mode,
        "db_retry_attempts": int(args.db_retry_attempts),
        "progress_log_path": str(progress_log_path),
        "progress_every_rows": int(args.progress_every_rows),
        "verify_row_counts": bool(args.verify_row_counts),
        "verified_shards": int(verified_shards),
        "verified_all_scored": bool((not args.verify_row_counts) or (verified_shards == len(shards) and total_candidates == total_expected_candidates)),
        "storage_strategy": "per_shard_connectorx_stream_then_chunk_parquet_then_polars_global_topk",
        "db_stream_method": "connectorx",
        "reuses_worker_connections": False,
        "shards_processed": int(len(shards)),
        "shards_with_candidates": int(sum(1 for item in shard_results if item["total_candidates"] > 0)),
        "shards": shard_results,
    }
    _append_progress_event(
        progress_log_path,
        {
            "event": "run_completed",
            "timestamp": _utc_now_iso(),
            "total_shards": int(len(shards)),
            "total_candidates": int(total_candidates),
            "expected_total_candidates": int(total_expected_candidates) if args.verify_row_counts else None,
            "selected_rows": int(selected_rows),
            "verified_shards": int(verified_shards),
            "verified_all_scored": summary["verified_all_scored"],
        },
    )
    write_json(args.summary_path, summary)
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
