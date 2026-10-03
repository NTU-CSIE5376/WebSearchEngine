"""Merges observations of every URL: the latest observation wins.

Base rows are treated as observed `base_observation_slack_s` earlier than the
moment they were read. A history row written by a transaction that was still
open while the base chunk was read therefore wins over the stale base row.

A feature set keeps each row's `obs_rank`, so it can be the prior of the next
merge (compaction): merging it with new deltas never re-applies the slack and
gives the same result as merging the base with every delta.

History rows written in one transaction share `snapshot_at` (the ingestor
splits duplicate URLs of a batch into several statements), so `obs_seq`, the
per-shard `snapshot_id`, breaks those ties: the later statement wins.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

import duckdb

from idxsel.config import Config
from idxsel.duck import copy_atomic, sql_list
from idxsel.manifest import atomic_write_json

PriorKind = Literal["base", "merged"]


def _shard_files(root: Path, shard: int) -> list[Path]:
    return sorted((root / f"shard={shard:03d}").glob("*.parquet"))


def build_features(cfg: Config, con: duckdb.DuckDBPyConnection, prior: tuple[PriorKind, Path],
                   delta_dirs: list[Path], out_dir: Path, shards: list[int]) -> dict[str, Any]:
    """Writes one deduplicated feature file per shard; returns row counts.

    Args:
        cfg: Config.
        con: DuckDB connection.
        prior: ("base", base export dir) or ("merged", earlier feature set dir).
        delta_dirs: History delta exports not yet merged into `prior`.
        out_dir: Output feature set directory.
        shards: Shards to merge.
    """
    kind, prior_dir = prior
    slack = float(cfg.export.base_observation_slack_s)
    # A merged prior already carries obs_rank and obs_seq; only raw base rows get the slack.
    select_prior = ("SELECT *" if kind == "merged"
                    else f"SELECT *, observed_at - to_seconds({slack}) AS obs_rank, "
                         f"NULL::BIGINT AS obs_seq")
    counts: dict[str, int] = {}
    for shard in shards:
        prior_files = _shard_files(prior_dir, shard)
        delta_files = [f for d in delta_dirs for f in _shard_files(d, shard)]
        if not prior_files and not delta_files:
            continue
        parts = []
        if prior_files:
            parts.append(f"{select_prior} FROM read_parquet({sql_list(prior_files)})")
        if delta_files:
            parts.append(
                f"SELECT * EXCLUDE (snapshot_id), observed_at AS obs_rank, snapshot_id AS obs_seq "
                f"FROM read_parquet({sql_list(delta_files)})"
            )
        source = " UNION ALL BY NAME ".join(parts)
        counts[str(shard)] = copy_atomic(
            con,
            f"SELECT DISTINCT ON (url) * FROM ({source}) "
            f"ORDER BY url, obs_rank DESC, obs_seq DESC NULLS LAST, last_fetch_ok DESC NULLS LAST",
            out_dir / f"shard={shard:03d}" / "part.parquet")
    summary = {"rows": sum(counts.values()), "per_shard": counts}
    atomic_write_json(out_dir / "_SUCCESS.json", summary)
    return summary
