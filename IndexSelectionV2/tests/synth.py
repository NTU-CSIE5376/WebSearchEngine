"""Synthetic work directory with a planted golden signal.

Golden propensity rises with a latent domain quality, with being a site root and
with external inlinks, so a correct pipeline must beat random and link-only
rankings. Pages are crawled over time, so pools grow from batch to batch.
"""

from __future__ import annotations

import math
import random
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
from conftest import replace, small_cfg

from isel.derive import derive_positives, derive_sample
from isel.golden import golden_table
from isel.io import atomic_write_json, write_parquet_atomic
from isel.pull import FULL_SCHEMA

START = datetime(2026, 1, 1, tzinfo=UTC)
FIRST_BATCH = datetime(2026, 2, 1, tzinfo=UTC)
SNAPSHOT = datetime(2026, 5, 20, tzinfo=UTC)


@dataclass
class World:
    pages: list[dict]
    golden_rows: list[tuple]
    batches: dict[int, datetime]


def _page(rng: random.Random, domain: str, quality: float, i: int, shard: int) -> dict:
    root = i == 0
    path = "/" if root else f"/{rng.choice(['news', 'wiki', 'shop', 'blog'])}/{i}"
    ext = int(rng.expovariate(1.0 / (1 + 4 * quality)) if rng.random() < 0.5 else 0)
    inl = ext + int(rng.expovariate(0.2))
    fetched = START + timedelta(seconds=rng.uniform(0, (SNAPSHOT - START).total_seconds() - 1))
    return {
        "url": f"https://www.{domain}{path}", "domain_id": shard, "last_fetch_ok": fetched,
        "title_len": None if rng.random() < 0.1 else rng.randint(5, 80),
        "has_anchor": rng.random() < 0.7, "discovery_source_type": 1, "robots_bits": 1,
        "is_redirect": False, "redirect_hop_count": 0, "has_json_ld": rng.random() < 0.3,
        "hreflang_count": 0, "has_last_modified": rng.random() < 0.5,
        "has_cache_control": rng.random() < 0.5, "inlink_count_approx": inl,
        "inlink_count_external": ext, "url_score": rng.random(), "source": 0,
        "shard": shard % 4, "_quality": quality, "_root": root,
    }


def make_world(seed: int = 1, domains: int = 120, pages_per_domain: int = 60,
               n_batches: int = 9, golden_per_batch: int = 240) -> World:
    rng = random.Random(seed)
    pages = []
    for d in range(domains):
        quality = rng.random() ** 3
        for i in range(pages_per_domain):
            pages.append(_page(rng, f"site{d}.com", quality, i, d))
    weights = [math.exp(4 * p["_quality"] + 1.5 * p["_root"]
                        + 0.6 * math.log1p(p["inlink_count_external"])) for p in pages]
    batches = {b: FIRST_BATCH + timedelta(days=14 * (b - 1)) for b in range(1, n_batches + 1)}
    golden_rows = []
    qid = 0
    for b, created in batches.items():
        chosen = rng.choices(pages, weights=weights, k=golden_per_batch)
        for j in range(0, len(chosen), 8):
            qid += 1
            tag = ["head"] if (j // 8) % 2 == 0 else ["random"]
            for rank, p in enumerate(chosen[j:j + 8], start=1):
                raw = p["url"]
                if rng.random() < 0.1:
                    raw = raw + "#top"
                golden_rows.append((b, created, qid, f"q{qid}", 100, tag, rank, raw))
        # Some golden URLs are never crawled.
        golden_rows.append((b, created, qid, f"q{qid}", 100, ["head"], 9,
                            f"https://uncrawled{b}.example/x"))
    return World(pages, golden_rows, batches)


def crawl_rows(world: World) -> list[dict]:
    return [{k: v for k, v in p.items() if not k.startswith("_")} for p in world.pages]


def write_export(world: World, work_dir: Path, extra: list[dict] = (),
                 started_at: datetime = SNAPSHOT) -> None:
    """Writes every crawled page plus `extra` as a finished `pull-full`, two files per shard."""
    pages = pa.Table.from_pylist(crawl_rows(world) + list(extra), schema=FULL_SCHEMA)
    shards = sorted(set(pages.column("shard").to_pylist()))
    for shard in shards:
        table = pages.filter(pc.equal(pages.column("shard"), shard))
        half = table.num_rows // 2
        for lo, part in ((0, table.slice(0, half)), (1, table.slice(half))):
            write_parquet_atomic(part, work_dir / "full" / "export" / f"shard={shard:03d}"
                                 / f"part-{lo:010d}-{lo + 1:010d}.parquet")
    atomic_write_json(work_dir / "full" / "export" / "_meta.json", {
        "started_at": started_at.isoformat(), "finished_at": started_at.isoformat(),
        "frozen_probe": {"tuple_changes": 0}, "shards_done": shards})


def write_work_dir(world: World, work_dir: Path, sample_percent: float = 100.0) -> None:
    """Writes golden, dims and an export, then derives the sample and positives from it."""
    write_parquet_atomic(golden_table(world.golden_rows), work_dir / "golden" / "golden.parquet")
    write_parquet_atomic(pa.table({"host": pa.array([], pa.string()),
                                   "migrated_at": pa.array([], pa.timestamp("us", tz="UTC"))}),
                         work_dir / "dims" / "split_hosts.parquet")
    write_export(world, work_dir)
    cfg = replace(small_cfg(work_dir), "full", sample_percent=sample_percent)
    derive_sample(cfg, work_dir)
    derive_positives(cfg, work_dir)
