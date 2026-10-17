"""Experiments: forward-chained backtest, frozen training, forward evaluation, selection."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

from isel.config import Config
from isel.dataset import to_numpy, train_batches, training_matrix
from isel.evaluate import (
    Golden,
    calibration,
    evaluate_batch,
    golden_by_tag,
    pooled,
    render_report,
)
from isel.features import FeatureStore, open_store, resolve_set
from isel.golden import PSEUDO, read_golden
from isel.io import atomic_write_json, code_version, sql_str, utc_id
from isel.manifest import write_run
from isel.model import (
    FORWARD_ONLY,
    RULE_BASELINES,
    TrainedModel,
    case_control_offset,
    fit_lgbm,
    fit_logreg,
    rule_score,
)


def model_name(feature_set: str) -> str:
    return f"lgbm:{feature_set}"


def _importance(model: TrainedModel, top: int) -> list[tuple[str, float]]:
    if model.booster is None:
        return []
    gains = model.booster.feature_importance(importance_type="gain")
    pairs = sorted(zip(model.features, gains.tolist(), strict=True), key=lambda p: -p[1])
    return pairs[:top]


def _fit_all(cfg: Config, store: FeatureStore, batches: list[int], feature_sets: list[str],
             baselines: list[str]) -> dict[str, TrainedModel]:
    sets = list(dict.fromkeys([*feature_sets,
                               *(["v1_features"] if "v1_features" in baselines else [])]))
    needed = list(dict.fromkeys(f for s in sets for f in resolve_set(cfg, s)))
    m = training_matrix(store, batches, needed)
    offset = case_control_offset(store.rate, cfg.dataset.neg_rate)
    official = [store.batch(b) for b in batches if store.batch(b).official]
    valid = max(official, key=lambda b: b.created_at).batch_id if official else None
    models = {model_name(fs): fit_lgbm(cfg, m, resolve_set(cfg, fs), offset, valid)
              for fs in feature_sets}
    if "v1_features" in baselines:
        models["v1_features"] = fit_lgbm(cfg, m, resolve_set(cfg, "v1_features"), offset,
                                         valid, kind="v1_features")
    if "logreg" in baselines:
        models["logreg"] = fit_logreg(cfg, m, resolve_set(cfg, cfg.model.feature_set), offset)
    if "later_unlabeled" in baselines:
        # Ablation: negatives that turn golden in a later training batch leave the data.
        models["later_unlabeled"] = fit_lgbm(cfg, m.subset(~m.later_golden),
                                             resolve_set(cfg, cfg.model.feature_set), offset,
                                             valid)
    return models


def _batch_result(cfg: Config, store: FeatureStore, batch: int, frame: pa.Table,
                  models: dict[str, TrainedModel], raw: dict[str, np.ndarray], rules: list[str],
                  reference: str, golden_rows: list[dict[str, Any]]
                  ) -> tuple[dict[str, Any], dict[str, dict[str, set]], dict[str, Golden]]:
    """Scores, metrics, calibration and model notes of one evaluated batch."""
    scores = {**raw, **{r: rule_score(cfg, r, frame) for r in rules}}
    golden = golden_by_tag(golden_rows, batch, cfg.golden.tags, key_prefix=batch)
    result, hits = evaluate_batch(cfg, frame, scores, golden, store.rate, reference)
    b = store.batch(batch)
    forward = b.created_at > store.snapshot_at
    top = cfg.eval.importance_top_n
    result.update(
        as_of=b.as_of, created_at=b.created_at, forward=forward,
        leaky_in_backtest=[] if forward else [r for r in rules if r in FORWARD_ONLY],
        calibration={n: calibration(m.probability(raw[n]), frame, store.rate, golden)
                     for n, m in models.items() if m.booster is not None},
        models={n: {**m.meta, "importance_gain_top": _importance(m, top)}
                for n, m in models.items()})
    return result, hits, golden


def _evaluate(cfg: Config, store: FeatureStore, batch: int, models: dict[str, TrainedModel],
              rules: list[str], reference: str, golden_rows: list[dict[str, Any]]
              ) -> tuple[dict[str, Any], dict[str, dict[str, set]], dict[str, Any]]:
    frame = store.frame(batch, "eval")
    raw = {name: m.raw_score(frame) for name, m in models.items()}
    result, hits, golden = _batch_result(cfg, store, batch, frame, models, raw, rules, reference,
                                         golden_rows)
    return result, hits, {tag: g.queries for tag, g in golden.items()}


def run_backtest(cfg: Config, work_dir: Path, feature_sets: list[str],
                 baselines: list[str]) -> Path:
    """Trains on batches before each test batch and evaluates every scorer on it."""
    store = open_store(cfg, work_dir)
    golden_rows = read_golden(work_dir).to_pylist()
    reference = model_name(cfg.model.feature_set)
    if cfg.model.feature_set not in feature_sets:
        feature_sets = [cfg.model.feature_set, *feature_sets]
    rules = [b for b in baselines if b in RULE_BASELINES]
    batches_out: dict[str, Any] = {}
    all_queries: dict[str, dict] = defaultdict(dict)
    all_hits: dict[str, dict[str, set]] = defaultdict(lambda: defaultdict(set))
    for t in cfg.eval.test_batches:
        train = train_batches(cfg, store, t)
        if not store.has_history(t):
            batches_out[str(t)] = {"skipped": "no golden history before this batch"}
            continue
        if len(train) < 2:
            batches_out[str(t)] = {"skipped": f"needs >= 2 training batches, has {train}"}
            continue
        models = _fit_all(cfg, store, train, feature_sets, baselines)
        result, hits, queries = _evaluate(cfg, store, t, models, rules, reference, golden_rows)
        batches_out[str(t)] = result
        for tag, q in queries.items():
            all_queries[tag].update(q)
        for name, by_tag in hits.items():
            for tag, found in by_tag.items():
                all_hits[name][tag] |= {(t, u) for u in found}
    keyed_queries = {tag: {k: {(k[0], u) for u in urls} for k, urls in q.items()}
                     for tag, q in all_queries.items()}
    results = {
        "title": "Forward-chained backtest",
        "reference": reference, "k_headline": cfg.eval.k_headline,
        "sample_rate": store.rate, "snapshot_at": store.snapshot_at,
        "config_sha256": cfg.sha256(), "code_version": code_version(), "batches": batches_out,
        "pooled": pooled(cfg, keyed_queries, all_hits, reference) if all_queries else {},
    }
    out = work_dir / "backtests" / utc_id()
    atomic_write_json(out / "results.json", results)
    (out / "report.md").write_text(render_report(results), encoding="utf-8")
    return out


def train_frozen(cfg: Config, work_dir: Path, baselines: list[str]) -> Path:
    """Trains on every batch visible at the snapshot and saves the models for later."""
    store = open_store(cfg, work_dir, next_batch=True)
    target = store.batches[-1].batch_id
    train = train_batches(cfg, store, target)
    if not train:
        raise RuntimeError("no batch with golden history is visible at the snapshot")
    models = _fit_all(cfg, store, train, [cfg.model.feature_set],
                      [b for b in baselines if b == "v1_features"])
    out = work_dir / "models" / utc_id()
    for name, model in models.items():
        model.meta.update(snapshot_at=store.snapshot_at, sample_fingerprint=store.fingerprint,
                          trained_at=datetime.now(UTC),
                          config_sha256=cfg.sha256(), code_version=code_version(), name=name,
                          use_pseudo=cfg.golden.use_pseudo)
        model.save(out / name.replace(":", "_"))
    return out


def load_models(model_dir: Path, num_threads: int) -> dict[str, TrainedModel]:
    models = {}
    for sub in sorted(p for p in model_dir.iterdir() if (p / "meta.json").exists()):
        model = TrainedModel.load(sub, num_threads)
        models[model.meta.get("name", sub.name)] = model
    if not models:
        raise FileNotFoundError(f"no models under {model_dir}")
    return models


def frozen_model(cfg: Config, store: FeatureStore, model_dir: Path) -> TrainedModel:
    """The configured feature set's model; it must come from the store's sample."""
    model = load_models(model_dir, cfg.model.num_threads)[model_name(cfg.model.feature_set)]
    if model.meta.get("sample_fingerprint") != store.fingerprint:
        raise RuntimeError(f"the model was trained on another sample "
                           f"({model.meta.get('sample_fingerprint')}); the sample is "
                           f"{store.fingerprint}")
    return model


def _align(frame: pa.Table, base: pa.Table) -> pa.Table:
    """Reorders `frame` into the (url, shard) row order of `base`."""
    keys = list(zip(frame.column("url").to_pylist(), frame.column("shard").to_pylist(),
                    strict=True))
    index = {k: i for i, k in enumerate(keys)}
    base_keys = zip(base.column("url").to_pylist(), base.column("shard").to_pylist(),
                    strict=True)
    order = [index.get(k, -1) for k in base_keys]
    if len(index) != len(keys) or len(order) != len(keys) or -1 in order:
        raise RuntimeError("the official and pseudo stores built different frames")
    return frame.take(pa.array(order, pa.int64()))


def _pseudo_overlap(work_dir: Path, snapshot_at: datetime, golden: dict[str, Golden],
                    hits: dict[str, dict[str, set]]) -> dict[str, Any]:
    """Splits each tag's golden URLs and hits by whether pseudo data known by S0 had them."""
    rows = read_golden(work_dir, include_pseudo=True)
    pseudo = rows.filter(pc.and_(pc.equal(rows.column("label_source"), PSEUDO),
                                 pc.less_equal(rows.column("available_at"),
                                               pa.scalar(snapshot_at, rows["available_at"].type))))
    known = set(pseudo.column("raw_url").to_pylist()) | set(
        pseudo.column("canonical_url").to_pylist())
    out = {}
    for tag, g in golden.items():
        inside = {u for u in g.urls if u in known or g.canonical[u] in known}
        out[tag] = {"golden_urls": len(g.urls), "in_pseudo": len(inside),
                    "hits": {name: {"in_pseudo": len(by_tag[tag] & inside),
                                    "not_in_pseudo": len(by_tag[tag] - inside)}
                             for name, by_tag in hits.items()}}
    return out


def forward_eval(cfg: Config, work_dir: Path, model_dirs: list[Path], batch: int,
                 baselines: list[str]) -> Path:
    """Evaluates frozen models on a batch created after the snapshot.

    Models trained with pseudo golden (`use_pseudo` in their meta) are scored on
    features built with pseudo batches and named `<name>+pseudo`; the others use
    official-only features. Frames are aligned by (url, shard), so every paired
    difference compares the same rows. Only official golden is evaluated.
    """
    groups: dict[bool, dict[str, TrainedModel]] = defaultdict(dict)
    for model_dir in model_dirs:
        for name, model in load_models(model_dir, cfg.model.num_threads).items():
            use = bool(model.meta.get("use_pseudo", False))
            groups[use][f"{name}+pseudo" if use else name] = model
    stores = {use: open_store(replace(cfg, golden=replace(cfg.golden, use_pseudo=use)),
                              work_dir)
              for use in groups}
    base_use = False not in stores   # official-only features when present
    base = stores[base_use]
    for use, models in groups.items():
        if any(m.meta.get("sample_fingerprint") != stores[use].fingerprint
               for m in models.values()):
            raise RuntimeError("a model was trained on another sample; "
                               f"the sample is {stores[use].fingerprint}")
    b = base.batch(batch)
    if b.created_at <= base.snapshot_at:
        raise RuntimeError(f"batch {batch} existed before the snapshot; use backtest")
    base_frame = base.frame(batch, "eval")
    frames = {use: base_frame if use == base_use else _align(s.frame(batch, "eval"), base_frame)
              for use, s in stores.items()}
    raw = {name: m.raw_score(frames[use]) for use, models in groups.items()
           for name, m in models.items()}
    rules = [r for r in baselines if r in RULE_BASELINES]
    reference = model_name(cfg.model.feature_set)
    reference = reference if reference in raw else next(iter(raw))
    models = {name: m for ms in groups.values() for name, m in ms.items()}
    result, hits, golden = _batch_result(cfg, base, batch, base_frame, models, raw, rules,
                                         reference, read_golden(work_dir).to_pylist())
    result["pseudo_overlap"] = _pseudo_overlap(work_dir, base.snapshot_at, golden, hits)
    results = {"title": f"Forward evaluation on batch {batch}", "reference": reference,
               "k_headline": cfg.eval.k_headline, "sample_rate": base.rate,
               "snapshot_at": base.snapshot_at, "model_dirs": [str(d) for d in model_dirs],
               "config_sha256": cfg.sha256(), "code_version": code_version(),
               "batches": {str(batch): result}, "pooled": {}}
    out = work_dir / "forward_evals" / f"batch={batch}_{utc_id()}"
    atomic_write_json(out / "results.json", results)
    (out / "report.md").write_text(render_report(results), encoding="utf-8")
    return out


def _reasons(model: TrainedModel, frame: pa.Table, top_n: int) -> list[list[str]]:
    if top_n == 0 or model.booster is None:
        return [[] for _ in range(frame.num_rows)]
    contrib = model.booster.predict(to_numpy(frame, model.features), pred_contrib=True,
                                    num_threads=model.num_threads)[:, :-1]
    order = np.argsort(-np.abs(contrib), axis=1)[:, :top_n]
    return [[f"{model.features[j]}:{contrib[i, j]:+.3f}" for j in row]
            for i, row in enumerate(order)]


def select_sample(cfg: Config, work_dir: Path, model_dir: Path, k: int) -> dict[str, Any]:
    """Selects the sample's share of a K-list and writes it as a manifest run.

    The rows are the sample rows whose rank is within round(K·p); this exercises
    the manifest contract at sample scale without touching any database. A URL
    sampled in two shards keeps its best-scored row.
    """
    store = open_store(cfg, work_dir, next_batch=True)
    model = frozen_model(cfg, store, model_dir)
    target = store.batches[-1].batch_id
    frame = store.frame(target, "eval")
    frame = frame.filter(frame.column("is_sample"))
    frame = frame.filter(pc.is_valid(frame.column("dkey")))
    scores = np.nan_to_num(model.raw_score(frame), nan=-np.inf)
    h = frame.column("h").to_numpy(zero_copy_only=False).astype(np.uint64)
    order = np.lexsort((h, -scores))
    keep = np.sort(order[:max(1, round(k * store.rate))])
    chosen = frame.take(pa.array(keep))
    store.con.register("chosen_rows", pa.table({
        "url": chosen.column("url"),
        "crawl_shard": chosen.column("shard"),
        "score": pa.array(scores[keep]),
        "reason": pa.array(_reasons(model, chosen, cfg.manifest.reason_top_n),
                           pa.list_(pa.string())),
    }))
    sample = sql_str(str(work_dir / "sample" / "shard=*.parquet"))
    rows_sql = (
        "SELECT c.url, c.crawl_shard, s.domain_id, c.score, c.reason FROM chosen_rows c "
        f"LEFT JOIN (SELECT url, shard, min(domain_id) AS domain_id FROM read_parquet({sample}, "
        "hive_partitioning = false) GROUP BY url, shard) s "
        "ON s.url = c.url AND s.shard = c.crawl_shard "
        "QUALIFY row_number() OVER (PARTITION BY c.url ORDER BY c.score DESC, "
        "c.crawl_shard ASC) = 1")
    try:
        return write_run(cfg, store.con, rows_sql, work_dir / "selection", utc_id(),
                         {"scope": "sample", "k": k, "sample_rate": store.rate,
                          "snapshot_at": store.snapshot_at, "model_dir": str(model_dir)})
    finally:
        store.con.unregister("chosen_rows")
