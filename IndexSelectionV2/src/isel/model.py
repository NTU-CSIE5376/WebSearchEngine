"""Golden-propensity models and baseline scorers.

Training is case-control: every golden row in the pool is a positive, while
negatives are sample rows (rate p) further subsampled at rate q. The logit of
the population probability is therefore the model logit plus ln(p·q); the
shift leaves the top-K order unchanged and is stored for calibration.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import lightgbm as lgb
import numpy as np
import pyarrow as pa
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from isel.config import Config
from isel.dataset import Matrix, to_numpy
from isel.features import FEATURES, assert_clean
from isel.io import atomic_write_json, read_json

FORWARD_ONLY = ("url_score", "v2_baseline_v1")
RULE_BASELINES = ("random", "inlinks", "ext", "domain_prior", "url_score", "v2_baseline_v1")


@dataclass
class TrainedModel:
    kind: str
    features: list[str]
    offset: float
    booster: lgb.Booster | None = None
    pipeline: Pipeline | None = None
    meta: dict[str, Any] = field(default_factory=dict)
    # Booster.predict ignores the training params, so the thread count is passed per call.
    num_threads: int = 0

    def raw_score(self, table: pa.Table) -> np.ndarray:
        x = to_numpy(table, self.features)
        if self.booster is not None:
            return self.booster.predict(x, raw_score=True, num_threads=self.num_threads)
        assert self.pipeline is not None
        return self.pipeline.decision_function(x)

    def probability(self, raw: np.ndarray) -> np.ndarray:
        """Population probability from a raw score (case-control prior correction)."""
        return 1.0 / (1.0 + np.exp(-(raw + self.offset)))

    def save(self, out_dir: Path) -> None:
        if self.booster is None:
            raise ValueError("only LightGBM models are saved")
        out_dir.mkdir(parents=True, exist_ok=True)
        self.booster.save_model(str(out_dir / "model.txt"))
        atomic_write_json(out_dir / "meta.json",
                          {**self.meta, "kind": self.kind, "features": self.features,
                           "offset": self.offset})

    @staticmethod
    def load(out_dir: Path, num_threads: int) -> TrainedModel:
        meta = read_json(out_dir / "meta.json")
        if meta is None:
            raise FileNotFoundError(f"{out_dir}/meta.json is missing")
        assert_clean(meta["features"])
        booster = lgb.Booster(model_file=str(out_dir / "model.txt"))
        return TrainedModel(meta["kind"], list(meta["features"]), float(meta["offset"]),
                            booster=booster, meta=meta, num_threads=num_threads)


def case_control_offset(sample_rate: float, neg_rate: float) -> float:
    return math.log(sample_rate * neg_rate)


def _columns(m: Matrix, features: list[str]) -> np.ndarray:
    assert_clean(features)
    index = [m.features.index(f) for f in features]
    return m.x[:, index]


def fit_lgbm(cfg: Config, m: Matrix, features: list[str], offset: float,
             valid_batch: int | None, kind: str = "lgbm") -> TrainedModel:
    """Trains LightGBM; `valid_batch` (the latest official batch) picks the rounds."""
    x = _columns(m, features)
    categorical = [f for f in features if FEATURES[f].categorical]
    params = dict(cfg.model.params)
    batches = sorted(set(m.batch.tolist()))

    def dataset(mask: np.ndarray) -> lgb.Dataset:
        return lgb.Dataset(x[mask], label=m.y[mask], weight=m.w[mask], feature_name=features,
                           categorical_feature=categorical, free_raw_data=True)

    rounds = cfg.model.num_boost_round
    best = rounds
    if len(batches) >= 2 and valid_batch is not None:
        valid = m.batch == valid_batch
        booster = lgb.train(params, dataset(~valid), num_boost_round=rounds,
                            valid_sets=[dataset(valid)],
                            callbacks=[lgb.early_stopping(cfg.model.early_stopping_rounds,
                                                          verbose=False)])
        best = max(1, booster.best_iteration or rounds)
        if cfg.model.refit_on_all:
            booster = lgb.train(params, dataset(np.ones(len(m.y), bool)), num_boost_round=best)
    else:
        booster = lgb.train(params, dataset(np.ones(len(m.y), bool)), num_boost_round=rounds)
    meta = {"best_iteration": best, "train_batches": batches, "valid_batch": valid_batch,
            "rows": len(m.y), "positives": int(m.y.sum())}
    return TrainedModel(kind, features, offset, booster=booster, meta=meta,
                        num_threads=cfg.model.num_threads)


def fit_logreg(cfg: Config, m: Matrix, features: list[str], offset: float) -> TrainedModel:
    pipeline = Pipeline([
        ("impute", SimpleImputer(strategy="median", add_indicator=True,
                                 keep_empty_features=True)),
        ("scale", StandardScaler()),
        ("lr", LogisticRegression(C=cfg.model.logreg_c, max_iter=cfg.model.logreg_max_iter)),
    ])
    pipeline.fit(_columns(m, features), m.y, lr__sample_weight=m.w)
    return TrainedModel("logreg", features, offset, pipeline=pipeline,
                        meta={"train_batches": sorted(set(m.batch.tolist()))})


def _col(table: pa.Table, name: str, fill: float) -> np.ndarray:
    values = table.column(name).to_numpy(zero_copy_only=False).astype(np.float64)
    return np.nan_to_num(values, nan=fill)


def rule_score(cfg: Config, name: str, table: pa.Table) -> np.ndarray:
    """Scores of a rule baseline; ties are broken later by URL hash (random order)."""
    if name == "random":
        return np.zeros(table.num_rows)
    if name == "inlinks":
        return _col(table, "log_inlinks", 0.0)
    if name == "ext":
        return _col(table, "log_ext", 0.0)
    if name == "domain_prior":
        return _col(table, "lift_dkey", -np.inf)
    url_score = _col(table, "url_score", 0.0)
    if name == "url_score":
        return url_score
    if name == "v2_baseline_v1":
        v = cfg.eval.v2_formula

        def capped(values: np.ndarray, cap: float) -> np.ndarray:
            return np.minimum(values, math.log1p(cap)) / math.log1p(cap)

        natural = (_col(table, "source", -1.0) == 0) & (url_score >= 0) & (url_score <= 1)
        unit = (v["w_inlinks"] * capped(_col(table, "log_inlinks", 0.0), v["inlinks_cap"])
                + v["w_ext"] * capped(_col(table, "log_ext", 0.0), v["ext_cap"])
                + v["w_url"] * np.where(natural, url_score, 0.0)
                + v["w_prior"] * capped(_col(table, "dem_dkey", 0.0), v["prior_cap"]))
        total = v["w_inlinks"] + v["w_ext"] + v["w_url"] + v["w_prior"]
        return (_col(table, "hist_decayed", 0.0) > 0) + unit / total
    raise ValueError(f"unknown rule baseline {name!r}")
