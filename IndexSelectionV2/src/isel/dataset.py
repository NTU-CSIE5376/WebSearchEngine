"""Training and evaluation matrices stacked across golden batches."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

from isel.config import Config
from isel.features import FeatureStore, assert_clean


@dataclass
class Matrix:
    x: np.ndarray
    y: np.ndarray
    w: np.ndarray
    batch: np.ndarray
    later_golden: np.ndarray   # negative whose URL is golden in a later training batch
    features: list[str]

    def subset(self, keep: np.ndarray) -> Matrix:
        """The rows where `keep` is true."""
        return Matrix(self.x[keep], self.y[keep], self.w[keep], self.batch[keep],
                      self.later_golden[keep], self.features)


def to_numpy(table: pa.Table, features: list[str]) -> np.ndarray:
    """Feature columns as float32, with NaN for NULL."""
    assert_clean(features)
    cols = [pc.cast(table.column(f), pa.float32()).to_numpy(zero_copy_only=False)
            for f in features]
    return np.column_stack(cols).astype(np.float32) if cols else np.empty((table.num_rows, 0))


def train_batches(cfg: Config, store: FeatureStore, test_batch: int) -> list[int]:
    """Label batches usable to train a model that is evaluated on `test_batch`.

    A batch qualifies when it existed at the test batch's as-of time, is at or
    after `dataset.first_label_batch`, and has golden history for its priors.
    """
    test = store.batch(test_batch)
    return [b.batch_id for b in store.batches
            if b.batch_id >= cfg.dataset.first_label_batch
            and b.created_at <= test.as_of and b.batch_id != test_batch
            and store.has_history(b.batch_id)]


def tag_weights(in_head: np.ndarray, in_random: np.ndarray) -> np.ndarray:
    """Positive weights that give head and random equal total weight in one batch.

    The weights are rescaled so they sum to the number of positives.
    """
    n_head, n_random = float(in_head.sum()), float(in_random.sum())
    mean = (n_head + n_random) / 2.0
    w = np.zeros(len(in_head), dtype=np.float64)
    if n_head:
        w += in_head * (mean / n_head)
    if n_random:
        w += in_random * (mean / n_random)
    w[w == 0] = 1.0
    return w * (len(w) / w.sum()) if w.sum() else w


def training_matrix(store: FeatureStore, batches: list[int], features: list[str]) -> Matrix:
    """Stacks the train frames of `batches`.

    `later_golden` marks negatives whose URL is golden in a batch of `batches`
    created after their own. Those labels are known wherever `batches` is a valid
    training set, so masking them does not leak (the `later_unlabeled` baseline).
    """
    created = {b: store.batch(b).created_at for b in batches}
    xs, ys, ws, bs, ls = [], [], [], [], []
    for batch in batches:
        table = store.frame(batch, "train")
        y = table.column("label").to_numpy(zero_copy_only=False).astype(np.int8)
        later = [b for b in batches if created[b] > created[batch]]
        golden = (pc.is_in(table.column("url"), value_set=store.golden_spellings(later))
                  .to_numpy(zero_copy_only=False) if later else np.zeros(len(y), bool))
        ls.append(golden & (y == 0))
        w = np.ones(len(y), dtype=np.float64)
        pos = y == 1
        head = table.column("in_head").to_numpy(zero_copy_only=False)[pos]
        rand = table.column("in_random").to_numpy(zero_copy_only=False)[pos]
        w[pos] = tag_weights(head.astype(bool), rand.astype(bool))
        xs.append(to_numpy(table, features))
        ys.append(y)
        ws.append(w)
        bs.append(np.full(len(y), batch, dtype=np.int32))
    if not xs:
        raise ValueError("no training batches")
    return Matrix(np.vstack(xs), np.concatenate(ys), np.concatenate(ws), np.concatenate(bs),
                  np.concatenate(ls), list(features))
