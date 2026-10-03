"""Pacing between export chunks with additive-increase / multiplicative-decrease.

The pacer never stalls inside a statement: a stalled COPY would keep its
snapshot and lock open longer. Each chunk runs at full speed, then the worker
sleeps long enough to hold the average heap read rate at the target.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from enum import IntEnum

from idxsel.config import ThrottleCfg

MB = 1_000_000


class Verdict(IntEnum):
    """Guard outcomes, ordered by severity."""

    OK = 0
    SLOW = 1
    PAUSE = 2
    ABORT = 3


class Pacer:
    """Holds the target read rate and adapts it to guard verdicts."""

    def __init__(
        self,
        cfg: ThrottleCfg,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._cfg = cfg
        self._sleep = sleep
        self.rate_mb_s = cfg.rate_mb_s_initial
        self._healthy = 0
        self._pause_level = 0

    def after_chunk(self, nbytes: int, elapsed_s: float) -> float:
        """Sleeps so `nbytes` over the whole interval matches the target rate."""
        wait = nbytes / (self.rate_mb_s * MB) - elapsed_s
        if wait > 0:
            self._sleep(wait)
            return wait
        return 0.0

    def on_verdict(self, verdict: Verdict) -> float:
        """Adapts the rate; for PAUSE sleeps on the backoff ladder. Returns seconds slept."""
        c = self._cfg
        if verdict == Verdict.OK:
            self._pause_level = 0
            self._healthy += 1
            if self._healthy >= c.increase_every_healthy_chunks:
                self.rate_mb_s = min(c.rate_mb_s_max, self.rate_mb_s + c.increase_mb_s)
                self._healthy = 0
            return 0.0
        self._healthy = 0
        self.rate_mb_s = max(c.rate_mb_s_min, self.rate_mb_s * c.decrease_factor)
        if verdict == Verdict.PAUSE:
            ladder = c.pause_backoff_s
            wait = ladder[min(self._pause_level, len(ladder) - 1)]
            self._pause_level += 1
            self._sleep(wait)
            return wait
        return 0.0
