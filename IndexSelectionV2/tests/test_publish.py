from __future__ import annotations

from datetime import UTC, datetime

import pytest

from isel.publish import swap_allowed
from isel.selectdb import ForbiddenTarget, connect


@pytest.mark.parametrize("now, allowed", [
    (datetime(2026, 10, 16, 11, 29, tzinfo=UTC), True),
    (datetime(2026, 10, 16, 11, 30, tzinfo=UTC), False),    # random set at 12:00
    (datetime(2026, 10, 16, 19, 59, tzinfo=UTC), False),    # head set at 18:00
    (datetime(2026, 10, 16, 20, 0, tzinfo=UTC), True),
    (datetime(2026, 10, 15, 15, 0, tzinfo=UTC), True),
])
def test_swaps_wait_while_the_metric_reads_the_list(base_cfg, now, allowed):
    assert swap_allowed(now, base_cfg.schedule) is allowed


@pytest.mark.parametrize("dsn", [
    "postgresql://select@172.16.191.1:5444/selectdb",     # V1 selectdb
    "postgresql://crawler@172.16.191.1:5432/crawlerdb",
    "postgresql://metric@172.16.191.1:5433/metricdb",
    "postgresql://select@172.16.191.1/selectdb",          # default port 5432
    "host=172.16.191.1 port=5444 dbname=selectdb",
])
def test_writes_never_connect_to_forbidden_ports(base_cfg, monkeypatch, dsn):
    monkeypatch.setenv(base_cfg.selectdb.admin_dsn_env, dsn)
    with pytest.raises(ForbiddenTarget, match="forbidden port"):
        connect(base_cfg, base_cfg.selectdb.admin_dsn_env, read_only=False)
