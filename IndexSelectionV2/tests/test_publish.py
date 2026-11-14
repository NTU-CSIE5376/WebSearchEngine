from __future__ import annotations

import pytest

from isel.selectdb import ForbiddenTarget, connect


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
