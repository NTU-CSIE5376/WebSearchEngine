#!/usr/bin/env python3
"""Provision the metricdb connection, virtual datasets, charts, and dashboard
in a running Superset, via its REST API. Idempotent: safe to re-run.

Usage:
    python3 bootstrap.py            # talks to http://localhost:8088, admin/admin

Env overrides: SUPERSET_URL, SUPERSET_USER, SUPERSET_PASSWORD,
               METRIC_DB_HOST, METRIC_DB_PORT.
"""
import json
import os
import sys
import time

import requests

BASE = os.environ.get("SUPERSET_URL", "http://localhost:8088").rstrip("/")
USER = os.environ.get("SUPERSET_USER", "admin")
PASSWORD = os.environ.get("SUPERSET_PASSWORD", "admin")

METRIC_DB_HOST = os.environ.get("METRIC_DB_HOST", "172.16.191.1")
METRIC_DB_PORT = os.environ.get("METRIC_DB_PORT", "5433")
METRIC_DB_URI = (
    f"postgresql+psycopg2://metric:metric@{METRIC_DB_HOST}:{METRIC_DB_PORT}/metricdb"
)

# --- virtual datasets: name -> SQL -------------------------------------------
DATASETS = {
    "vw_crawler_overview": (
        "SELECT stat_date, discovered, crawled, indexed\n"
        "FROM crawler_stat_total ORDER BY stat_date"
    ),
    "vw_crawled_rolling": (
        "SELECT stat_date,\n"
        "       fetch_ok    AS crawled_daily,\n"
        "       fetch_ok_7  AS crawled_weekly,\n"
        "       fetch_ok_30 AS crawled_monthly\n"
        "FROM crawler_stat_total ORDER BY stat_date"
    ),
    "vw_request_effectiveness": (
        "SELECT stat_date,\n"
        "       ROUND(fetch_ok::numeric / NULLIF(fetch_total, 0), 4) AS eff_daily\n"
        "FROM crawler_stat_total ORDER BY stat_date"
    ),
    "vw_headset_coverage": (
        "SELECT stat_date, discovered_rate, crawled_rate, indexed_rate, ranked_rate\n"
        "FROM metric_headset_total ORDER BY stat_date"
    ),
    "vw_randomset_coverage": (
        "SELECT stat_date, discovered_rate, crawled_rate, indexed_rate, ranked_rate\n"
        "FROM metric_randomset_total ORDER BY stat_date"
    ),
    "vw_request_sent": (
        "SELECT stat_date, fetch_total, fetch_ok, fetch_fail\n"
        "FROM crawler_stat_total ORDER BY stat_date"
    ),
    # Hourly per-stage rows written by Metric/pipeline_rate.py.
    "vw_pipeline_rate": (
        "SELECT bucket_start, stage, records, new_links, folders_done,\n"
        "       avg_lag_min, max_lag_min, avg_duration_s, skipped_folders, errors\n"
        "FROM pipeline_stage_rate ORDER BY bucket_start"
    ),
}

# x-axis (temporal) column of each dataset; default stat_date.
DATASET_TIME_COLUMN = {"vw_pipeline_rate": "bucket_start"}


def metric(sql_expr, label):
    return {
        "expressionType": "SQL",
        "sqlExpression": sql_expr,
        "label": label,
        "hasCustomLabel": True,
    }


# --- charts: each maps to one dataset and a set of line metrics --------------
CHARTS = [
    {
        "name": "Total Overview Volumn",
        "dataset": "vw_crawler_overview",
        "metrics": [
            metric("MAX(discovered)", "T - Discovered"),
            metric("MAX(crawled)", "T - Crawled"),
            metric("MAX(indexed)", "T - Indexed"),
        ],
    },
    {
        "name": "Crawled (Daily / Weekly / Monthly)",
        "dataset": "vw_crawled_rolling",
        "metrics": [
            metric("MAX(crawled_daily)", "Crawled - Daily"),
            metric("MAX(crawled_weekly)", "Crawled - Weekly"),
            metric("MAX(crawled_monthly)", "Crawled - Monthly"),
        ],
    },
    {
        "name": "HeadSet Coverage",
        "dataset": "vw_headset_coverage",
        "y_format": ".0%",
        "metrics": [
            metric("MAX(discovered_rate)", "H - DiscCov"),
            metric("MAX(crawled_rate)", "H - CrawlCov"),
            metric("MAX(indexed_rate)", "H - IndexCov"),
            metric("MAX(ranked_rate)", "H - RankCov"),
        ],
    },
    {
        "name": "RandomSet Coverage",
        "dataset": "vw_randomset_coverage",
        "y_format": ".0%",
        "metrics": [
            metric("MAX(discovered_rate)", "R - DiscCov"),
            metric("MAX(crawled_rate)", "R - CrawlCov"),
            metric("MAX(indexed_rate)", "R - IndexCov"),
            metric("MAX(ranked_rate)", "R - RankCov"),
        ],
    },
    {
        "name": "Request Effectiveness (Daily)",
        "dataset": "vw_request_effectiveness",
        "y_format": ".0%",
        "metrics": [
            metric("MAX(eff_daily)", "Effectiveness - Daily"),
        ],
    },
    {
        "name": "Request Sent",
        "dataset": "vw_request_sent",
        "metrics": [
            metric("MAX(fetch_total)", "Fetch Total"),
            metric("MAX(fetch_ok)", "Fetch OK"),
            metric("MAX(fetch_fail)", "Fetch Fail"),
        ],
    },
]

TAB_METRICS = "Crawler Metrics"
TAB_PIPELINE = "Pipeline (24h)"

FOLDER_STAGES = ["router", "ingestor", "extractor"]
# Same color for a stage on every pipeline chart.
STAGE_COLORS = {
    "crawler": "#1f77b4",
    "router": "#ff7f0e",
    "ingestor": "#2ca02c",
    "extractor": "#9467bd",
}


def pipeline_chart(name, sql_expr, label, stages, description):
    return {
        "name": name,
        "dataset": "vw_pipeline_rate",
        "tab": TAB_PIPELINE,
        "metrics": [metric(sql_expr, label)],
        "groupby": ["stage"],
        "stages": stages,
        # Rolling 24 h; Superset's "Last day" is yesterday's calendar day.
        "time_range": 'DATEADD(DATETIME("now"), -24, hour) : now',
        "time_grain": "PT1H",
        "description": description,
    }


# One series per stage. When crawled volume drops, the stage whose line breaks
# first is the one to look at.
PIPELINE_CHARTS = [
    pipeline_chart(
        "Pipeline: Crawled vs Ingested (per hour)", "MAX(records)", "records",
        ["crawler", "ingestor"],
        "crawler = fetches finished (request.end); ingestor = crawl results written "
        "to crawlerdb. Ingestor far below crawler for hours = router/ingestor backlog.",
    ),
    pipeline_chart(
        "Pipeline: Folders Done (per hour)", "MAX(folders_done)", "folders",
        FOLDER_STAGES,
        "10-minute folders finished per hour; healthy is ~96 (16 workers x 6). "
        "Below that for hours = the stage falls behind.",
    ),
    pipeline_chart(
        "Pipeline: Max Lag (minutes)", "MAX(max_lag_min)", "max lag (min)",
        FOLDER_STAGES,
        "Folder finish time minus the folder's own time. Healthy ~20-35 min. "
        "crawl_result is deleted after ~8 h, so ~480 means unprocessed data is lost.",
    ),
    pipeline_chart(
        "Pipeline: Avg Folder Duration (s)", "MAX(avg_duration_s)", "duration (s)",
        FOLDER_STAGES,
        "folder_start -> folder_done. Above 600 s a worker cannot keep up with "
        "one folder every 10 minutes.",
    ),
    pipeline_chart(
        "Pipeline: Skipped Folders (data loss)", "MAX(skipped_folders)", "skipped",
        FOLDER_STAGES,
        "10-minute folders a worker never processed (deleted by the cleanup cron "
        "before it got to them). Should be 0.",
    ),
    pipeline_chart(
        "Pipeline: Errors (per hour)", "MAX(errors)", "errors",
        FOLDER_STAGES,
        "ERROR log lines per service and hour (Loki). The crawler is left out: "
        "its per-request fetch errors (~30-40k/h) would flatten the other lines; "
        "they are in pipeline_stage_rate if needed.",
    ),
]

PIPELINE_NOTE = """\
#### Pipeline (last 24 h, hourly)
`crawler -> router -> ingestor (+ extractor)`. Each stage hands work on in
10-minute folders. Rows come from `Metric/pipeline_rate.py` (Loki -> metricdb,
hourly cron).

When **Crawled** drops, look for the first stage that breaks:
- crawler records drop -> crawler / offerer side
- router or ingestor **folders done** below ~96/h and **lag** rising -> that
  stage is the bottleneck; lag near **480 min** plus **skipped folders** > 0 means
  results are deleted before they are ingested
- **errors** spike -> check that service's logs
"""

DASHBOARD_TITLE = "Crawler Metrics"

# Loosen the cramped echarts rich-tooltip (tight <table> rows by default).
DASHBOARD_CSS = (
    ".echarts-tooltip table { border-collapse: separate !important;"
    " border-spacing: 0 5px !important; }\n"
    ".echarts-tooltip td { padding: 2px 8px !important; line-height: 1.5 !important; }\n"
    ".echarts-tooltip .x-value { line-height: 2.2 !important; }\n"
)


class Superset:
    def __init__(self):
        self.s = requests.Session()
        self.s.headers["Referer"] = BASE

    def login(self):
        r = self.s.post(
            f"{BASE}/api/v1/security/login",
            json={"username": USER, "password": PASSWORD, "provider": "db", "refresh": True},
        )
        r.raise_for_status()
        self.s.headers["Authorization"] = "Bearer " + r.json()["access_token"]
        r = self.s.get(f"{BASE}/api/v1/security/csrf_token/")
        r.raise_for_status()
        self.s.headers["X-CSRFToken"] = r.json()["result"]

    def _list(self, resource, name_col):
        r = self.s.get(f"{BASE}/api/v1/{resource}/?q=(page_size:100)")
        r.raise_for_status()
        return {row[name_col]: row["id"] for row in r.json()["result"]}

    def post(self, resource, payload):
        r = self.s.post(f"{BASE}/api/v1/{resource}/", json=payload)
        if r.status_code not in (200, 201):
            raise RuntimeError(f"POST {resource} -> {r.status_code}: {r.text}")
        return r.json()["id"]

    # -- database -----------------------------------------------------------
    def ensure_database(self):
        existing = self._list("database", "database_name")
        if "metricdb" in existing:
            return existing["metricdb"]
        return self.post(
            "database",
            {
                "database_name": "metricdb",
                "sqlalchemy_uri": METRIC_DB_URI,
                "expose_in_sqllab": True,
            },
        )

    # -- datasets -----------------------------------------------------------
    def ensure_dataset(self, db_id, table_name, sql):
        existing = self._list("dataset", "table_name")
        if table_name in existing:
            ds_id = existing[table_name]
            r = self.s.put(f"{BASE}/api/v1/dataset/{ds_id}", json={"sql": sql})
            r.raise_for_status()
        else:
            ds_id = self.post(
                "dataset",
                {
                    "database": db_id,
                    "schema": "public",
                    "table_name": table_name,
                    "sql": sql,
                },
            )
        self._mark_temporal(ds_id, DATASET_TIME_COLUMN.get(table_name, "stat_date"))
        return ds_id

    def _mark_temporal(self, ds_id, col_name):
        r = self.s.get(f"{BASE}/api/v1/dataset/{ds_id}")
        r.raise_for_status()
        cols = r.json()["result"]["columns"]
        if not cols:
            return  # columns not inferred yet; chart still renders by name
        keep = ("id", "column_name", "type", "expression", "verbose_name",
                "groupby", "filterable", "description")
        payload_cols, changed = [], False
        for c in cols:
            d = {k: c.get(k) for k in keep if c.get(k) is not None}
            d["is_dttm"] = c["column_name"] == col_name
            if d["is_dttm"] != bool(c.get("is_dttm")):
                changed = True
            payload_cols.append(d)
        if changed:
            r = self.s.put(
                f"{BASE}/api/v1/dataset/{ds_id}?override_columns=true",
                json={"columns": payload_cols},
            )
            r.raise_for_status()

    # -- charts -------------------------------------------------------------
    def ensure_chart(self, spec, ds_id):
        existing = self._list("chart", "slice_name")
        x_axis = DATASET_TIME_COLUMN.get(spec["dataset"], "stat_date")
        filters = []
        if spec.get("time_range"):
            filters.append({
                "clause": "WHERE", "expressionType": "SIMPLE",
                "subject": x_axis, "operator": "TEMPORAL_RANGE",
                "comparator": spec["time_range"],
            })
        if spec.get("stages"):
            filters.append({
                "clause": "WHERE", "expressionType": "SIMPLE",
                "subject": "stage", "operator": "IN", "comparator": spec["stages"],
            })
        params = {
            "viz_type": "echarts_timeseries_line",
            "datasource": f"{ds_id}__table",
            "x_axis": x_axis,
            "x_axis_sort_asc": True,
            "x_axis_title_margin": 15,
            "metrics": spec["metrics"],
            "groupby": spec.get("groupby", []),
            "adhoc_filters": filters,
            "row_limit": 10000,
            "order_desc": True,
            "show_legend": True,
            "markerSize": 6,
            "opacity": 0.2,
            "seriesType": "line",
            "y_axis_format": spec.get("y_format", "SMART_NUMBER"),
            "rich_tooltip": True,
            # no summed "Total" row in the hover tooltip
            "showTooltipTotal": False,
            "showTooltipPercentage": False,
        }
        if spec.get("time_range"):
            params["time_range"] = spec["time_range"]
        if spec.get("time_grain"):
            params["time_grain_sqla"] = spec["time_grain"]
        payload = {
            "slice_name": spec["name"],
            "description": spec.get("description"),
            "viz_type": "echarts_timeseries_line",
            "datasource_id": ds_id,
            "datasource_type": "table",
            "params": json.dumps(params),
        }
        if spec["name"] in existing:
            cid = existing[spec["name"]]
            r = self.s.put(f"{BASE}/api/v1/chart/{cid}", json=payload)
            r.raise_for_status()
            return cid
        return self.post("chart", payload)

    # -- dashboard ----------------------------------------------------------
    def ensure_dashboard(self, title, tabs):
        """tabs: [(tab title, markdown or None, [(chart_id, name), ...])].
        The first tab is the one shown when the dashboard opens."""
        tabs_id = "TABS-top"
        position = {
            "DASHBOARD_VERSION_KEY": "v2",
            "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": [tabs_id]},
            tabs_id: {"type": "TABS", "id": tabs_id, "parents": ["ROOT_ID"], "children": []},
            "HEADER_ID": {"type": "HEADER", "id": "HEADER_ID", "meta": {"text": title}},
        }
        chart_ids_names = []
        n_rows = 0
        for t, (tab_title, note, charts) in enumerate(tabs):
            tab_id = f"TAB-{t}"
            tab_parents = ["ROOT_ID", tabs_id, tab_id]
            position[tabs_id]["children"].append(tab_id)
            position[tab_id] = {
                "type": "TAB", "id": tab_id, "parents": ["ROOT_ID", tabs_id],
                "children": [], "meta": {"text": tab_title, "defaultText": "Tab title",
                                         "placeholder": "Tab title"},
            }

            def new_row():
                nonlocal n_rows
                row_id = f"ROW-{n_rows}"
                n_rows += 1
                position[tab_id]["children"].append(row_id)
                position[row_id] = {
                    "type": "ROW", "id": row_id, "parents": tab_parents,
                    "children": [], "meta": {"background": "BACKGROUND_TRANSPARENT"},
                }
                return row_id

            if note:
                row_id = new_row()
                md_id = f"MARKDOWN-{t}"
                position[row_id]["children"].append(md_id)
                position[md_id] = {
                    "type": "MARKDOWN", "id": md_id, "parents": tab_parents + [row_id],
                    "children": [], "meta": {"code": note, "width": 12, "height": 26},
                }
            # two charts per row (width 6 of the 12-column grid)
            for i, (cid, name) in enumerate(charts):
                if i % 2 == 0:
                    row_id = new_row()
                chart_node = f"CHART-{len(chart_ids_names)}"
                position[row_id]["children"].append(chart_node)
                position[chart_node] = {
                    "type": "CHART", "id": chart_node,
                    "parents": tab_parents + [row_id], "children": [],
                    "meta": {"chartId": cid, "width": 6, "height": 50, "sliceName": name},
                }
                chart_ids_names.append((cid, name))
        existing = self._list("dashboard", "dashboard_title")
        payload = {
            "dashboard_title": title,
            "position_json": json.dumps(position),
            "css": DASHBOARD_CSS,
            "published": True,
        }
        if title in existing:
            did = existing[title]
            # Merge into the existing json_metadata so native filters and
            # other settings made in the UI survive a re-run.
            r = self.s.get(f"{BASE}/api/v1/dashboard/{did}")
            r.raise_for_status()
            meta = json.loads(r.json()["result"].get("json_metadata") or "{}")
            meta["label_colors"] = {**meta.get("label_colors", {}), **STAGE_COLORS}
            payload["json_metadata"] = json.dumps(meta)
            r = self.s.put(f"{BASE}/api/v1/dashboard/{did}", json=payload)
            r.raise_for_status()
        else:
            did = self.post("dashboard", payload)
        # position_json references the charts but does not create the
        # slice<->dashboard association; link each chart explicitly.
        for cid, _ in chart_ids_names:
            r = self.s.put(f"{BASE}/api/v1/chart/{cid}", json={"dashboards": [did]})
            r.raise_for_status()
        return did


def wait_ready():
    for _ in range(60):
        try:
            if requests.get(f"{BASE}/health", timeout=3).text.strip() == "OK":
                return
        except requests.RequestException:
            pass
        time.sleep(3)
    sys.exit("Superset did not become healthy in time")


def main():
    wait_ready()
    sup = Superset()
    sup.login()

    db_id = sup.ensure_database()
    print(f"database metricdb -> id {db_id}")

    ds_ids = {}
    for name, sql in DATASETS.items():
        ds_ids[name] = sup.ensure_dataset(db_id, name, sql)
        print(f"dataset {name} -> id {ds_ids[name]}")

    tab_charts = {TAB_METRICS: [], TAB_PIPELINE: []}
    for spec in CHARTS + PIPELINE_CHARTS:
        cid = sup.ensure_chart(spec, ds_ids[spec["dataset"]])
        tab_charts[spec.get("tab", TAB_METRICS)].append((cid, spec["name"]))
        print(f"chart {spec['name']!r} -> id {cid}")

    did = sup.ensure_dashboard(DASHBOARD_TITLE, [
        (TAB_METRICS, None, tab_charts[TAB_METRICS]),
        (TAB_PIPELINE, PIPELINE_NOTE, tab_charts[TAB_PIPELINE]),
    ])
    print(f"dashboard {DASHBOARD_TITLE!r} -> id {did}")
    print(f"\nDone. Open {BASE}/superset/dashboard/{did}/")


if __name__ == "__main__":
    main()
