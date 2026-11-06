# Superset dashboard for crawler metrics

Self-hosted Superset that visualises `metricdb.crawler_stat_total`, reproducing the
existing Power BI panels plus a new request-effectiveness chart.

## What it builds

| Chart | Source |
|-------|--------|
| Total Overview Volumn | `crawler_stat_total`: `discovered` / `crawled` / `indexed` |
| Crawled (Daily / Weekly / Monthly) | `crawler_stat_total`: `fetch_ok` / `fetch_ok_7` / `fetch_ok_30` |
| HeadSet Coverage | `metric_headset_total`: discovered/crawled/indexed/ranked rate (%) |
| RandomSet Coverage | `metric_randomset_total`: discovered/crawled/indexed/ranked rate (%) |
| Request Effectiveness (Daily) | `crawler_stat_total`: `fetch_ok / fetch_total` per day (%) |
| Request Sent | `crawler_stat_total`: `fetch_total` / `fetch_ok` / `fetch_fail` per day |

All six sit on the **Crawler Metrics** dashboard, two charts per row.

## Bring it up

```bash
cd superset
docker compose up -d --build          # superset + its metadata postgres
python3 bootstrap.py                   # create db conn, datasets, charts, dashboard
```

`bootstrap.py` needs `requests` (already in `/home/r14921046/.venv`):

```bash
/home/r14921046/.venv/bin/python3 bootstrap.py
```

It is idempotent, re-run any time after editing the dataset/chart definitions.

- UI: http://172.16.191.2:8088  (login `admin` / `admin`)
- metricdb is added **read-only**; Superset only issues SELECTs against `172.16.191.1:5433`.

## Expose externally (optional, off by default)

The `cloudflared` service dials out to Cloudflare, so it works behind NAT / campus
firewall with no sudo and no inbound port. It only starts under the `tunnel` profile.

**Before exposing publicly:** Superset here reaches the production metricdb, so set a
real admin password first:

```bash
docker exec -it wse_superset superset fab reset-password \
  --username admin --password '<a-strong-password>'
```

### Quick tunnel (no domain, ephemeral URL)

```bash
docker compose --profile tunnel up -d cloudflared
docker compose logs cloudflared | grep trycloudflare   # your public URL
```

Random `https://*.trycloudflare.com`, changes on restart, no Cloudflare Access in
front of it. Fine for a quick demo, not for anything left running.

### Named tunnel (needs a domain on Cloudflare; stable URL + Access)

```bash
# Zero Trust dashboard -> Networks -> Tunnels -> Create tunnel -> Cloudflared:
#   - name it, copy the connector token
#   - Public hostname: superset.<your-domain>  ->  service http://superset:8088
echo 'CLOUDFLARED_TOKEN=<token>' >> .env
docker compose --profile tunnel up -d cloudflared
# then add Zero Trust -> Access -> Applications policy on that hostname for login.
```

## Tear down

```bash
docker compose down            # keep metadata
docker compose down -v         # also drop superset's metadata volume
```
