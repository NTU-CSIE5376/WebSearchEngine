# Index Selection V2

每兩週從 crawlerdb 的**已爬頁面**選出 1.28 億個 URL，寫進**我們自己的** selectdb（Host1 `:5445`）。
`selected_urls_current` 的 schema 與 PR #4、官方 metric 的查詢相容：
`SELECT url FROM public.selected_urls_current WHERE url = ANY(:urls)`。

## 資料流

```
crawlerdb (read-only, paced)
  ├─ export full : url_state_current_NNN by ctid chunks   (every N cycles)
  ├─ export delta: url_state_history_NNN by snapshot_id   (daily)
  └─ export dims : domain_state, shard_split_subdomain
            ▼ Parquet on the compute host (work_dir)
metricdb (read-only) ──golden──► past-golden URLs + time-decayed domain prior
            ▼
features (latest observation per URL; the result is the next merge source) → score
            ▼
exact top-K (radix select, md5 tie-break) → load (new table + PK) → rename swap
            ▼
verify-live (reader role) · eval: metric replica on the run's own list, funnel, bootstrap CI
```

## 安全設計（對生產環境）

| 風險 | 做法 |
|---|---|
| 寫入 crawlerdb | 每條連線都設 `default_transaction_read_only=on`，用戶端另有 SQL 白名單（`pg.assert_read_only_sql`）。`crawler` 是 superuser，所以兩層都要 |
| 長交易擋住 vacuum | 每個 ctid 區塊是獨立的 autocommit `COPY`，`statement_timeout=60s` |
| 伺服器端暫存檔寫滿 `/data` | 不做 server 端排序或彙總，並設 `temp_file_limit=0` |
| 擋住別人的 DDL 或寫入 | watchdog 每秒檢查一次，只對正在等鎖的 backend 呼叫 `pg_blocking_pids`。我們擋到別人時就取消自己的查詢；它的連線斷了會重連，重連前不開始新的讀取 |
| I/O 搶資源 | 每塊之間 sleep 限速（AIMD），依防護判定自動減速或暫停 |
| 撞到其他排程 | 每日 02:50–04:40 UTC 和量測日 00:00–09:30 UTC 不讀 DB；量測日 11:30–20:00 不 swap |
| 磁碟或記憶體不足 | 匯出在啟動前和每塊之間檢查。BUILD 在開始前和 load 前檢查 Host2 磁碟（加上 `build_reserve_gb` 預留）、記憶體、Host1 根目錄與 IO 壓力；它不讀 crawlerdb，所以黑名單時段和 Host1 `/data` 不擋它。遠端探測失敗一律視為不安全 |
| 讀者看到半套清單 | 新表在交易 1 驗證完才 rename swap，`lock_timeout` 5 秒；`COPY FREEZE` 讓讀者第一次讀時不寫 hint bit |
| 錯誤的 history 列 | crawler 的 accounting 每天寫入只有 17 欄的 history 列（inlink 為 0）。delta 只取 `robots_bits <> 0` 的完整列 |
| 兩個程序同時寫工作目錄 | 會改工作目錄的指令都要先取 `state/lock`（flock）。daemon 每個 tick 取一次，sleep 時放開；另有 `state/daemon.lock` 擋住第二個 daemon。DuckDB 的暫存檔名會撞，所以不取鎖的 `verify-live` 不寫暫存檔 |
| crawlerdb 卡住，擋住 BUILD | BUILD 不讀 crawlerdb。它在量測前 30 小時開始，用每日 05:00 delta 已匯出的資料。每日 delta 最多跑 `schedule.delta_max_h`，逾時就停下，之後續跑。metricdb 讀不到時沿用上一份 golden；selectdb 連不上時在 features 之前就失敗 |

緊急停止：

```
docker compose -p idxsel_r14725023 -f compose.compute.yaml stop
# crawlerdb 上：SELECT pg_cancel_backend(pid) FROM pg_stat_activity
#              WHERE application_name LIKE 'idxsel_r14725023%';
```

回滾到上一份清單：`idxsel --config ... rollback`

**不要刪壓縮後的 feature set。** 它是 `state/exports.json` 裡 `compacted.features` 指向的目錄，也是下一輪合併的來源。base 匯出和已併入的 delta 都已刪除，所以它遺失（連一個 shard 檔也算）時只能靠全量匯出重建。daemon 每個 tick 都會檢查，發現時在 `exports.json` 設 `force_full`，下一個全量視窗自動重建。如果已經過了視窗，手動跑 `run --once --full`（約 14 小時加 BUILD）。

## 指令

```
idxsel --config config/idxsel.example.toml --config config/idxsel.host.toml <command>
```

| 指令 | 作用 | 會碰到的資源 |
|---|---|---|
| `init-selectdb` | 建表和 reader 角色，只需跑一次 | 我們的 selectdb |
| `preflight` / `probe` | 檢查現在能不能啟動匯出 | crawlerdb 統計視圖 |
| `export --mode full\|delta [--shards 0-3] [--dry-run]` | 匯出；`--dry-run` 只讀 catalog 估算規模。排程開啟時，BUILD 視窗開始就停下，視窗內拒絕（回傳碼 4） | crawlerdb（唯讀） |
| `golden` / `features` / `select` | 各自單獨跑一個階段 | metricdb（唯讀）、本機 |
| `load` / `swap` / `rollback` | 載入（會清掉沒上線的舊 run 表）、上線、回滾。回滾也會丟掉還沒上線的 run，所以等待中的 swap 不會把它蓋回去 | 我們的 selectdb |
| `eval` | 用該次 run 自己的 `selected.parquet` 重算 metric（完全比對＋canonical），結果寫回 `model_runs` | 本機；有載入時寫 selectdb |
| `verify-live` | 用 metric 的 reader 角色確認：線上清單是這次 run、共 K 列、抽樣查得到 | 我們的 selectdb（唯讀） |
| `run --once [--full]` | 從頭到尾跑完一輪正式流程，清單會上線 | 以上全部 |
| `backtest` | 用最新的 feature set 跑 golden → select → eval，結果寫到 `backtests/<id>/`。不改 state 和 pointers；BUILD 視窗內拒絕 | metricdb（唯讀）、本機 |
| `daemon` | 依排程執行：每日 delta、全量更新、量測前建置 | 以上全部 |
| `status` / `gc` | 查看狀態、清理舊資料 | 本機 |

會改工作目錄的指令要先取工作目錄鎖。拿不到鎖時，指令以回傳碼 4 結束，並印出持鎖者。
不取鎖的指令：`status`、`probe`、`preflight`、`verify-live`、`rollback`（緊急用，不能排在長時間的匯出後面）、`init-selectdb`。

所有參數都在 `config/idxsel.example.toml`，載入時會嚴格檢查：多出未知的 key、缺少 key、型別錯誤都會直接報錯。
本機或主機專屬的差異放在第二個 `--config` overlay 檔。

## 評估與回測

- **樣本內的定義。** 符合任一條件，報告就標 `in_sample: true`，數字偏高，不能預測下一批：
  - 這一批進了 past-golden tier 或網域先驗（`golden.target_batch = "next"` 的正式執行就是這樣）
  - 這一批在 feature set 的 `as_of` 之前就已建立。crawler 會依新 batch 調整爬取：`update_golden_domain_scores.py` 每天依最近兩批調整網域排序，`golden_inject.py` 會把滿 4 週的 batch 注入回去。所以這些 URL 可能正是因為是 golden 才被爬到
- **無洩漏的前瞻數字：** 量測之後、下一次 BUILD 之前，用預設的 `target_batch = "next"` 跑 `backtest`。
  - 這時 feature set 還是 BUILD 用的那份，`as_of` 早於量測產生的新 batch。
  - golden 只用 `as_of` 之前的 batch 建 tier 和先驗，所以選出的清單和線上那份相同。
  - 報告裡新 batch 標 `in_sample: false`，`indexed_exact` 就是官方 metric 會量到的數字。
  - 中間如果有人手動跑過 `features` 或 `run --once`，報告會自動改標樣本內。
- **整數 `target_batch` 的回測**一律標樣本內，因為 feature set 晚於那一批。它只能用來比較 profile 的相對好壞。
- 量測前的 BUILD 視窗內，如果這一期還沒建好，`backtest` 會拒絕執行（回傳碼 4），避免擋住 BUILD。
- `indexed_exact` 與官方 metric 相同：原始 golden 字串必須完全等於清單裡的一列。`indexed_canonical` 是正規化後才相等的比例，兩者差距就是拼法造成的損失；`selection.emit_raw_spelling_for_past_golden` 可以補回其中 past-golden 的部分（維持關閉，見 ARCHITECTURE_SPEC §6.10）。
- 95% 信賴區間用群集 bootstrap：整個 query 一起重抽。出現在 c 個 query 的 URL，在每個 query 裡權重 1/c，所以全樣本時等於不重複 URL 的比例。

## 磁碟預算（Host2 工作目錄）

**合併壓縮：** 每次建好的 feature set 就是下一次合併的來源。它的 schema 和 base 匯出相同，而且保留每列的 `obs_rank` 和 `obs_seq`，所以結果和「base + 全部 delta」完全相同。BUILD 在 features 完成後立刻跑 gc，先刪掉舊的 set 和已併入的 delta，再做 select。

工作目錄在 Host2 根目錄（877 GB，部署前剩約 629 GB）。防護設定：

- `guard.work_dir_min_free_gb = 150`：根目錄至少保留 150 GB 給爬蟲的容器和日誌。匯出在每塊之間檢查。
- `guard.build_reserve_gb = 170`：BUILD 開始時，剩餘空間減掉 170 GB 仍要 ≥ 150 GB。170 GB 是新的 feature set（約 85 GB）加上 DuckDB 暫存上限（60 GB）和餘量。
- 所以 BUILD 開始時工作目錄要 ≤ 約 309 GB，匯出時要 ≤ 約 479 GB。

以約 15.3 億列已爬列估算，每列 Parquet 約 55 B；delta 假設每天 ≤ 8 GB：

| 時間點 | 工作目錄內容 | 用量 |
|---|---|---|
| 一般週期 BUILD 開始 | 壓縮 set 85 + 14 天 delta 112 + runs、golden、backtests 40 | 約 237 GB，通過 |
| BUILD 的 features 階段 | 上一列 + 新 set 85 + DuckDB 暫存 ≤ 60 | 約 382 GB |
| BUILD 的 select、load 階段 | 新 set 85 + scored 55 + load.csv 15（在 `duckdb_tmp/`）+ runs 等 40 | 約 195 GB |
| 全量匯出結束時 | 壓縮 set 85 + delta 104 + 新 base 85 + runs 等 40 | 約 315 GB，低於 479 |
| 全量剛完成後的 BUILD 開始 | 舊 set 85（pointer 保護）+ 新 base 85 + catch-up 與 delta + runs 等 40 | 約 220 GB，通過 |
| 最壞情況：全量錯過 deadline，delta 又到上限 | 壓縮 set 85 + delta 112 + 未完成的 base ≤ 85 + runs 等 40 | 約 322 GB，BUILD 被拒；舊清單留在線上，事件 `daemon.deferred` |

- history 增量的估計假設每天的已爬完整列 ≤ 8 GB。調研只量到 history 總量（每天約 1.6 億列，多數是新連結的插入列，會被濾掉）。Phase 0b 要量出完整列的比例，再校正這一行，並決定最壞情況要不要改成「放棄延後中的全量」。
- `guard.work_dir_max_gb`（500）只在匯出時檢查。
- gc 只把有完成標記的 golden、run、backtest 算進保留份數；失敗留下的半成品目錄直接刪。feature set 只保留 pointer 和壓縮後的那份。daemon 每個 tick 開頭都會先跑 gc。

## 開發與測試

```
uv sync
uv run ruff check src tests
uv run pytest                       # unit tests, no network
uv run pytest -m integration        # needs local Docker with postgres:16
```

整合測試會在本機起兩個 Postgres 16 容器，schema 與正式環境相同：
crawlerdb 的 33 欄、fillfactor 80、按月分區的 history；metricdb；我們的 selectdb（`wal_level=minimal`）。
涵蓋：

- CLI 端到端三輪（full → delta → delta），以及回滾；評估結果等於 reader 角色跑的 metric SQL
- 量測後的回測：重建出和線上相同的清單，新 batch 標樣本外，數字等於 metric SQL；早於 feature set 的 batch 標樣本內
- 合併壓縮：第二輪後 base 和已併入的 delta 已刪；第三輪在壓縮後的 feature set 上仍選出真值的 top-K；單元測試確認壓縮與一次合併全部結果相同，同一交易的 history 列以 `snapshot_id` 決定
- accounting 型的不完整 history 列不會改變排名
- `backtest` 不讀 crawlerdb、不寫 selectdb，state 和 pointers 不變
- 工作目錄鎖：CLI 回傳碼 4、daemon 跳過被佔用的 tick、第二個 daemon 被擋下；BUILD 視窗內手動 `export` 和 `backtest` 被拒
- 匯出期間有並行寫入時的一致性
- 中斷後續跑，包括中斷期間 watermark 前進
- superuser 也被強制唯讀
- watchdog 自我取消、斷線重連
- swap 不影響 metric 讀者、鎖逾時、冪等、沒上線的表會被清掉；回滾會取消等待中的 swap
- daemon：
  - BUILD 不呼叫匯出；metricdb 讀不到時沿用上一份 golden；selectdb 連不上時在 features 之前就失敗
  - 空間不足時 BUILD 在開始前就被拒，不留任何目錄，也不進冷卻
  - 壓縮 set 少一個 shard 檔時要求全量：不進冷卻、不產生新目錄；手動全量也會清掉 `force_full`
  - 每日 delta 一直 PAUSE 時在期限停下並保留進度，BUILD 照常產出
  - load 之後的 verify 或 eval 失敗，不會再載入一次
  - swap 碰到 freeze 時留到之後的 tick，冷卻中也會完成，完成後再 verify
  - 錯誤後冷卻；全量進度不被 delta 覆蓋；全量拖過 history 保留期時重新開始
  - select 失敗不留下 scored；gc 清掉半成品目錄；BUILD 結束時舊的 set 和 delta 已刪，run 目錄沒有 `load.csv`

## 部署（Phase 0 之後才做）

1. Host1：`compose.selectdb.yaml` 起我們的 selectdb（:5445，資料放在 root NVMe），接著跑 `idxsel init-selectdb`。
2. Host2：準備 `.env`（chmod 600）和 `config/idxsel.host.toml`，再用 `compose.compute.yaml` 起 daemon。
3. Host1 探測：在 `~/.ssh/authorized_keys` 加一把 forced-command key，限定只能執行
   `command="df -B1 --output=target,avail / /data; cat /proc/pressure/io",no-pty,no-port-forwarding,no-agent-forwarding`。
4. 讀 crawlerdb heap 之前，要取得助教或維運同意，而且 Phase 0c 的小量讀取驗收必須通過。
