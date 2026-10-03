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
| `backtest [--profile NAME]` | 用最新的 feature set 跑 golden → select → eval，結果寫到 `backtests/<id>/`，並寫 labels。不改 state 和 pointers；BUILD 視窗內拒絕；開始前檢查磁碟（預留 `build_reserve_gb`）；未知的 profile 以回傳碼 2 結束 | metricdb（唯讀）、本機 |
| `sweep [--profiles a,b] [--batches 18,19]` | 用 labels 在幾秒內估計各 profile 的 indexed rate，並和第一個 profile 做成對比較。只讀 labels、不寫檔、不溢寫 | 本機 |
| `daemon` | 依排程執行：每日 delta、全量更新、量測前建置、量測後的前瞻評估 | 以上全部 |
| `status` / `gc` | 查看狀態、清理舊資料 | 本機 |

會改工作目錄的指令要先取工作目錄鎖。拿不到鎖時，指令以回傳碼 4 結束，並印出持鎖者。
不取鎖的指令：`status`、`probe`、`preflight`、`verify-live`、`rollback`（緊急用，不能排在長時間的匯出後面）、`init-selectdb`、`sweep`。

所有參數都在 `config/idxsel.example.toml`，載入時會嚴格檢查：多出未知的 key、缺少 key、型別錯誤都會直接報錯。
本機或主機專屬的差異放在第二個 `--config` overlay 檔。

## 評估與回測

- **樣本內的定義。** 符合任一條件，報告就標 `in_sample: true`，數字偏高，不能預測下一批：
  - 這一批進了 past-golden tier 或網域先驗（`golden.target_batch = "next"` 的正式執行就是這樣）
  - 這一批在 feature set 的 `as_of` 之前就已建立。crawler 會依新 batch 調整爬取：`update_golden_domain_scores.py` 每天依最近兩批調整網域排序，`golden_inject.py` 會把滿 4 週的 batch 注入回去。所以這些 URL 可能正是因為是 golden 才被爬到
- **無洩漏的前瞻數字由 daemon 自動產生（`forward_eval`）。** 量測後 `schedule.forward_eval_delay_h`（20 小時，約隔天 08:00，那時當天的 delta 已跑完），daemon 做一次：
  1. 用 reader 角色唯讀查 `model_runs`，找出量測當下 live 的 run。量測之後如果有過 swap 或 rollback，就記 `forward_eval.ambiguous_live` 並跳過，不猜。找不到 run 時記 `forward_eval.no_live_run`。
  2. 讀 metricdb，找比那個 run 的 feature set 新的 batch。沒有時記 `forward_eval.no_new_batch`：metric 只在上一批滿 14 天才建新 batch，所以 2/16 → 3/1 那一期一定會重用。
  3. 直接對 live run 的 `selected.parquet` 評估，不重新 select。結果寫到 `forward_evals/<id>/`，記 `forward_eval.done`。
  4. 為新 batch 寫一份樣本外的 labels。
  - 只有我們建的清單被量測過（`built_for` 等於那次量測）才會做，每次量測只做一次。它不讀 crawlerdb、不寫 selectdb。
- 手動的 `backtest` 在量測之後、下一次 BUILD 之前用 `target_batch = "next"` 跑，也會得到同一個數字，但它會重新 select 一次。中間如果有人手動跑過 `features` 或 `run --once`，報告會自動改標樣本內。
- **整數 `target_batch` 的回測**一律標樣本內，因為 feature set 晚於那一批。它只能用來比較 profile 的相對好壞。
- 量測前的 BUILD 視窗內，如果這一期還沒建好，`backtest` 會拒絕執行（回傳碼 4），避免擋住 BUILD。
- `indexed_exact` 與官方 metric 相同：原始 golden 字串必須完全等於清單裡的一列。`indexed_canonical` 是正規化後才相等的比例，兩者差距就是拼法造成的損失；`selection.emit_raw_spelling_for_past_golden` 可以補回其中 past-golden 的部分（維持關閉，見 ARCHITECTURE_SPEC §6.10）。
- 95% 信賴區間用群集 bootstrap：整個 query 一起重抽。出現在 c 個 query 的 URL，在每個 query 裡權重 1/c，所以全樣本時等於不重複 URL 的比例。query 依 id 排序後才抽，所以結果和讀取順序無關。
- **其他欄位：**
  - `crawled_exact`：原始字串完全等於已爬 URL 的比例。crawler 存的是正規化後的字串，所以它低於 `crawled_rate`（正規化比對），完全比對的上限是它。
  - `cutoff_loss_exact` / `cutoff_loss_canonical`：在 feature set 之後、batch 建立之前才被爬到的 golden URL。用那份 feature set 建的清單不可能有它們。資料來自 feature set 之後的 delta；沒有 delta 涵蓋到 batch 建立時間時是空值。連續兩期 ≥ 1pp，才值得做量測前的 top-up。

### labels（特徵日誌）

依 Rules of ML #29，保存清單建立時用的特徵，之後調參和訓練都用它。每個 golden batch 一份 `labels/<batch>/`：

| 檔案 | 內容 |
|---|---|
| `golden.parquet` | 這一批的 golden 列，加上旗標：`crawled_exact`、`crawled_canonical`、`selected_exact`、`selected_canonical`，以及在該 profile 下的 `tier`、`score` |
| `golden_features.parquet` | golden URL（原始或正規化拼法）的完整特徵列 |
| `sample.parquet` | 候選列中 `md5_number_lower(url) % labels.sample_mod = 0` 的列（預設 1/1000，約 150 萬列），加上 `selected` 旗標。這是純雜湊樣本，不額外加入 golden 列 |
| `past_golden.parquet`、`domain_prior.parquet`、`domain_stats.parquet` | 建清單時的 tier、先驗與網域統計（只留樣本和 golden 出現的網域） |
| `meta.json`（最後寫） | batch、feature set 與 `as_of`、`in_sample`、`forward`、`candidates`（套用 filters 後的候選總數）、`filters`、`p0`、config 雜湊、評估結果、診斷 |

- 寫入規則：先寫到隱藏目錄再 rename。進了 tier 或先驗的 batch 會跳過（記 `labels.skipped`）。已存在的 label 只在「舊的樣本內、新的樣本外」時被取代。
- 診斷（從樣本估計）：清單中佔比最高的 `labels.top_domains` 個網域和它們的 golden 命中數；被選頁面的 `content_hash64` 在候選中重複的比例。重複比例高時，才值得做依內容去重。
- gc 永遠保留完整的 label，每期約 0.2 GB；缺 `meta.json` 或不是數字名稱的目錄會被刪。

### sweep

- 每個 label、每個 profile，用和正式流程相同的 SQL 對樣本和 golden 特徵列打分。
- 依正式排序（`tier DESC, key52 DESC, h, url, shard`，同 URL 只留最好的一份）取樣本中排名 round(K·n/N) 的列當邊界。n 是樣本列數，N 是 meta 的 `candidates`。
- golden URL 的最佳一份排在邊界或之前，就算命中。同分和 tier 都和正式流程一樣處理；只比分數時，大群同分會偏差到約 1.8pp。
- `sample_mod = 1` 時結果就是精確的 top-K。預設 1/1000 時，邊界的 1σ 約 0.3% K，對比例的影響約 0.03–0.1pp，遠小於 query 抽樣的區間（約 ±1.5pp）。
- 報告每個 batch 的結果，以及兩種合併：只用 `forward` 的 label，和全部 label。`diff_vs_first_ci95` 是和第一個 profile 的成對差異（同一組 query 重抽）。
- 在同一批上調參又報告會偏高。調參用 t−1，報告用 t。
- caps、`prior_rate_pseudo_golden`、`url_date_tau_days` 是全域設定；要掃它們，就每個值疊一個 `--config` overlay。label 寫入時的 filters 必須和現在相同，否則拒絕。

### 候選訊號（所有 profile 的權重都是 0）

| 訊號 | 定義 |
|---|---|
| `prior_rate` | 網域的平滑 golden 比例。每個過去 batch b 的截止時間 t_b = 建立時間 − `build_start_h_before_measure`，權重 w_b 依 `prior_half_life_days` 衰減。e = Σ w_b·（到 t_b 為止已抓的候選數），g = Σ w_b·（原始字串等於到 t_b 為止已抓候選的 golden 數）。r = (g + a)/(e + a/p0)，取 ln(r/p0) 截在 ±ln(`prior_rate_lift`) 再映到 [0, 1]。沒有統計的網域是 0.5 |
| `domain_ext` | 網域內所有候選頁的外部連入總數，log 後除以 `caps.domain_ext_inlinks` 的 log |
| `url_date` | 路徑裡的日期（`selection.url_date_regex`）的新鮮度，exp(−天數 / `url_date_tau_days`)；沒有日期或日期無效時是 0 |
| `json_ld`、`hreflang`、`no_title` | 0/1 旗標 |

- 網域統計只在 profile 用到網域訊號時才計算（一次 `GROUP BY domain_id`）。現有 profile 產生的 SQL 和以前相同。
- p0 用 `math.fsum` 算，結果和列的順序無關，`key52` 每次都一樣。
- `prior_rate` 的分母依賴「每個 URL 只抓一次」。Phase 0b 要確認 `num_fetch_ok_90d > 1` 的比例接近 0。
- 不採用 `first_seen` 新鮮度：golden 會讓 crawler 去爬它們，注入時也會重設 `first_seen`，回測一定偏高。

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
- gc 只把有完成標記的 golden、run、backtest、forward_eval 算進保留份數；失敗留下的半成品目錄直接刪。feature set 只保留 pointer 和壓縮後的那份。daemon 每個 tick 開頭都會先跑 gc。
- labels 每期約 0.2 GB，永遠保留。
- 前瞻評估不 select，只寫評估結果和 labels，磁碟用量遠小於 BUILD。

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
- 量測後的回測：重建出和線上相同的清單，新 batch 標樣本外，數字等於 metric SQL；早於 feature set 的 batch 標樣本內；labels 的 `selected_exact` 比例等於 `indexed_exact_rate`
- cutoff loss：只有在 feature set 之後、batch 之前爬到的頁面才計入
- sweep：`sample_mod = 1` 時，每個 profile 的數字和信賴區間都等於完整 `backtest --profile`；單元測試另外用大量同分、跨 K 的 tier、跨 shard 的重複 URL 和原始拼法，比對正式流程的 top-K
- 現有 profile 的清單和改動前完全相同（`baseline_v1`；`ext_tier_v1` 因為拿掉 `fetch_ok` 而改變）
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
  - 前瞻評估：沒有新 batch 時記 `no_new_batch`；有新 batch 時對 live run 寫出樣本外 labels，不重新 select，也不需要 crawlerdb 和 selectdb admin；沒有 live run、或量測後換過清單時跳過；metricdb 讀不到時進冷卻並重試

## 部署（Phase 0 之後才做）

1. Host1：`compose.selectdb.yaml` 起我們的 selectdb（:5445，資料放在 root NVMe），接著跑 `idxsel init-selectdb`。
2. Host2：準備 `.env`（chmod 600）和 `config/idxsel.host.toml`，再用 `compose.compute.yaml` 起 daemon。
3. Host1 探測：在 `~/.ssh/authorized_keys` 加一把 forced-command key，限定只能執行
   `command="df -B1 --output=target,avail / /data; cat /proc/pressure/io",no-pty,no-port-forwarding,no-agent-forwarding`。
4. 讀 crawlerdb heap 之前，要取得助教或維運同意，而且 Phase 0c 的小量讀取驗收必須通過。
