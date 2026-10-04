# Index Selection V2 架構與演算法詳解

- **程式位置：** `IndexSelectionV2/src/idxsel/`，每一節標出對應的檔案
- **相關文件：** 總覽、指令和磁碟預算見 [`../README.md`](../README.md)

---

## 1. 部署架構

| 元件 | 位置 | 做什麼 | 資源上限 |
|---|---|---|---|
| 計算容器（idxsel daemon） | Host2，`compose.compute.yaml` | 匯出、計算、載入、排程 | 16 核、64 GB、不用 swap |
| 工作目錄 | Host2 `/home/r14725023/idxsel/data`，掛載為 `/work` | 存 Parquet、狀態檔和 log | Host2 根目錄至少保留 150 GB |
| 我們的 selectdb | Host1 `:5445`，`compose.selectdb.yaml` | 存選出的清單 | 4 核、16 GB，放在根目錄的 NVMe |
| crawlerdb | Host1 `:5432` | 資料來源，只讀 | — |
| metricdb | Host1 `:5433` | golden set 來源，只讀 | — |
| Host1 探測 | Host1 上的 forced-command SSH key | 只能執行 `df` 和讀 `/proc/pressure/io` | — |

- 計算容器設 `cpu_shares 256`。它和爬蟲搶 CPU 時，爬蟲優先。
- 計算容器設 `oom_score_adj 1000`。記憶體不足時，系統先殺它。
- selectdb 用 `wal_level=minimal`，大量載入時幾乎不寫 WAL。這個設定只能用在獨立的 Postgres instance。
- 官方 metric 目前讀 PR #4 的 `:5444`。metric 的 cron 把 `--select_db_url` 改成 `:5445` 之後，才會讀這份清單。

資料流：

```
crawlerdb (read-only, paced)
  ├─ base : url_state_current_NNN by ctid chunks     (every 4 cycles)
  ├─ delta: url_state_history_NNN by snapshot_id     (daily 05:00 UTC)
  └─ dims : domain_state, shard_split_subdomain
            ▼ Parquet in the work dir
metricdb (read-only) ──► past-golden URLs + time-decayed domain prior
            ▼
features (latest observation per URL) → score → exact top-K
            ▼
our selectdb :5445 (new table → rename swap) → verify-live, eval
```

## 2. 一輪的流程（`pipeline.py`）

`run --once` 依下面的順序跑完一輪：

| # | 階段 | 程式 | 讀 | 寫 |
|---|---|---|---|---|
| 1 | export | `export.py` | crawlerdb（唯讀） | `exports/base_*`、`delta_*`、`dims_*` |
| 2 | golden | `golden.py` | metricdb（唯讀）、dims | `golden/<id>/` |
| 3 | features | `features.py` | base 或上一份 feature set、delta | `features/<id>/` |
| 4 | score | `scoring.py` | features、golden | `runs/<id>/scored/`，用完即刪 |
| 5 | select | `select_topk.py` | scored | `runs/<id>/selected.parquet` |
| 6 | load | `load.py` | `selected.parquet` | selectdb 的新表 `selected_urls_r<id>` |
| 7 | swap | `load.py` | — | selectdb 的表名 |
| 8 | verify-live、eval | `evaluate.py` | selectdb（reader 角色）、golden | `eval.json`、`report.md`、`model_runs` |
| 9 | gc | `pipeline.py` | — | 刪除過期的目錄 |

- daemon 的 BUILD 只做第 2 到第 9 步，不讀 crawlerdb。它用每日 delta 已匯出的資料，所以 crawlerdb 卡住時仍能產出清單。
- verify-live 和 eval 失敗時，已上線的清單不會被撤回。
- `backtest` 只做第 2 到第 5 步和 eval。它不讀 crawlerdb，不寫 selectdb，也不改正式流程的狀態。

## 3. 匯出（`export.py`）

| 種類 | 來源表 | 切法 | 頻率 |
|---|---|---|---|
| base（全量） | `url_state_current_NNN` | ctid 區塊，每塊 16,384 頁，也就是 128 MiB | 每 4 期一次 |
| delta（增量） | `url_state_history_NNN` | `snapshot_id` 區間，每塊 500,000 個 id | 每天 05:00 UTC |
| dims | `domain_state`、`shard_split_subdomain` | ctid 區塊 | 和 base 一起 |

**讀取的欄位：**

- 17 個欄位直接複製：`url`、`domain_id`、`first_seen`、`last_fetch_ok`、`num_fetch_ok_90d`、`num_consecutive_fail`、`inlink_count_approx`、`inlink_count_external`、`parent_page_score`、`url_score`、`source`、`discovery_source_type`、`robots_bits`、`is_redirect`、`redirect_hop_count`、`has_json_ld`、`hreflang_count`。
- 3 個衍生欄位：
  - `has_anchor`：`anchor_text` 不是空值。
  - `title_null`：`title` 是空值。
  - `content_hash64`：`content_hash` 的 64 位元雜湊。
- `observed_at`：base 用讀取當下的時間，delta 用 `snapshot_at`。

**規則：**

- V2 只讀已爬的列。base 和 delta 的查詢都有 `last_fetch_ok IS NOT NULL`。
- 每塊是一個獨立的 autocommit `COPY ... TO STDOUT (FORMAT BINARY)`。查詢不排序也不彙總，所以 server 端不會產生暫存檔。
- 每 32 塊寫成一個 Parquet 檔，約 4 GiB heap。這個檔是續跑的單位，已完成的檔不會重讀。
- shard 依 ingestor 輪流讀，順序是 0、16、32、…、240、1、17、…。`shard // 16` 是負責該 shard 的 ingestor，所以相鄰兩次讀取會落在不同 ingestor 上。
- 每個 shard 第一次開始時，V2 把區塊範圍和 history watermark（當時最大的 `snapshot_id`）記在 `_shards/NNN.start.json`。續跑時沿用同一份計畫。
- 讀完一個 shard 後，V2 會再查一次表的大小。表變大時，它追加新的區塊範圍。
- 一次全量包含 base、dims 和 catch-up delta。catch-up delta 補上讀 base 期間寫進 history 的變更。

**delta 的細節：**

- 讀取範圍是「上次 watermark − 50,000」到「現在最大的 `snapshot_id`」。往回多讀 50,000 個 id，是為了接住晚 commit 的列。
- `snapshot_at` 的下限是上次 watermark 的時間減 6 小時。
- 只取 `robots_bits <> 0` 的完整列。accounting 每天寫入的不完整列會被擋掉。
- 如果 history 已經刪過了上次的 watermark，delta 會回報 NeedFullRefresh，下一輪改做全量。

**每一塊的執行順序：**

1. 檢查期限。過了期限就停下，並保留進度。
2. 執行 guard 檢查。ABORT 就停止，PAUSE 就等待後重查，SLOW 就降速。
3. watchdog 的連線不健康時，視同 PAUSE。
4. 執行 COPY。查詢被取消或斷線時重試，最多 5 次，間隔依序是 5、15、60、300 秒。
5. 依限速器 sleep。

## 4. golden 與網域先驗（`golden.py`、`urlnorm.py`）

V2 從 metricdb 的 `metric_batches`、`metric_queries`、`metric_url` 讀出每個 golden URL 的 batch、query 和 tag（head 或 random）。

每個 golden URL 會再算兩個值：

- **canonical_url：** 用 w3lib 的 `canonicalize_url` 正規化。爬蟲存 URL 時用同一個函式，所以兩邊的拼法一致。
- **domain_key：** host 在 `shard_split_subdomain` 白名單裡時，用整個 host。不在時，用 eTLD+1（註冊網域，例如 `bbc.co.uk`）。這和爬蟲分 shard 的規則相同。V2 再用 domain_key 對到 `domain_state`，得到 `domain_id`。

`golden.target_batch` 決定哪些 batch 算「過去」：

- **`"next"`（正式流程）：** 建立時間不晚於 as_of 的 batch 都算過去。as_of 是最新一次匯出的完成時間。
- **整數 N（回測）：** 只有 batch < N 算過去。as_of 是 batch N 的建立時間。

| 產出 | 內容 |
|---|---|
| `golden.parquet` | 全部 golden 列，加上 canonical_url 和 domain_id |
| `past_golden.parquet` | 過去 batch 的 canonical URL，以及它最新一次的原始拼法 |
| `domain_prior.parquet` | 每個網域的 golden 先驗 |

網域先驗的公式：

```
prior(d) = Σ_b  n(d, b) × 0.5 ^ (age_b / 90)
```

- `n(d, b)`：網域 d 在 batch b 中不重複的 canonical golden URL 數。
- `age_b`：batch b 建立到 as_of 經過的天數。
- 90 是半衰期，由 `golden.prior_half_life_days` 設定。90 天前的 batch，權重只剩一半。

## 5. features：合併多次觀測（`features.py`）

同一個 URL 可能同時出現在 base 和好幾份 delta 裡。V2 每個 URL 只留一列，規則是「最新的觀測勝出」。比較順序如下：

1. `obs_rank` 大的優先。delta 列的 `obs_rank` 是 `snapshot_at`。base 列的 `obs_rank` 是讀取時間減 600 秒。
2. `obs_seq` 大的優先。`obs_seq` 是 delta 列的 `snapshot_id`。
3. `last_fetch_ok` 新的優先。

**base 為什麼要減 600 秒：**

- base 讀到某一列時，寫這列新值的交易可能還沒 commit。
- 這筆新值之後會出現在 history。它的時間戳記可能早於 base 的讀取時間。
- 往前扣 600 秒，history 的新值就能勝過 base 的舊值。

**為什麼需要 `obs_seq`：** ingestor 會把同一批的重複 URL 拆成多個語句，但它們在同一個交易裡，共用同一個 `snapshot_at`。這時用 `snapshot_id` 判斷，後寫的勝出。

**合併壓縮：**

- 每次建好的 feature set，會成為下一次合併的來源。它保留每列的 `obs_rank` 和 `obs_seq`。
- 所以「上一份 feature set 加新 delta」的結果，和「base 加全部 delta」完全相同。
- 已併入的 base 和 delta 會立刻刪除。磁碟只需要放一期的 delta。

**已知限制：** 既有 URL 的連入數增加時，爬蟲不會寫 history。所以連入數只在 URL 被重抓或全量匯出時更新。V2 因此每 4 期做一次全量。

## 6. 評分（`scoring.py`）

**候選：** 候選是 feature set 中通過 `selection.filters` 的列。正式設定不過濾，所以全部已爬 URL 都是候選，約 15.3 億列。

**第一步：tier。** profile 的 `tiers` 依序給分。baseline_v1 只有一個 tier：

| tier | 條件 |
|---|---|
| 1 | URL 等於某個過去 golden 的 canonical URL |
| 0 | 其他 |

ext_tier_v1 有兩個 tier：過去 golden 是 2，有外部連入（`inlink_count_external > 0`）是 1，其他是 0。

**第二步：訊號正規化。** 每個訊號都換算到 [0, 1]。計數型的訊號用對數加固定上限：

```
s = min(ln(1 + x), ln(1 + cap)) / ln(1 + cap)
```

上限 `cap` 寫在 `selection.caps`。V2 不用 min-max 正規化，所以某處出現極端值，不會改變其他 URL 的分數。

**第三步：加權。**

```
score_unit = (Σ wᵢ·sᵢ − Σ min(0, wᵢ)) / (Σ max(0, wᵢ) − Σ min(0, wᵢ))
```

- 結果夾在 [0, 1] 之間。算不出來（NULL）時當成 0。
- 權重可以是負數，例如 ext_tier_v1 的 `query = −0.5`。分母會把範圍拉回 [0, 1]。

**第四步：排序用的值。**

- `score = tier + score_unit`。所以 tier 1 的 URL 一定排在 tier 0 前面。
- `key52 = floor(score_unit × 2⁵²)`。排序用這個 52 位元整數，避免浮點數比較的誤差。
- `h` 是 URL 的 MD5 雜湊的低 64 位元。同分時，V2 用它決定順序。

**baseline_v1 用到的 4 個訊號：**

| 訊號 | 來源欄位 | 算法 | 上限 | 權重 |
|---|---|---|---|---|
| inlinks | `inlink_count_approx` | 對數加上限 | 10,000 | 1 |
| ext | `inlink_count_external` | 對數加上限 | 1,000 | 1 |
| url_score | `url_score` | `source = 0` 且值在 [0, 1] 時採用，否則是 0 | — | 1 |
| prior | 網域先驗 | 對數加上限 | 200 | 2 |

所以 baseline_v1 的公式是：

```
score = tier + (inlinks + ext + url_score + 2 × prior) / 5
```

url_score 只採用 `source = 0` 的列，原因是 `source ≠ 0` 的列是注入的，例如 golden 注入和 wiki pageview 注入。這些列的 url_score 是爬蟲設的固定值，不是 scorer 算出的分數。

**算例：** 一個 URL 有 100 個連入、10 個外部連入，url_score 是 0.6，網域先驗是 20。

- inlinks = ln 101 / ln 10001 ≈ 0.501
- ext = ln 11 / ln 1001 ≈ 0.347
- url_score = 0.6
- prior = ln 21 / ln 201 ≈ 0.574
- score_unit = (0.501 + 0.347 + 0.6 + 2 × 0.574) / 5 ≈ 0.519
- 它不是過去 golden 時，score ≈ 0.519。它是過去 golden 時，score ≈ 1.519。

**權重為 0 的 14 個訊號：** 程式已經實作，但 baseline_v1 不啟用。它們要等 sweep 在樣本外的 labels 上證明有效，才會打開。

| 訊號 | 定義 |
|---|---|
| parent | 父頁分數 `parent_page_score`，上限 1.0；空值補 0 |
| short | 1 − min(URL 長度, 200) / 200 |
| root | URL 是網站首頁時為 1 |
| query | URL 含 `?` 時為 1 |
| anchor | 有 anchor text 時為 1 |
| redirect | 是轉址頁時為 1 |
| fresh | exp(−距上次抓取的天數 / 30) |
| pageview | wiki pageview 注入的列（`source = 2`）取 url_score − 1 |
| prior_rate | 網域的平滑 golden 比例，取它相對全體平均的對數 lift，截在 ±ln 20 後映到 [0, 1] |
| domain_ext | 網域內所有候選頁的外部連入總和，上限 1,000,000 |
| url_date | 路徑裡的日期，exp(−天數 / 30)；沒有日期時為 0 |
| json_ld | 頁面有 JSON-LD 時為 1 |
| hreflang | 頁面有 hreflang 時為 1 |
| no_title | 頁面沒有 title 時為 1 |

## 7. 精確 top-K（`select_topk.py`）

**排序規則：** tier 由大到小，接著 key52 由大到小，再來 h、url、shard 由小到大。url 和 shard 只在 MD5 碰撞，或同一個 URL 出現在兩個 shard 時才用到。

**為什麼不直接全排序：** 候選約 15.3 億列。全域排序會讓 DuckDB 寫大量暫存檔，而暫存上限只有 60 GB。

**radix select 的做法：**

1. 把排序 key 切成 9 個「位數」：tier 一個；key52 切成 16、16、16、4 位元；h 反轉後切成 4 個 16 位元。
2. 每一輪對目前的桶做一次 GROUP BY，數出每個位數值各有幾列。
3. 從大到小累加，找出第 K 列落在哪個位數值。這個值就是邊界。
4. 比邊界大的列一定入選。邊界那一格成為下一輪的桶。還需要的列數，扣掉已入選的數量。
5. 桶不超過 2,000,000 列（`selection.exact_sort_max_rows`）時停止。V2 只對這個桶精確排序，取出需要的列數。

每一輪只讀 tier、key52、h 三個窄欄位。在正式 crawlerdb 的 shard 212 上驗證時，有 296 萬個候選，V2 只做了 2 次 GROUP BY 就找到邊界。

**跨 shard 的重複 URL：**

- 網域搬到別的 shard 後，同一個 URL 可能同時存在於兩個 shard。
- 選完後，V2 檢查清單裡有沒有重複的 URL。有的話，只保留排序最好的那份，排除其他份，然後重選。
- 重選超過 5 輪（`selection.max_dedupe_rounds`）仍有重複時，V2 改用 h 一次排除所有跨 shard 的副本。這保證流程會結束。
- 最後 V2 確認清單剛好 K 列。候選不足 K 列時，它直接報錯。

`exact_sort_max_rows` 和 `max_dedupe_rounds` 只影響速度，不影響結果。用假資料在伺服器上驗證時，`exact_sort_max_rows` 設成 50，強制走 radix 路徑，結果仍然正確。

## 8. 載入、上線、回滾（`load.py`）

selectdb 的表結構和 PR #4 相同，所以 metric 原本的查詢可以直接用：

```sql
SELECT url FROM public.selected_urls_current WHERE url = ANY(:urls)
```

**載入（交易 1）：**

1. 刪掉「已載入但還沒上線」的舊表，並把它們標成 abandoned。
2. 檢查空間。資料庫大小加上 K × 260 bytes，不能超過 100 GB。空間不夠時，先刪 `selected_urls_prev`。
3. 在 `model_runs` 新增一列，狀態是 loading。
4. 建立新表 `selected_urls_r<id>`，用 COPY FREEZE 載入。
5. 加上主鍵和外鍵，再執行 ANALYZE。主鍵保證 URL 不重複。
6. 檢查列數等於 K，把狀態改成 validated，然後 commit。

COPY FREEZE（載入時直接把列標成已凍結、對所有交易可見）有兩個好處：

- 新表在同一個交易裡建立，加上 `wal_level=minimal`，所以載入幾乎不寫 WAL。
- 讀者第一次讀新表時，不需要回寫 hint bit，所以不會產生額外的寫入。

**上線（交易 2）：**

1. 設 `lock_timeout` 5 秒，對 `selected_urls_current` 取 ACCESS EXCLUSIVE 鎖。
2. 刪掉舊的 `selected_urls_prev`。
3. 把 current 改名成 prev，再把新表改名成 current。
4. 更新 `model_runs`：舊的 live 改成 retired，新的改成 live。

- 拿不到鎖時，V2 最多重試 12 次，每次間隔 300 秒。
- 量測日 11:30–20:00 UTC 不上線。這讓 random（12:00）和 head（18:00）兩次量測看到同一份清單。

**回滾：** V2 在一個交易裡做三次改名，把 current 和 prev 對調。回滾也會刪掉還沒上線的表，所以排程中等待的 swap 不會把回滾蓋回去。

**稽核：** `model_runs` 記錄 profile、權重、config 的 SHA-256、程式版本和評估結果。寫入前，V2 會檢查內容不含密碼或 DSN。

## 9. 評估（`evaluate.py`、`labels.py`）

**複製 metric 的規則：** 官方 metric 判斷 indexed 的規則是「原始 golden 字串完全等於清單裡的一列」。V2 用同一規則，直接對這次 run 的 `selected.parquet` 計算。

每個 batch 和 tag 都會報告下面的數字：

| 欄位 | 意思 |
|---|---|
| `indexed_exact_rate` | 原始字串完全比對時的入選比例，等於官方 metric |
| `indexed_canonical_rate` | 正規化後比對的入選比例 |
| `crawled_rate`、`crawled_exact_rate` | golden 已爬的比例，分正規化比對和完全比對 |
| `recall_of_crawled` | 已爬的 golden 中，被選進清單的比例 |
| `cutoff_loss` | feature set 之後、batch 建立之前才被爬到的 golden |
| `indexed_exact_ci95` | 95% 信賴區間 |
| `in_sample` | 這個數字是不是樣本內 |

- 信賴區間用群集 bootstrap 算，重抽 1,000 次。每次重抽整個 query，而不是單一 URL。
- 一個 URL 出現在 c 個 query 時，它在每個 query 裡的權重是 1/c。

符合下面任一條時，數字標成樣本內：

- 這個 batch 進了 past-golden tier 或網域先驗。
- 這個 batch 在 feature set 的 as_of 之前就已建立。爬蟲會依新 batch 調整爬取，所以這些 URL 可能正是因為是 golden 才被爬到。

**verify-live：** V2 用 metric 使用的 reader 角色連 selectdb，檢查三件事：

- 線上表有 K 列。
- 線上表的 `run_id` 等於這次 run。
- 從清單隨機抽 1,000 個 URL，全部查得到。

**前瞻評估：** 量測後 20 小時，daemon 找出量測當下 live 的清單，再對新出現的 batch 打分。它不重選，不讀 crawlerdb，也不寫 selectdb。量測後換過清單時，它會跳過，不猜是哪份清單被量到。

**labels 和 sweep：**

- labels 是每個 golden batch 的特徵日誌。它包含 golden URL 的特徵列，以及 1/1000 的候選雜湊樣本。
- sweep 用 labels 估計各 profile 的 indexed 比例，幾秒內完成。它在樣本中取排名 round(K × n / N) 的列當邊界。n 是樣本列數，N 是候選總數。
- `labels.sample_mod = 1` 時，sweep 的結果等於精確的 top-K。

## 10. 排程（`schedule.py`、`pipeline.py`）

daemon 每 300 秒醒來一次，這叫一個 tick。每個 tick 先取工作目錄鎖，再依下面的優先順序選一件事做：

1. 現在是禁止執行時段：等待。
2. 需要全量，而且距離量測 24–55 小時：做全量匯出。
3. 還沒為這次量測建清單、距離量測不超過 30 小時，而且不在 swap 凍結時段：做 BUILD。
4. 上次量測的是我們建的清單、量測後已過 20 小時，而且還沒做前瞻評估：做前瞻評估。
5. 已過 05:00 UTC，而且距上次 delta 至少 20 小時：做每日 delta。
6. 都不符合：等待。

量測在每月 1 日和 16 日的 12:00 UTC。以量測時間 D 日 12:00 為例：

| 時間（UTC） | 事件 |
|---|---|
| D−3 日 05:00 起 | 全量匯出的視窗開始，距量測 55 小時 |
| 每天 05:00 | 每日 delta，最多跑 2 小時 |
| D−1 日 06:00 起 | BUILD 的視窗開始，距量測 30 小時 |
| D−1 日 12:00 | 全量匯出的期限，距量測 24 小時。沒做完就保留進度，BUILD 改用上一份 feature set 加 delta |
| D 日 00:00–09:30 | 量測日不讀 DB，避開 PR #4 的執行和 metric 08:00 的 count(*) |
| D 日 11:30–20:00 | 不換清單 |
| D 日 12:00、18:00 | metric 分別量測 random 和 head |
| D+1 日 08:00 | 前瞻評估，量測後 20 小時 |

另外，每天 02:50–04:40 UTC 不讀 DB。這是為了避開 accounting 03:00 刪 partition，以及 04:00 的 gc。

**錯誤處理：**

- 只有全量才能修的問題，例如 history 已刪或壓縮後的 feature set 缺檔：設定 `force_full`，在下一個全量視窗重建。
- guard 拒絕或中止：記錄 `daemon.deferred`，下一個 tick 再試。
- 其他錯誤：冷卻 1,800 秒。冷卻期間，已載入的清單仍會在允許時上線。
- metricdb 讀不到：沿用上一份 golden。
- selectdb 連不上：在 features 之前就失敗，不白跑。

## 11. 安全機制（`guards.py`、`throttle.py`、`pg.py`）

**guard 的判定：** 每項檢查回傳 OK、SLOW（降速）、PAUSE（暫停）或 ABORT（中止）。啟動前的檢查叫 preflight，PAUSE 以上都會拒絕啟動。執行中，V2 採用最嚴重的那個判定。

| 檢查 | 門檻 | 啟動前 | 執行中 |
|---|---|---|---|
| 禁止執行時段 | 見 §10 | 拒絕 | PAUSE |
| crawlerdb 的 datfrozenxid age | ≥ 2.5 億 | 拒絕 | ABORT |
| 防 wraparound 的 vacuum 數 | ≥ 2 | 拒絕 | PAUSE |
| 別人的重查詢 | `indexselection_v1%`，或跑超過 300 秒的查詢 | 拒絕 | PAUSE |
| 剩餘連線數 | < 20 | 拒絕 | PAUSE |
| 等 DataFileRead 的連線數 | ≥ 40 / ≥ 80 | 不擋 / 拒絕 | SLOW / PAUSE |
| 等 LWLock 的連線數 | ≥ 20 | 不擋 | SLOW |
| Host1 IO PSI avg60 | ≥ 35% / ≥ 60% | 不擋 / 拒絕 | SLOW / PAUSE |
| Host1 `/data` 剩餘 | 啟動 < 500 GB，執行中 < 450 GB | 拒絕 | ABORT |
| Host1 `/` 剩餘 | < 450 GB | 拒絕 | ABORT |
| Host2 工作磁碟剩餘 | 扣掉預留後 < 150 GB | 拒絕 | ABORT |
| 工作目錄大小 | > 500 GB | 拒絕 | ABORT |
| Host2 MemAvailable | 啟動 < 300 GB，執行中 < 200 GB | 拒絕 | ABORT |
| 探測失敗 | — | 拒絕 | 遠端探測 ABORT，DB 探測 PAUSE |

- crawlerdb 的檢查只套用在會讀 crawlerdb 的階段。BUILD 和 load 的 preflight 不檢查禁止執行時段和 `/data`。它們仍會檢查磁碟、記憶體和 Host1 的 IO 壓力。
- 遠端探測的結果會快取 60 秒，避免每塊都 SSH 一次。

**限速器（AIMD，加法增、乘法減）：**

- 每塊讀完後 sleep，讓平均讀取速度等於目標速度。sleep 時間是「這塊的 heap 大小 / 目標速度 − 實際讀取時間」。
- 目標速度一開始是 60 MB/s，範圍是 20–150 MB/s。
- 連續 5 塊判定 OK，目標速度加 10 MB/s。
- 判定 SLOW 或 PAUSE，目標速度減半。
- 判定 PAUSE 時，V2 依序等 60、120、300、900 秒，再重新檢查。
- 限速只發生在塊與塊之間。查詢執行中不停頓，因為停頓會讓 snapshot 和鎖存在更久。

**watchdog：**

- watchdog 用一條獨立連線，每秒檢查一次：有沒有正在等鎖的連線被我們擋住。
- 有的話，V2 立刻取消自己的查詢。
- 原因是：我們的 ACCESS SHARE 鎖會讓排隊中的 ACCESS EXCLUSIVE 請求等待，例如 accounting 刪 partition。之後所有寫入都會排在那個請求後面。
- watchdog 自己的連線斷掉時，匯出視同 PAUSE，不開始新的讀取。

**唯讀的兩層保護：**

- 伺服器端：每條 crawlerdb 和 metricdb 連線都設 `default_transaction_read_only=on`。
- 客戶端：每條 SQL 送出前都會檢查。只允許單一的 SELECT、WITH、SHOW、EXPLAIN（不含 ANALYZE）和 `COPY ... TO STDOUT`。含寫入關鍵字或 `FOR UPDATE` 的語句會被拒絕。
- 兩層都需要，因為 crawlerdb 的帳號是 superuser。

**其他連線設定：** `statement_timeout` 60 秒、`lock_timeout` 1 秒、`temp_file_limit` 0、不開平行 worker、不開 JIT、`work_mem` 16 MB。

**工作目錄鎖：** 會改工作目錄的指令，都要先取 `state/lock`（flock）。拿不到鎖時，指令以回傳碼 4 結束。另外，`state/daemon.lock` 會擋住第二個 daemon。

## 12. 參數

### 12.1 指令列

```
idxsel --config <主設定檔> [--config <覆寫檔> ...] <指令> [選項]
```

- `--config` 可以給多次，後面的檔案覆寫前面的。
- 回傳碼：0 是成功，2 是設定錯誤，3 是 guard 拒絕或中止，4 是工作目錄被占用。

| 指令 | 選項 | 作用 |
|---|---|---|
| `init-selectdb` | — | 建表和 reader 角色，只需跑一次 |
| `preflight` | `--no-db` | 檢查現在能不能開始匯出 |
| `probe` | — | 印出目前的 guard 判定 |
| `export` | `--mode full\|delta`（必填）、`--shards`、`--dry-run` | 匯出；`--dry-run` 只讀 catalog 估算大小 |
| `golden`、`features`、`select`、`load`、`eval` | — | 單獨跑一個階段 |
| `swap` | `--run-id` | 讓指定的 run 上線，預設是最新載入的 run |
| `rollback` | — | 切回上一份清單 |
| `verify-live` | — | 用 reader 角色檢查線上清單 |
| `run` | `--once`（必填）、`--full`、`--shards` | 跑完整一輪 |
| `backtest` | `--profile` | 在最新的 feature set 上回測 |
| `sweep` | `--profiles`、`--batches` | 用 labels 比較 profile |
| `daemon` | `--max-ticks` | 依排程常駐執行 |
| `status`、`gc` | — | 查看狀態、清理舊資料 |

`--shards` 的格式是 `0-3,7,10-12`。

### 12.2 環境變數

密碼和 DSN 不寫在設定檔裡。設定檔只寫環境變數的名稱。

| 環境變數 | 用途 |
|---|---|
| `IDXSEL_CRAWLERDB_DSN` | crawlerdb 連線，唯讀 |
| `IDXSEL_METRICDB_DSN` | metricdb 連線，唯讀 |
| `IDXSEL_SELECTDB_ADMIN_DSN` | 我們的 selectdb，寫入用 |
| `IDXSEL_SELECTDB_READER_DSN` | 我們的 selectdb，reader 角色；verify-live 和前瞻評估用 |
| `IDXSEL_SELECTDB_READER_PASSWORD` | 只有 `init-selectdb` 用，設定 reader 角色的密碼 |

### 12.3 設定檔的載入規則

- 主設定檔是 `config/idxsel.example.toml`，每個 key 都必填。
- 多出未知的 key、缺少 key 或型別錯誤，都會直接報錯，不會默默套用預設值。
- 載入後，V2 還會檢查數值之間的關係。例如限速要符合 min ≤ initial ≤ max，profile 的權重不能全是 0。
- 每次 run 都會記錄有效設定的 SHA-256，寫進 `model_runs`，方便事後稽核。
- `config/idxsel.local.example.toml` 是筆電上試跑用的覆寫檔：k = 1,000，關掉排程和遠端探測。

### 12.4 會改變選取結果的參數

| 參數 | 正式值 | 作用 |
|---|---|---|
| `run.k` | 128,000,000 | 清單大小 |
| `selection.profile` | `baseline_v1` | 使用哪一組 tier 和權重 |
| `selection.profiles.<名稱>.tiers` | `["past_golden"]` | tier 的種類和順序 |
| `selection.profiles.<名稱>.weights.*` | 見 §6 | 18 個訊號的權重 |
| `selection.caps.inlinks` | 10,000 | 連入數的上限 |
| `selection.caps.ext_inlinks` | 1,000 | 外部連入數的上限 |
| `selection.caps.prior` | 200 | 網域先驗的上限 |
| `selection.caps.parent_page_score` | 1.0 | 父頁分數的上限 |
| `selection.caps.prior_rate_lift` | 20 | prior_rate 的對數 lift 截在 ±ln 20 |
| `selection.caps.domain_ext_inlinks` | 1,000,000 | 網域外部連入總和的上限 |
| `selection.caps.url_len` | 200 | short 訊號的 URL 長度上限 |
| `selection.caps.fresh_tau_days` | 30 | fresh 訊號的衰減天數 |
| `selection.caps.url_date_tau_days` | 30 | url_date 訊號的衰減天數 |
| `selection.impute.parent_page_score` | 0.0 | 父頁分數是空值時的補值 |
| `selection.impute.url_score` | 0.0 | url_score 不能用時的補值 |
| `selection.url_score_natural_source` | 0 | 只有這個 source 的 url_score 會被採用 |
| `selection.pageview_source` | 2 | wiki pageview 注入列的 source |
| `selection.emit_raw_spelling_for_past_golden` | false | true 時，過去 golden 以原始拼法寫進清單 |
| `selection.url_date_regex` | 正規表示式 | 從路徑抓年、月、日 |
| `selection.filters.exclude_robots_bits` | `[]` | 要排除的 robots_bits；空的代表不排除 |
| `selection.filters.max_consecutive_fail` | −1 | 連續失敗次數的上限；−1 代表不過濾 |
| `golden.target_batch` | `"next"` | 正式流程用 next；回測時給 batch id |
| `golden.prior_half_life_days` | 90 | 網域先驗的半衰期 |
| `golden.prior_rate_pseudo_golden` | 1.0 | prior_rate 的平滑虛擬計數 |
| `golden.tldextract_offline` | true | 用內建的公共字尾表，不連網 |
| `export.base_observation_slack_s` | 600 | base 列的觀測時間往前扣幾秒 |
| `export.full_refresh_every_n_cycles` | 4 | 每幾期全量匯出一次，決定連入數有多新 |
| `selection.exact_sort_max_rows` | 2,000,000 | 桶小於這個列數就直接排序；只影響速度 |
| `selection.max_dedupe_rounds` | 5 | 跨 shard 去重的最大輪數；只影響速度 |

### 12.5 資料來源

| 參數 | 正式值 | 作用 |
|---|---|---|
| `run.work_dir` | `/work` | 工作目錄 |
| `run.app_name_prefix` | `idxsel_r14725023` | 每條 DB 連線的 application_name 前綴，用來辨識和取消 |
| `crawlerdb.dsn_env` | `IDXSEL_CRAWLERDB_DSN` | DSN 所在的環境變數 |
| `crawlerdb.num_shards` | 256 | shard 數 |
| `crawlerdb.url_table_fmt` | `url_state_current_{:03d}` | current 表名格式 |
| `crawlerdb.history_table_fmt` | `url_state_history_{:03d}` | history 表名格式 |
| `crawlerdb.domain_table` | `domain_state` | 網域表 |
| `crawlerdb.split_table` | `shard_split_subdomain` | 子網域白名單表 |
| `crawlerdb.connect_timeout_s` | 10 | 連線逾時秒數 |
| `crawlerdb.session.*` | 見 §11 | 8 個連線參數 |
| `metricdb.dsn_env` | `IDXSEL_METRICDB_DSN` | DSN 所在的環境變數 |
| `metricdb.connect_timeout_s` | 10 | 連線逾時秒數 |
| `metricdb.session.*` | 唯讀、`statement_timeout` 120s、`lock_timeout` 1s | 3 個連線參數 |
| `selectdb.admin_dsn_env` | `IDXSEL_SELECTDB_ADMIN_DSN` | 寫入用 DSN 的環境變數 |
| `selectdb.reader_dsn_env` | `IDXSEL_SELECTDB_READER_DSN` | reader DSN 的環境變數 |
| `selectdb.reader_role` | `select` | reader 角色名稱，和 metric 連線用的角色相同 |
| `selectdb.reader_password_env` | `IDXSEL_SELECTDB_READER_PASSWORD` | reader 密碼的環境變數 |
| `selectdb.connect_timeout_s` | 10 | 連線逾時秒數 |
| `selectdb.max_db_bytes` | 100 GB | selectdb 大小上限 |
| `selectdb.est_bytes_per_row` | 260 | 每列含主鍵的估計大小；1.28 億列約 31 GB |
| `selectdb.swap_lock_timeout` | 5s | swap 和 rollback 等鎖的上限 |
| `selectdb.swap_retry_attempts` | 12 | swap 重試次數 |
| `selectdb.swap_retry_backoff_s` | 300 | swap 重試間隔 |
| `selectdb.maintenance_work_mem` | 4GB | 建主鍵時的記憶體 |
| `selectdb.copy_block_bytes` | 8 MiB | COPY 每次送出的大小 |
| `selectdb.keep_prev` | true | 是否保留上一份清單供回滾 |

### 12.6 匯出與限速

| 參數 | 正式值 | 作用 |
|---|---|---|
| `export.chunk_pages` | 16,384 | 每塊讀幾頁，也就是 128 MiB |
| `export.chunks_per_file` | 32 | 幾塊寫成一個 Parquet 檔，也是續跑單位 |
| `export.shard_order` | `ingestor_round_robin` | shard 的讀取順序；另一個選項是 `sequential` |
| `export.shards_per_ingestor` | 16 | 每個 ingestor 負責的 shard 數 |
| `export.history_chunk_ids` | 500,000 | delta 每塊的 `snapshot_id` 數 |
| `export.history_overlap_ids` | 50,000 | delta 往回重讀的 id 數 |
| `export.history_prune_slack_hours` | 6 | delta 的 `snapshot_at` 下限往前放寬幾小時 |
| `export.history_row_bytes_estimate` | 420 | delta 每列的估計大小，用來限速 |
| `export.max_chunk_retries` | 5 | 每塊的重試次數 |
| `export.retry_backoff_s` | `[5, 15, 60, 300]` | 重試間隔 |
| `export.parquet_compression` | `zstd` | Parquet 壓縮法 |
| `export.parquet_compression_level` | 3 | 壓縮等級 |
| `throttle.rate_mb_s_initial` | 60 | 初始讀取速度 |
| `throttle.rate_mb_s_min` | 20 | 最低讀取速度 |
| `throttle.rate_mb_s_max` | 150 | 最高讀取速度 |
| `throttle.increase_mb_s` | 10 | 每次加速的幅度 |
| `throttle.increase_every_healthy_chunks` | 5 | 連續幾塊 OK 才加速 |
| `throttle.decrease_factor` | 0.5 | SLOW 或 PAUSE 時的減速倍率 |
| `throttle.pause_backoff_s` | `[60, 120, 300, 900]` | PAUSE 時的等待秒數 |

### 12.7 guard

| 參數 | 正式值 | 作用 |
|---|---|---|
| `guard.db_checks_enabled` | true | 是否檢查 crawlerdb 的統計視圖 |
| `guard.host_meminfo_enabled` | true | 是否檢查 Host2 的記憶體 |
| `guard.fail_closed_on_probe_error` | true | 探測失敗時是否視為不安全 |
| `guard.max_datfrozenxid_age` | 250,000,000 | 達到就 ABORT |
| `guard.max_wraparound_vacuum_workers` | 2 | 達到就 PAUSE |
| `guard.min_free_connections` | 20 | 低於就 PAUSE |
| `guard.foreign_long_query_s` | 300 | 別人的查詢跑超過這個秒數就 PAUSE |
| `guard.foreign_app_name_patterns` | `["indexselection_v1%"]` | 這些程式在跑時 PAUSE |
| `guard.db_io_wait_backends_soft` | 40 | 達到就 SLOW |
| `guard.db_io_wait_backends_hard` | 80 | 達到就 PAUSE |
| `guard.db_lwlock_wait_backends_soft` | 20 | 達到就 SLOW |
| `guard.watchdog_interval_s` | 1.0 | watchdog 的檢查間隔 |
| `guard.work_dir_max_gb` | 500 | 工作目錄的大小上限 |
| `guard.work_dir_scan_interval_s` | 60 | 工作目錄大小的重新量測間隔 |
| `guard.work_dir_min_free_gb` | 150 | Host2 根目錄至少保留的空間 |
| `guard.build_reserve_gb` | 170 | BUILD 開始時額外預留的空間 |
| `guard.host_mem_available_min_gb_start` | 300 | 啟動時 Host2 MemAvailable 的下限 |
| `guard.host_mem_available_min_gb_run` | 200 | 執行中 Host2 MemAvailable 的下限 |
| `guard.remote_data_min_free_gb_start` | 500 | 啟動時 Host1 `/data` 的剩餘下限 |
| `guard.remote_data_min_free_gb_run` | 450 | 執行中 Host1 `/data` 的剩餘下限 |
| `guard.remote_root_min_free_gb` | 450 | Host1 `/` 的剩餘下限 |
| `guard.remote_io_psi_some_avg60_soft` | 35.0 | 達到就 SLOW |
| `guard.remote_io_psi_some_avg60_hard` | 60.0 | 達到就拒絕啟動，執行中 PAUSE |
| `guard.remote.enabled` | true | 是否探測 Host1 |
| `guard.remote.command` | `ssh … probe` | 探測指令，使用 forced-command key |
| `guard.remote.data_mount`、`root_mount` | `/data`、`/` | 要看的掛載點 |
| `guard.remote.timeout_s` | 15 | 探測逾時秒數 |
| `guard.remote.interval_s` | 60 | 兩次探測的最短間隔 |

### 12.8 排程

| 參數 | 正式值 | 作用 |
|---|---|---|
| `schedule.enabled` | true | 是否啟用排程和禁止執行時段 |
| `schedule.tick_s` | 300 | daemon 的 tick 間隔 |
| `schedule.measure_days` | `[1, 16]` | 量測日 |
| `schedule.measure_time_utc` | `12:00` | 量測時間 |
| `schedule.blackout_daily_utc` | `["02:50-04:40"]` | 每天不讀 DB 的時段 |
| `schedule.blackout_measure_day_utc` | `["00:00-09:30"]` | 量測日不讀 DB 的時段 |
| `schedule.swap_freeze_measure_day_utc` | `["11:30-20:00"]` | 量測日不換清單的時段 |
| `schedule.full_export_start_h_before_measure` | 55 | 量測前幾小時開始全量 |
| `schedule.full_export_deadline_h_before_measure` | 24 | 量測前幾小時全量必須停下 |
| `schedule.build_start_h_before_measure` | 30 | 量測前幾小時開始 BUILD；prior_rate 也用它決定截止時間 |
| `schedule.daily_delta_utc` | `05:00` | 每日 delta 的時間 |
| `schedule.min_hours_between_deltas` | 20 | 兩次 delta 的最短間隔 |
| `schedule.delta_max_h` | 2.0 | 每日 delta 的時間上限，超過就停下並續跑 |
| `schedule.error_cooldown_s` | 1800 | 非預期錯誤後的冷卻秒數 |
| `schedule.forward_eval_delay_h` | 20 | 量測後幾小時做前瞻評估 |

### 12.9 計算、評估與儲存

| 參數 | 正式值 | 作用 |
|---|---|---|
| `duckdb.threads` | 16 | DuckDB 執行緒數 |
| `duckdb.memory_limit` | 40GB | DuckDB 記憶體上限 |
| `duckdb.max_temp_directory_size` | 60GB | DuckDB 暫存檔上限 |
| `duckdb.preserve_insertion_order` | false | 不保留插入順序，減少記憶體用量 |
| `eval.batches` | `[]` | 要評估的 batch；空的代表目標 batch 或最新一批 |
| `eval.tags` | `["head", "random"]` | 要評估的 tag |
| `eval.lookup_chunk_size` | 10,000 | 查線上表時每批的 URL 數，和 metric 相同 |
| `eval.bootstrap_samples` | 1000 | bootstrap 重抽次數 |
| `eval.bootstrap_seed` | 7 | 亂數種子 |
| `eval.live_check_sample_size` | 1000 | verify-live 抽查的 URL 數 |
| `labels.sample_mod` | 1000 | labels 的雜湊抽樣比例，1/1000 |
| `labels.sweep_memory_limit` | 4GB | sweep 的記憶體上限 |
| `labels.top_domains` | 20 | 診斷報告列出的網域數 |
| `storage.keep_runs` | 6 | 保留幾份完整的 run、golden、回測和前瞻評估 |
| `logging.level` | `INFO` | log 等級 |
