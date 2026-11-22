from Metric.RawDataReader.RawDataReader import RawDataReader
from Metric.Query.SerpApi import SerpApiError, serpapi_key
from serpapi import GoogleSearch
from datetime import datetime
from sqlalchemy import desc
import time
from collections import defaultdict

class DatabaseRawDataReader(RawDataReader):
    def __init__(self, db, modelFactory, update_day=14):
        """
        :param db: Database instance (含有 session context manager)
        :param modelFactory: AppModelFactory instance
        :param update_day: 資料快取天數
        """
        super().__init__()
        self.db = db
        self.modelFactory = modelFactory
        self.update_day = update_day
        
        self.countries = [
            {"geo": "US", "Country": "United States"},
            {"geo": "IN", "Country": "India"},
            {"geo": "JP", "Country": "Japan"},
            {"geo": "GB", "Country": "United Kingdom"},
            {"geo": "CA", "Country": "Canada"},
            {"geo": "DE", "Country": "Germany"},
            {"geo": "AU", "Country": "Australia"},
            {"geo": "BR", "Country": "Brazil"},
            {"geo": "FR", "Country": "France"},
            {"geo": "TW", "Country": "Taiwan"}
        ]
    
    def readData(self) -> list:
        """
        主邏輯：
        1. 檢查 DB 是否有最新的 Batch
        2. 有且未過期 -> 從 DB 讀取
        3. 無或過期 -> 爬取 API -> 存入 DB -> 回傳
        """
        MetricBatch = self.modelFactory.create_metric_batches()
        
        latest_batch = None
        
        # 1. 檢查資料庫最新紀錄
        with self.db.session() as session:
            # 假設 Batch 用於紀錄 trending 來源，我們可以透過 meta_geo_counts 判斷，
            # 或是未來可以在 Batch 加個 source 欄位。這裡先單純取最新的一筆。
            # An empty batch (a failed fetch) is never reused.
            latest_batch = session.query(MetricBatch)\
                .filter(MetricBatch.meta_total_queries > 0)\
                .order_by(desc(MetricBatch.created_at))\
                .first()
            
            # 如果有找到資料，且資料夠新 (Created time < update_day)
            if latest_batch:
                delta = datetime.now() - latest_batch.created_at
                if delta.days < self.update_day:
                    print(f'Use cached trending data from DB (Batch ID: {latest_batch.id}, Date: {latest_batch.created_at})')
                    return self._fetch_from_db(latest_batch.id)

        # 2. 資料過期或不存在，執行爬蟲並存檔
        print("Fetch Trending Query from SerpApi...")
        return self._fetch_from_api_process_and_store()

    def _fetch_from_db(self, batch_id: int) -> list:
        """
        從資料庫讀取指定 Batch 的資料，還原成 list of dict
        """
        MetricQuery = self.modelFactory.create_metric_queries()
        result_list = []
        
        with self.db.session() as session:
            queries = session.query(MetricQuery).filter(MetricQuery.batch_id == batch_id).all()
            
            for q in queries:
                result_list.append({
                    "keyword": q.keyword,
                    "frequency": q.frequency,
                    "geo": q.geo,  # DB 裡是 JSONB list ["US", "TW"]
                    # "started": ... (如果 DB 沒存 started timestamp，這裡就無法還原，視需求決定是否加欄位)
                })
        
        # 依照 frequency 排序 (大到小)
        return sorted(result_list, key=lambda x: x['frequency'], reverse=True)

    def _fetch_from_api_process_and_store(self) -> list:
        """
        從 API 獲取 -> 去重合併 -> 存入 DB -> 回傳
        Any geo that fails raises before anything is saved, so a failed fetch leaves no batch.
        """
        # 1. 獲取原始資料
        all_data = []
        for country in self.countries:
            trending = self._fetch_trending_now(country["geo"])
            all_data.extend(trending)
            # country["query nums"] = len(trending) # 這行在 DB 模式下暫時用不到，統計會存到 Batch metadata
            
        # 2. Dedup & Merge Logic
        merged_dict = {}
        for item in all_data:
            kw = item['keyword']
            
            if kw not in merged_dict:
                # 第一次出現：
                # 為了配合 DB JSONB，將 'US' 轉為 ['US']
                new_item = item.copy()
                new_item['geo'] = [item['geo']]
                merged_dict[kw] = new_item
            else:
                # 已經存在：
                # 合併 Geo List (避免重複)
                if isinstance(item['geo'], str):
                    # 防禦性檢查，雖然 _fetch_trending_now 回傳的是 str
                    new_geo = item['geo']
                else:
                    new_geo = item['geo'][0]

                if new_geo not in merged_dict[kw]['geo']:
                    merged_dict[kw]['geo'].append(new_geo)
                
                # 合併 Frequency
                merged_dict[kw]['frequency'] += item['frequency']
                # 取較早的時間
                merged_dict[kw]['started'] = min(merged_dict[kw]['started'], item['started'])

        dedup_data = list(merged_dict.values())

        # 3. 排序
        sorted_data = sorted(dedup_data, key=lambda x: x['frequency'], reverse=True)

        # 4. 存入資料庫
        self._save_to_database(sorted_data)
        
        return sorted_data

    def _save_to_database(self, data_list: list):
        """
        將處理好的資料寫入 MetricBatch 與 MetricQuery
        """
        MetricBatch = self.modelFactory.create_metric_batches()
        MetricQuery = self.modelFactory.create_metric_queries()

        # 計算 Metadata
        total_keywords = len(data_list)
        geo_counts = defaultdict(int)

        for item in data_list:
            # item['geo'] 是一個 list ["US", "TW"]
            for g in item['geo']:
                geo_counts[g] += 1

        with self.db.session() as session:
            # A. 建立 Batch
            new_batch = MetricBatch(
                created_at=datetime.now(),
                meta_total_queries=total_keywords,
                meta_total_urls=0, # 這個階段只有關鍵字，沒有爬 URL
                meta_tag_stats={},
                meta_geo_counts=dict(geo_counts)
            )
            session.add(new_batch)
            session.flush() # 取得 ID

            print(f"Saving new Trending Batch to DB. ID: {new_batch.id}, Count: {total_keywords}")

            # B. 建立 Queries
            # 這邊加上 tags=["trending"] 方便之後辨識來源
            for item in data_list:
                query = MetricQuery(
                    batch_id=new_batch.id,
                    keyword=item['keyword'],
                    geo=item['geo'],    # 存入 JSONB
                    frequency=item['frequency'],
                    tags=[]
                )
                session.add(query)
            
            session.commit()
            print("Save completed.")

    def _fetch_trending_now(self, geo: str) -> list:
        """
        從 SerpApi 獲取指定國家的 Google Trends Trending Now 數據
        (維持原邏輯，僅微調註解)
        """
        max_retries = 3
        retry_delay = 30 
        api_key = serpapi_key()

        for attempt in range(max_retries + 1):
            try:
                params = {
                    "engine": "google_trends_trending_now",
                    "geo": geo,
                    "api_key": api_key,
                    "hours": 168
                }

                search = GoogleSearch(params)
                results = search.get_dict()

                if "error" in results:
                    raise Exception(results['error'])
                
                trending_searches = results.get("trending_searches", [])

                trending_list = []
                for row in trending_searches:
                    keyword = row.get("query")
                    freq = row.get("search_volume") or 0
                    started_ts = row.get("start_timestamp")
                    
                    data = {
                        "keyword": keyword,
                        "frequency": freq,
                        "started": started_ts,
                        "geo": geo # 這裡回傳 string，merge 時會轉 list
                    }
                    trending_list.append(data)
                    
                print(f'Fetch {geo} Trending Query Success')
                return trending_list

            except Exception as e:
                error_msg = str(e)
                print(f"Error fetching {geo} (Attempt {attempt + 1}/{max_retries + 1}): {error_msg}")

                if attempt < max_retries:
                    print(f"Waiting {retry_delay} seconds before retrying...")
                    time.sleep(retry_delay)
                else:
                    raise SerpApiError(f"All {max_retries + 1} attempts failed for {geo}")