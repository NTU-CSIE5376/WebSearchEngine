from Metric.Measure.Measure import Measure
from Database.Database import Database
from Database.ModelFactory.AppModelFactory import AppModelFactory
from sqlalchemy import select, func, and_
from sqlalchemy.dialects.postgresql import insert
from tqdm import tqdm
import tldextract
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

class CrawlerAllMetricMeasure(Measure):
    def __init__(self, modelFactory: AppModelFactory, crawlerDB: Database, metricDB: Database, batch_id: int, tag: str):
        """
        初始化 CrawlerAllMetricMeasure
        :param modelFactory: 模型工廠
        :param crawlerDB: 爬蟲資料庫 (讀取 Shard 狀態)
        :param metricDB: 指標資料庫 (讀取 Golden URL / 寫入覆蓋率)
        :param batch_id: 指定要評估的 MetricBatch ID
        :param tag: 指定 Metric 標籤 (例如 'head', 'random')，用來篩選 Golden URLs
        """
        super().__init__()
        self.modelFactory = modelFactory
        self.crawlerDB: Database = crawlerDB
        self.metricDB: Database = metricDB
        self.batch_id = batch_id
        self.tag = tag
        
        # 初始化 TLD Extractor (關閉快取避免權限問題)
        self.extractor = tldextract.TLDExtract(cache_dir=False)
    
    def get_domain(self, url: str) -> str:
        try:
            extracted = self.extractor(url)
            if extracted.domain and extracted.suffix:
                return f"{extracted.domain}.{extracted.suffix}"
            return ""
        except:
            return ""

    def _scan_domain_shard(self, domain_tuple):
        """
        掃描 CrawlerDB 的 Domain Tables，建立 Domain -> Shard ID 的映射
        """
        if not domain_tuple:
            return {}

        found_map = {}
        with self.crawlerDB.session() as session:
            try:
                DomainState = self.modelFactory.create_domain_state_model()
                stmt = (
                    select(DomainState.domain, DomainState.shard_id)
                    .where(DomainState.domain.in_(domain_tuple))
                )

                for domain, shard_id in session.execute(stmt).all():
                    found_map[domain] = shard_id

            except Exception:
                print("[Error] scan domain_state failed")

        return found_map

    def _scan_url_shard(self, shard_ids, url_tuple):
        """
        掃描 CrawlerDB 的 Url State Tables，取得 URL 的發現/爬取/索引狀態
        回傳: List[(url: str, crawled: bool, indexed: bool, shard_id: int)]

        """
        found_data = []
        
        with self.crawlerDB.session() as session:
            for i in shard_ids:
                try:
                    UrlState = self.modelFactory.create_url_state_current_model(i)
                    # 判斷 URL 是否存在於該分片
                    stmt = select(
                        UrlState.url,
                        UrlState.last_fetch_ok.is_not(None).label("crawled")
                    ).where(UrlState.url.in_(url_tuple))

                    for url, crawled in session.execute(stmt).all():
                        found_data.append((url, bool(crawled), False, i))
                except Exception:
                    print("[Error] scan url_state_current failed")
        return found_data

    def test(self):
        """
        主執行邏輯 (使用 __init__ 傳入的 batch_id 和 tag)
        """
        print(f'🚀 Start Measuring Crawler Coverage (Batch: {self.batch_id}, Tag: {self.tag})')
        today_date = datetime.now().date()
        
        # 對應 MetricCoverage 的 Set Type (例如 "head" -> "HeadSet")
        set_type = f"{self.tag.capitalize()}Set"

        # ==========================================
        # 1. 從 MetricDB 讀取 Golden URLs
        # ==========================================
        MetricURL = self.modelFactory.create_metric_url()
        MetricQuery = self.modelFactory.create_metric_queries()
        
        # 結構: url_str -> list of MetricURL ID
        url_id_map = {} 
        all_golden_domains = set()
        
        print(f"📥 Loading Golden URLs with tag '{self.tag}'...")
        with self.metricDB.session() as session:
            # 透過 Join 篩選：
            # 1. MetricQuery.batch_id 符合
            # 2. MetricQuery.tags 包含指定的 tag (使用 JSONB @> 操作符)
            stmt = select(MetricURL)\
                .join(MetricQuery)\
                .where(
                    and_(
                        MetricQuery.batch_id == self.batch_id,
                        MetricQuery.tags.contains([self.tag])
                    )
                )
            
            
            results = session.execute(stmt).scalars().all()
            
            if not results:
                print(f"⚠️ No URLs found for Batch {self.batch_id} with tag '{self.tag}'. Exiting.")
                return

            for m_url in results:
                url_str = m_url.url
                
                if url_str not in url_id_map:
                    url_id_map[url_str] = []
                
                url_id_map[url_str].append(m_url.id)

                domain = self.get_domain(url_str)
                if domain:
                    all_golden_domains.add(domain)

        # 準備掃描用的 Tuple
        url_list = list(url_id_map.keys())
        domain_list = list(all_golden_domains)
        url_tuple = tuple(url_list)
        domain_tuple = tuple(domain_list)
        
        print(f"   Loaded {len(url_list)} unique URLs from {len(domain_list)} domains.")

        # ==========================================
        # 2. 並行掃描 CrawlerDB (Shards)
        # ==========================================
        MAX_WORKERS = 16
        chunk_size = 256 // MAX_WORKERS + 1
        shard_chunks = [range(i, min(i + chunk_size, 256)) for i in range(0, 256, chunk_size)]

        # A. 掃描 Domain Tables (為了確定 Shard ID / Team)
        print(f"🔍 Scanning Domain Tables ...")
        domain_shard_map = self._scan_domain_shard(domain_tuple)

        # B. 掃描 URL Tables (為了確定 Status)
        # 初始化 URL 狀態
        url_status_map = {
            u: {'discovered': False, 'crawled': False, 'indexed': False, 'shard_id': -1} 
            for u in url_list
        }
        
        print(f"🔍 Scanning URL Tables ({MAX_WORKERS} threads)...")
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(self._scan_url_shard, chunk, url_tuple) for chunk in shard_chunks]
            for future in tqdm(as_completed(futures), total=len(futures), desc="URLs"):
                results = future.result()
                for (url, crawled, indexed, shard_id) in results:
                    if url in url_status_map:
                        url_status_map[url]['discovered'] = True
                        url_status_map[url]['crawled'] = crawled
                        url_status_map[url]['indexed'] = indexed
                        url_status_map[url]['shard_id'] = shard_id

        # ==========================================
        # 3. 聚合統計與分組 (Team A / Team B)
        # ==========================================
        print("🔄 Aggregating Stats...")
        
        # 統計容器：分別統計 Total, Team A, Team B
        stats = {
            "Total": {"total": 0, "disc": 0, "crawl": 0, "idx": 0},
            "A":     {"total": 0, "disc": 0, "crawl": 0, "idx": 0},
            "B":     {"total": 0, "disc": 0, "crawl": 0, "idx": 0},
        }
        
        bulk_update_mappings = []

        for url_str, ids in url_id_map.items():
            status = url_status_map.get(url_str, {})
            
            is_disc = status.get('discovered', False)
            is_crawl = status.get('crawled', False)
            is_idx = status.get('indexed', False)
            shard_id = status.get('shard_id', -1)
            
            # 如果 URL table 沒找到，嘗試用 Domain table 找 Team
            if shard_id == -1:
                domain = self.get_domain(url_str)
                shard_id = domain_shard_map.get(domain, -1)

            # 判斷 Team
            team_key = None
            if 0 <= shard_id <= 127:
                team_key = "A"
            elif 128 <= shard_id <= 255:
                team_key = "B"
            
            # 準備批量更新 MetricURL 的資料
            for m_id in ids:
                bulk_update_mappings.append({
                    "id": m_id,
                    "is_discovered": is_disc,
                    "is_crawled": is_crawl,
                    "is_indexed": is_idx,
                    "shard_id": shard_id # 記錄找到的 shard，-1 表示未找到
                })

            # 累加統計 (Team A/B 與 Total)
            target_groups = ["Total"]
            if team_key:
                target_groups.append(team_key)
            
            for g in target_groups:
                stats[g]["total"] += 1
                if is_disc: stats[g]["disc"] += 1
                if is_crawl: stats[g]["crawl"] += 1
                if is_idx:  stats[g]["idx"] += 1

        # ==========================================
        # 4. 寫入 MetricDB
        # ==========================================
        print(f"💾 Saving results to MetricDB...")
        
        with self.metricDB.session() as session:
            # A. 更新 MetricURL 詳細狀態
            if bulk_update_mappings:
                session.bulk_update_mappings(MetricURL, bulk_update_mappings)
            
            # B. 寫入 MetricCoverage 統計表 (Total, A, B)
            suffixes = ["Total", "A", "B"]
            
            for suffix in suffixes:
                d = stats[suffix]
                total_count = d["total"]
                
                try:
                    ModelClass = self.modelFactory.create_metric_coverage_model(set_type, suffix)
                except Exception as e:
                    print(f"   [Warning] Could not create model for {set_type}_{suffix}: {e}")
                    continue

                row_data = {
                    "stat_date": today_date,
                    "total": total_count,
                    "discovered_num": d["disc"],
                    "discovered_rate": d["disc"] / total_count if total_count > 0 else 0.0,
                    "crawled_num": d["crawl"],
                    "crawled_rate": d["crawl"] / total_count if total_count > 0 else 0.0,
                    "indexed_num": d["idx"],
                    "indexed_rate": d["idx"] / total_count if total_count > 0 else 0.0,
                    "ranked_num": 0,
                    "ranked_rate": 0.0
                }

                stmt = insert(ModelClass).values(row_data)
                stmt = stmt.on_conflict_do_update(
                    index_elements=['stat_date'],
                    set_=row_data
                )
                session.execute(stmt)
            
            session.commit()
            print("✅ MetricDB Updated Successfully.")

        # ==========================================
        # 5. 輸出報告
        # ==========================================
        self._print_report(stats)

    def _print_report(self, stats):
        print("\n" + "="*60)
        print(f"📊 Coverage Report - Tag: {self.tag}")
        print("="*60)
        
        headers = f"{'Group':<12} | {'Total':>8} | {'Disc %':>10} | {'Crawl %':>10} | {'Index %':>10}"
        print(headers)
        print("-" * len(headers))
        
        groups = ["Total", "A", "B"]
        
        for g in groups:
            d = stats[g]
            total = d['total']
            if total == 0:
                print(f"{g:<12} | {0:>8} | {'N/A':>10} | {'N/A':>10} | {'N/A':>10}")
                continue
                
            disc_rate = d['disc'] / total
            crawl_rate = d['crawl'] / total
            idx_rate = d['idx'] / total
            
            print(f"{g:<12} | {total:>8,} | {disc_rate:>9.1%} | {crawl_rate:>9.1%} | {idx_rate:>9.1%}")
        
        print("="*60)
