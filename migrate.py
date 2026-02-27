import pandas as pd
from sqlalchemy import create_engine
import time

# --- 設定區 ---

# 1. 來源：ws2 (沒有 SSL)
SOURCE_URL = "postgresql+psycopg2://metric:metric@172.16.191.1:5433/metricdb"

# 2. 目的：Neon (有 SSL)
# 注意：我移除了 channel_binding=require 以增加相容性，保留 sslmode=require
DEST_URL = "postgresql+psycopg2://neondb_owner:npg_5mJhDMH9LTqB@ep-hidden-morning-a1kzhlt5-pooler.ap-southeast-1.aws.neon.tech/neondb?sslmode=require"

# 3. 要複製的資料表清單 (共 9 張)
TABLES_TO_COPY = [
    "crawler_stat_a",
    "crawler_stat_b",
    "crawler_stat_total",
    "metric_headset_a",
    "metric_headset_b",
    "metric_headset_total",
    "metric_randomset_a",
    "metric_randomset_b",
    "metric_randomset_total"
]

def migrate_data():
    print("--- 開始資料遷移工作 ---")
    
    # 建立連線引擎
    try:
        source_engine = create_engine(SOURCE_URL)
        dest_engine = create_engine(DEST_URL)
    except Exception as e:
        print(f"連線設定錯誤: {e}")
        return

    for table_name in TABLES_TO_COPY:
        print(f"\n正在處理資料表: [{table_name}] ...")
        start_time = time.time()
        
        try:
            # 1. 從 ws2 讀取資料
            # 使用 chunksize 分批讀取，避免記憶體爆掉 (如果資料量很大的話)
            # 這裡簡單起見直接讀取，若資料超過 100萬筆建議改用 chunk 讀寫
            df = pd.read_sql(f"SELECT * FROM public.{table_name}", source_engine)
            row_count = len(df)
            print(f"  -> 已讀取 {row_count} 筆資料")

            if row_count > 0:
                # 2. 寫入 Neon
                # if_exists='replace': 如果表存在就刪除重建 (確保欄位正確)
                # index=False: 不要在資料庫中建立 DataFrame 的 index 欄位
                df.to_sql(table_name, dest_engine, if_exists='replace', index=False, chunksize=1000)
                print(f"  -> 寫入成功！")
            else:
                print("  -> 資料表是空的，跳過寫入。")

        except Exception as e:
            print(f"  [錯誤] 處理 {table_name} 時發生問題: {e}")
            # 發生錯誤時不中斷，繼續處理下一張表

        end_time = time.time()
        print(f"  耗時: {end_time - start_time:.2f} 秒")

    print("\n--- 全部完成！現在去 Power BI 連線吧 ---")

if __name__ == "__main__":
    migrate_data()
