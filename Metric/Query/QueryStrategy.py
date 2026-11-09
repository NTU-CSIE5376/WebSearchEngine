from serpapi import GoogleSearch
from Metric.Query.GoogleRedirect import resolve_golden_urls
import time
import os

class QueryStrategy:
    def __init__(self, rawData: list, keywordNums: int):
        self.rawData: list = rawData
        self.keywordNums: int = keywordNums

    def getGoldenSet(self):
        pass
    
    def getQuery(self, query, nums=10, max_retries=3, initial_delay=15):
        """
        Args:
            max_retries (int): 最大重試次數
            initial_delay (int): 初始等待秒數 (會隨著重試次數增加)
        """
        for attempt in range(max_retries):
            try:
                params = {
                    "engine": "google",
                    "q": query,
                    "num": nums,
                    "api_key": os.environ.get('SERPAPI_KEY')
                }

                search = GoogleSearch(params)
                results = search.get_dict()
                
                # SerpApi 有時會回傳 200 OK 但內容包含 error 欄位
                if "error" in results:
                    raise Exception(f"SerpApi Error: {results['error']}")

                # 成功取得資料
                urls = [r["link"] for r in results.get("organic_results", []) if "link" in r]
                break

            except Exception as e:
                print(f"[Attempt {attempt + 1}/{max_retries}] Failed: {e}")
                
                # 如果還沒達到最大重試次數，就等待後重試
                if attempt < max_retries - 1:
                    # 指數退避: 2s, 4s, 8s...
                    wait_time = initial_delay * (2 ** attempt) 
                    print(f"Waiting {wait_time} seconds before retrying...")
                    time.sleep(wait_time)
                else:
                    # 最後一次也失敗，回傳空 list
                    print("Max retries reached. Returning empty list.")
                    return []

        # google.com/goto 轉址連結換成實際網址，解析不了的記在 log 後丟掉
        return resolve_golden_urls(urls, query=query)