import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from Metric.Query.SerpApi import SerpApiError, account

try:
    data = account()
except SerpApiError as e:
    sys.exit(str(e))

print("本月已用搜尋次數:", data.get("this_month_usage"))
print("本月剩餘搜尋次數:", data.get("plan_searches_left"))
print("總剩餘搜尋次數:", data.get("total_searches_left"))
