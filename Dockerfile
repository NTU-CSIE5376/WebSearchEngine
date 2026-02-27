# 1. 使用 Ubuntu 作為基底
FROM ubuntu:24.04

# 避免安裝過程中的互動式提問
ENV DEBIAN_FRONTEND=noninteractive

# 2. 執行 apt update, apt upgrade 並安裝必要套件
RUN apt update && apt upgrade -y && \
    apt install -y git python3 python3-pip python3-venv cron vim && \
    rm -rf /var/lib/apt/lists/*

# 設定家目錄路徑
WORKDIR /root

# 3. Copy 專案
COPY . /root/WebSearchEngine

# 4. 建立虛擬環境並安裝依賴
RUN python3 -m venv /root/system-venv
# 使用虛擬環境內的 pip 進行安裝 (對應您提供的路徑)
RUN /root/system-venv/bin/pip install --upgrade pip && \
    /root/system-venv/bin/pip install -r /root/WebSearchEngine/Metric/requirments.txt

# 5. 設定環境變數
# 您可以在編譯時指定，或是在執行時透過 -e 傳入
ENV SERPAPI_KEY="YOUR_ACTUAL_API_KEY_HERE"

# 6. 設定 Crontab
# 注意：cron 預設環境乾淨，需要手動指定路徑與環境變數
RUN echo "SERPAPI_KEY=$SERPAPI_KEY" >> /etc/environment

RUN echo "\
# 每小時執行一次 (Status) \n\
0 * * * * cd /root/WebSearchEngine && /root/system-venv/bin/python3 measure.py --test --metric_db_url 172.16.191.1:5433 --crawler_db_url 172.16.191.1:5432 --measure status >> /var/log/cron.log 2>&1 \n\
0 * * * * cd /root/WebSearchEngine && /root/system-venv/bin/python3 migrate.py >> /var/log/cron.log 2>&1 \n\
# 每月 1 號與 16 號 中午 12 點 (Strategy: random) \n\
0 12 1,16 * * cd /root/WebSearchEngine && /root/system-venv/bin/python3 measure.py --create --metric_db_url 172.16.191.1:5433 --crawler_db_url 172.16.191.1:5432 --strategy random --keywordNums 50 --test --measure crawler_all >> /var/log/cron.log 2>&1 \n\
# 每月 1 號與 16 號 下午 6 點 (Strategy: head) \n\
0 18 1,16 * * cd /root/WebSearchEngine && /root/system-venv/bin/python3 measure.py --create --metric_db_url 172.16.191.1:5433 --crawler_db_url 172.16.191.1:5432 --strategy head --keywordNums 50 --test --measure crawler_all >> /var/log/cron.log 2>&1 \n\
" > /etc/cron.d/search-engine-cron

# 賦予 crontab 檔案權限並套用
RUN chmod 0644 /etc/cron.d/search-engine-cron && crontab /etc/cron.d/search-engine-cron

# 建立日誌檔案以便查看
RUN touch /var/log/cron.log

# 啟動 cron 並持續輸出日誌，防止容器停止
CMD cron && tail -f /var/log/cron.log
