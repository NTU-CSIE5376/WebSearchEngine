# 1. 使用 Ubuntu 作為基底
FROM ubuntu:24.04

# 避免安裝過程中的互動式提問
ENV DEBIAN_FRONTEND=noninteractive

# 2. 執行 apt update, apt upgrade 並安裝必要套件
RUN apt update && apt upgrade -y && \
    apt install -y git python3 python3-pip python3-venv curl vim && \
    rm -rf /var/lib/apt/lists/*

# 3. supercronic: cron for containers. Jobs inherit the container env (SERPAPI_KEY),
# their output goes to docker logs, and SIGTERM waits for running jobs.
# Releases: https://github.com/aptible/supercronic/releases
ARG SUPERCRONIC_URL=https://github.com/aptible/supercronic/releases/download/v0.2.51/supercronic-linux-amd64
ARG SUPERCRONIC_SHA1SUM=1692066e350a88ff4d7d6474fe3c7a6c41ec5e89
RUN curl -fsSL -o /usr/local/bin/supercronic "$SUPERCRONIC_URL" && \
    echo "${SUPERCRONIC_SHA1SUM}  /usr/local/bin/supercronic" | sha1sum -c - && \
    chmod +x /usr/local/bin/supercronic

# 設定家目錄路徑
WORKDIR /root

# 4. Copy 專案
COPY . /root/WebSearchEngine

# 5. 建立虛擬環境並安裝依賴
RUN python3 -m venv /root/system-venv
# 使用虛擬環境內的 pip 進行安裝 (對應您提供的路徑)
RUN /root/system-venv/bin/pip install --upgrade pip && \
    /root/system-venv/bin/pip install -r /root/WebSearchEngine/Metric/requirments.txt

# 6. 排程: SERPAPI_KEY is not set here. It comes from the env at run time (metric.env in
# docker-compose.yml), so the image holds no secret. A bad crontab fails the build.
# METRIC_CRONTAB picks the schedule: `crontab` (1st and 16th) or a course override.
COPY Metric/deploy/crontab Metric/deploy/crontab.course-202612 /etc/supercronic/
RUN for f in /etc/supercronic/*; do supercronic -test "$f" || exit 1; done

CMD ["sh", "-c", "exec supercronic -split-logs /etc/supercronic/${METRIC_CRONTAB:-crontab}"]
