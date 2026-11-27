#!/bin/sh
# Checks the schedule the container will run, without spending a search:
#   docker compose run --rm metric sh /root/WebSearchEngine/Metric/deploy/check-cron.sh
set -eu
name=${METRIC_CRONTAB:-crontab}
file=/etc/supercronic/$name
supercronic -test "$file"
jobs=$(grep -v '^[[:space:]]*#' "$file" | grep -v '^[[:space:]]*$')
creates=$(printf '%s\n' "$jobs" | grep -c -- '--create' || true)
runners=$(printf '%s\n' "$jobs" | grep -c 'run_golden.py' || true)
measures=$(printf '%s\n' "$jobs" | grep -c 'measure_golden.py' || true)
echo "$name: $creates measure.py --create jobs, $runners course runner jobs, $measures course measure jobs"
case $name in
  crontab)
    [ "$creates" -eq 2 ] && [ "$runners" -eq 0 ] && [ "$measures" -eq 0 ] || { echo "FAIL: expected 2 default golden jobs"; exit 1; } ;;
  crontab.course-*)
    [ "$creates" -eq 0 ] && [ "$runners" -eq 1 ] && [ "$measures" -eq 1 ] ||
      { echo "FAIL: expected only the course runner and its measure job"; exit 1; }
    state=${GOLDEN_COURSE_STATE:?the course runner needs GOLDEN_COURSE_STATE}
    [ -d "$state" ] && [ -w "$state" ] || { echo "FAIL: $state is not a writable directory"; exit 1; }
    ls "$state"
    expected="2026-12-01 [('seed', '08:05', 10), ('random', '09:00', 1000), ('head', '15:00', 1000)]
2026-12-08 [('seed', '08:05', 10), ('head', '09:00', 950)]
2026-12-15 [('seed', '08:05', 10), ('random', '09:00', 1000), ('head', '15:00', 1000)]"
    got=$(/root/system-venv/bin/python3 /root/WebSearchEngine/Metric/deploy/course/run_golden.py --preview)
    printf '%s\n' "$got"
    [ "$got" = "$expected" ] || { echo "FAIL: the runner's schedule differs"; exit 1; }
    /root/system-venv/bin/python3 /root/WebSearchEngine/Metric/deploy/course/measure_golden.py --help >/dev/null ||
      { echo "FAIL: the measure job does not start"; exit 1; } ;;
  *)
    echo "FAIL: unknown crontab $name"; exit 1 ;;
esac
echo OK
