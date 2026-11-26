"""Measure each December course batch once, right after its last phase.

run_golden.py only creates the batches, and the course crontab has no
`measure.py --measure crawler_all` job, so without this G's coverage rows would not
exist. It runs CrawlerAllMetricMeasure for the day's tags on the runner's batch, both
tags in one run: Head reinserts the rows of keywords it shares with Random, so Random
measured before Head would leave those rows unmeasured.

It holds the runner's `run.lock` and records `measure` in the runner's state file, so a
batch is measured once. A later re-measure rewrites the rows the IndexSelection rank job
snapshots, and the rank job then refuses G. No search is spent.
"""
import argparse
from datetime import datetime
import fcntl
import json
import sys

from run_golden import APP, DATES, STATE, TAIPEI, save_state


def due(day, state):
    """The tags to measure on `day`; None until every phase is done, and once measured."""
    steps = DATES.get(day)
    if not steps or 'batch_id' not in state or state.get('measure') in ('done', 'failed'):
        return None
    if any(state.get(phase) != 'done' for phase, _, _ in steps):
        return None
    return [phase for phase, _, _ in steps if phase != 'seed']


def measure(batch_id, tags, args):
    sys.path.insert(0, str(APP))
    from Database.CrawlerModels import Base as CrawlerBase
    from Database.MetricModels import Base as MetricBase
    from Database.ModelFactory.AppModelFactory import AppModelFactory
    from Database.utils import createDB
    from Metric.Measure.CrawlerAllMetricMeasure import CrawlerAllMetricMeasure

    factory = AppModelFactory(CrawlerBase, MetricBase)
    crawler = createDB('crawler', 'crawler', args.crawler_db_url, 'crawlerdb')
    metric = createDB('metric', 'metric', args.metric_db_url, 'metricdb')
    select = createDB('select', 'select', args.select_db_url, 'selectdb')
    for tag in tags:
        CrawlerAllMetricMeasure(factory, crawler, metric, select, batch_id, tag).test()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--metric_db_url', required=True)
    parser.add_argument('--crawler_db_url', required=True)
    parser.add_argument('--select_db_url', required=True)
    parser.add_argument('--max-attempts', type=int, default=3)
    args = parser.parse_args()
    now = datetime.now(TAIPEI)
    path = STATE / f'{now.date().isoformat()}.json'
    if not path.exists():
        return
    with (STATE / 'run.lock').open('a') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return
        state = json.loads(path.read_text())
        tags = due(now.date(), state)
        if not tags:
            return
        try:
            measure(state['batch_id'], tags, args)
        except Exception as error:
            # Upstream exceptions can include connection details: keep only the type.
            state['measure_attempts'] = state.get('measure_attempts', 0) + 1
            state['measure_error'] = type(error).__name__
            if state['measure_attempts'] >= args.max_attempts:
                state['measure'] = 'failed'
            save_state(path, state)
            print(f'{now.isoformat()} measure failed: {type(error).__name__}', flush=True)
            sys.exit(1)
        state['measure'] = 'done'
        state['measured_at'] = datetime.now(TAIPEI).isoformat()
        state['measured_tags'] = tags
        save_state(path, state)
        print(json.dumps({'date_taipei': now.date().isoformat(), 'measured': tags,
                          'batch_id': state['batch_id']}), flush=True)


if __name__ == '__main__':
    main()
