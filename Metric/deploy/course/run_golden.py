"""Run the three December 2026 course batches using the installed Metric code.

Written by the course TA on Host1 (2026-11-23). Its state (`<date>.json`, `run.lock`)
lives in GOLDEN_COURSE_STATE, a host directory, so it survives a container replacement.
`--preview` prints the schedule and spends no search.
"""
import argparse
from datetime import date, datetime, time, timedelta
import fcntl
import json
import os
from pathlib import Path
import random
import sys
from urllib.parse import urlencode
from urllib.request import urlopen
from zoneinfo import ZoneInfo

TAIPEI = ZoneInfo('Asia/Taipei')
STATE = Path(os.environ.get('GOLDEN_COURSE_STATE', Path(__file__).resolve().parent))
APP = Path('/root/WebSearchEngine')
DATES = {
    date(2026, 12, 1): [('seed', time(8, 5), 10), ('random', time(9), 1000), ('head', time(15), 1000)],
    date(2026, 12, 8): [('seed', time(8, 5), 10), ('head', time(9), 950)],
    date(2026, 12, 15): [('seed', time(8, 5), 10), ('random', time(9), 1000), ('head', time(15), 1000)],
}


def scheduled(now, state):
    for phase, start, target in DATES.get(now.date(), ()):
        if state.get(phase) == 'done':
            continue
        if state.get(phase) is not None or now.time() < start:
            return None
        return phase, target
    return None


def reserve_after(day, phase):
    # Future complete batches take precedence over the intermediate Head set.
    if day.day == 8:
        return 2010 + (1 if phase == 'seed' else 0)
    after = [p for p, _, _ in DATES[day]].index(phase) + 1
    same_day = sum(target for _, _, target in DATES[day][after:])
    return same_day + (2010 if day.day == 1 else 0)


def load_container_key():
    # Debian cron can overwrite the Docker runtime key via pam_env.
    for entry in Path('/proc/1/environ').read_bytes().split(b'\0'):
        name, _, value = entry.partition(b'=')
        if name == b'SERPAPI_KEY' and value:
            os.environ['SERPAPI_KEY'] = value.decode()
            return
    raise RuntimeError('Container SerpApi key is unavailable')


def quota():
    load_container_key()
    params = urlencode({'api_key': os.environ['SERPAPI_KEY']})
    with urlopen('https://serpapi.com/account.json?' + params, timeout=10) as response:
        data = json.load(response)
    if data.get('account_status') != 'Active':
        raise RuntimeError('SerpApi account not active')
    left = int(data['total_searches_left'])
    hourly = int(data['account_rate_limit_per_hour']) - int(data['this_hour_searches'])
    return left, max(0, hourly)


def allowed_target(day, phase, target, left, hourly):
    available = left - reserve_after(day, phase)
    if day.day == 8 and phase == 'head':
        # Allow the intermediate set to shrink if needed to protect Dec 15.
        target = min(target, available)
    if target <= 0 or min(available, hourly) < target:
        return None
    return target


def save_state(path, state):
    temp = path.with_suffix('.tmp')
    with temp.open('w') as f:
        json.dump(state, f, sort_keys=True)
        f.write('\n')
        f.flush()
        os.fsync(f.fileno())
    temp.replace(path)


def execute(phase, target, state, state_path, now):
    sys.path.insert(0, str(APP))
    from Database.CrawlerModels import Base as CrawlerBase
    from Database.MetricModels import Base as MetricBase
    from Database.ModelFactory.AppModelFactory import AppModelFactory
    from Database.utils import createDB
    from Metric.Query.HeadQueryStrategy import HeadQueryStrategy
    from Metric.Query.RandomQueryStrategy import RandomQueryStrategy
    from Metric.RawDataReader.DatabaseRawDataReader import DatabaseRawDataReader
    from sqlalchemy import distinct, func, select

    db = createDB('metric', 'metric', '172.16.191.1:5433', 'metricdb')
    factory = AppModelFactory(CrawlerBase, MetricBase)
    reader = DatabaseRawDataReader(db, factory)
    batch = factory.create_metric_batches()
    query = factory.create_metric_queries()
    url = factory.create_metric_url()

    if phase == 'seed':
        with db.session() as session:
            old_id = session.scalar(select(func.max(batch.id))) or 0
        keywords = reader._fetch_from_api_process_and_store()
        with db.session() as session:
            ids = session.scalars(select(batch.id).where(batch.id > old_id)).all()
        if len(ids) != 1:
            raise RuntimeError('Expected one newly seeded batch')
        state['batch_id'] = ids[0]
        save_state(state_path, state)
        if len(keywords) < 1000:
            raise RuntimeError(f'Trending seed has only {len(keywords)} keywords')
        return {'batch_id': ids[0], 'keywords': len(keywords)}

    batch_id = state['batch_id']
    keywords = reader._fetch_from_db(batch_id)
    if len(keywords) < target:
        raise RuntimeError(f'Batch {batch_id} has only {len(keywords)} keywords')
    strategy_class = RandomQueryStrategy if phase == 'random' else HeadQueryStrategy

    class ReuseSameBatchResults(strategy_class):
        def getQuery(self, keyword, *args, **kwargs):
            # Existing Metric stores one ordered result list per keyword/batch.
            with db.session() as session:
                existing = session.scalars(
                    select(url.url).join(query, query.id == url.query_id)
                    .where(query.batch_id == batch_id, query.keyword == keyword)
                    .order_by(url.rank)
                ).all()
            return existing or super().getQuery(keyword, *args, **kwargs)

    random.seed(now.date().toordinal())
    ReuseSameBatchResults(db, factory, batch_id, keywords, target).getGoldenSet()
    with db.session() as session:
        tagged = session.scalar(select(func.count(query.id)).where(query.batch_id == batch_id, query.tags.contains([phase])))
        with_urls = session.scalar(
            select(func.count(distinct(query.id))).join(url, url.query_id == query.id)
            .where(query.batch_id == batch_id, query.tags.contains([phase]))
        )
    if tagged != target or with_urls != target:
        raise RuntimeError(f'Batch {batch_id} {phase} incomplete: {tagged} tagged, {with_urls} with results, {target} expected')
    return {'batch_id': batch_id, 'target': target, 'keywords_with_results': with_urls}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--preview', action='store_true')
    parser.add_argument('--check-account', action='store_true')
    args = parser.parse_args()
    if args.preview:
        for day, steps in DATES.items():
            print(day, [(phase, start.strftime('%H:%M'), count) for phase, start, count in steps])
        return
    if args.check_account:
        try:
            left, hourly = quota()
        except Exception as error:
            print(f'Account check failed ({type(error).__name__})', file=sys.stderr)
            sys.exit(1)
        print(json.dumps({'credits_remaining': left, 'current_hour_capacity': hourly}))
        return
    now = datetime.now(TAIPEI)
    if now.date() not in DATES:
        return
    path = STATE / f'{now.date().isoformat()}.json'
    os.umask(0o077)
    with (STATE / 'run.lock').open('a') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return
        state = json.loads(path.read_text()) if path.exists() else {}
        plan = scheduled(now, state)
        if not plan:
            return
        phase, target = plan
        try:
            left, hourly = quota()
        except Exception as error:
            print(f'{now.isoformat()} quota check unavailable ({type(error).__name__})', flush=True)
            return
        allowed = allowed_target(now.date(), phase, target, left, hourly)
        if allowed is None:
            print(f'{now.isoformat()} quota deferred {phase} (credits={left}, current-hour capacity={hourly})', flush=True)
            return
        state[phase] = 'running'
        state[f'{phase}_started_at'] = now.isoformat()
        save_state(path, state)
        try:
            result = execute(phase, allowed, state, path, now)
        except RuntimeError as error:
            # Only print our own result counts; upstream exceptions can include request details.
            state[phase] = 'failed'
            state[f'{phase}_error'] = type(error).__name__
            msg = str(error)
            if msg.startswith(('Batch ', 'Trending seed has only ', 'Expected one newly seeded batch')):
                state[f'{phase}_error'] = msg
            save_state(path, state)
            print(f'{now.isoformat()} {phase} failed: {state[f"{phase}_error"]}', flush=True)
            sys.exit(1)
        except Exception as error:
            state[phase] = 'failed'
            state[f'{phase}_error'] = type(error).__name__
            save_state(path, state)
            print(f'{now.isoformat()} {phase} failed: {type(error).__name__}', flush=True)
            sys.exit(1)
        state[phase] = 'done'
        state[f'{phase}_result'] = result
        save_state(path, state)
        print(json.dumps({'date_taipei': now.date().isoformat(), 'phase': phase, **result}), flush=True)


if __name__ == '__main__':
    main()
