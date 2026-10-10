"""Dated trading eligibility; prices and listing membership are not execution proof."""
from datetime import date, timedelta
from functools import lru_cache
import json
from pathlib import Path
from _auto_schedule import is_trading_day

ROOT = Path(__file__).resolve().parent
POLICY_VERSION = 3
REVIEW_SESSIONS = 20


def iso_date(value):
    if not value or str(value).strip() in ('--', '-', 'None'):
        return None
    value = str(value).strip().replace('/', '-').split('T')[0].split(' ')[0]
    if len(value) == 8 and value.isdigit():
        value = f'{value[:4]}-{value[4:6]}-{value[6:]}'
    return date.fromisoformat(value).isoformat()


@lru_cache(maxsize=1)
def calendar():
    return json.loads((ROOT / 'trading_calendar.json').read_text())


def next_session(value, inclusive=False):
    day = date.fromisoformat(iso_date(value))
    if not inclusive:
        day += timedelta(days=1)
    while not is_trading_day(day, calendar()):
        day += timedelta(days=1)
    return day.isoformat()


def previous_session(value):
    day = date.fromisoformat(iso_date(value)) - timedelta(days=1)
    while not is_trading_day(day, calendar()):
        day -= timedelta(days=1)
    return day.isoformat()


@lru_cache(maxsize=1)
def events():
    return json.loads((ROOT / 'bond_lifecycle_events.json').read_text())['events']


def lifecycle(item, asof):
    """Use only evidence known on asof, even when evaluating a later execution day."""
    asof = iso_date(asof)
    code = item['code']
    facts = {k: item.get(k) for k in ('last_trade_date', 'stop_trading_date',
             'conversion_end_date', 'redemption_date', 'delisting_date',
             'redemption_price', 'redemption_payment_date', 'lifecycle_source', 'lifecycle_known_on')}
    if facts.get('lifecycle_known_on') and facts['lifecycle_known_on'] > asof:
        facts = dict.fromkeys(facts)
    facts['conversion_end_date'] = facts['conversion_end_date'] or item.get('maturity')
    explicit_stop = item.get('redemp_stop_date')
    if (explicit_stop and not facts['stop_trading_date']
            and (not item.get('lifecycle_known_on') or item['lifecycle_known_on'] <= asof)):
        facts.update(stop_trading_date=explicit_stop, lifecycle_source='iFinD snapshot',
                     lifecycle_known_on=asof)
    for event in events():
        if event['code'] == code and event['announced_on'] <= asof:
            facts.update({k: v for k, v in event.items() if k not in ('code', 'announced_on')})
            facts['lifecycle_known_on'] = event['announced_on']
    for key in ('last_trade_date', 'stop_trading_date', 'conversion_end_date',
                'redemption_date', 'redemption_payment_date', 'delisting_date'):
        facts[key] = iso_date(facts.get(key))
    if facts['stop_trading_date'] and not facts['last_trade_date']:
        facts['last_trade_date'] = previous_session(facts['stop_trading_date'])
    facts['estimated_stop_date'] = None
    end = facts['conversion_end_date']
    # The final three exchange sessions are a guard, not a fabricated announcement.
    # Avoid requiring calendars many years ahead for ordinary long-dated bonds.
    if end and -60 <= (date.fromisoformat(end) - date.fromisoformat(asof)).days <= 60:
        last_conversion_session = (end if is_trading_day(date.fromisoformat(end), calendar())
                                   else previous_session(end))
        facts['estimated_stop_date'] = previous_session(previous_session(last_conversion_session))
    return facts


def trade_status(item, day, asof=None):
    day = iso_date(day)
    facts = lifecycle(item, asof or day)
    listed = iso_date(item.get('list_date') or item.get('listed'))
    if listed and day < listed:
        return 'not_yet_listed'
    if facts['delisting_date'] and day >= facts['delisting_date']:
        return 'delisted'
    stop = facts['stop_trading_date']
    if stop and day >= stop:
        return 'stopped_trading'
    last = facts['last_trade_date']
    if last and day > last:
        return 'stopped_trading'
    end = facts['conversion_end_date']
    if end and day > end:
        return 'matured'
    if not stop and facts['estimated_stop_date'] and day >= facts['estimated_stop_date']:
        return 'unverified_stop_date'
    for pause in item.get('trading_suspensions', []):
        if pause['known_on'] <= (asof or day) and pause['start'] <= day and (not pause.get('end') or day <= pause['end']):
            return 'temporarily_suspended'
    return 'trading'


def recommendation_reason(item, asof, execution_date=None):
    execution_date = execution_date or next_session(asof)
    if item.get('quote_date') and iso_date(item['quote_date']) != iso_date(asof):
        return 'stale_quote'
    if item.get('quote_volume') == 0:
        return 'no_execution_liquidity'
    state = trade_status(item, execution_date, asof)
    if state != 'trading':
        return state
    facts = lifecycle(item, asof)
    end = facts['conversion_end_date']
    if not end:
        return 'missing_conversion_end_date'
    if facts['stop_trading_date'] or facts['last_trade_date']:
        return None
    days_to_end = (date.fromisoformat(end) - date.fromisoformat(iso_date(asof))).days
    if days_to_end <= 60:
        day = date.fromisoformat(iso_date(asof))
        remaining = 0
        while day <= date.fromisoformat(end):
            remaining += int(is_trading_day(day, calendar()))
            day += timedelta(days=1)
        if remaining <= REVIEW_SESSIONS:
            return 'unverified_last_trade_date'
    return None


def annotate(item, asof):
    item.update(lifecycle(item, asof))
    item['trade_status'] = trade_status(item, asof)
    item['execution_date'] = next_session(asof)
    item['recommendation_exclusion'] = recommendation_reason(item, asof, item['execution_date'])
    item['recommendation_eligible'] = item['recommendation_exclusion'] is None
    return item


def filter_snapshot(items, asof):
    active, excluded = [], []
    for original in items:
        item = annotate(dict(original), asof)
        if item['trade_status'] == 'trading':
            active.append(item)
        else:
            excluded.append({'code': item['code'], 'name': item.get('name', ''),
                             'reason': item['trade_status'], **lifecycle(item, asof)})
    return active, excluded


@lru_cache(maxsize=256)
def snapshot_metadata(asof, root=None):
    """Historical bond-stock mapping; never fall back to today's universe."""
    root = Path(root or ROOT.parent)
    paths = [p for p in (root/'data/raw').glob('asof=*/cbond_universe.json')
             if len(p.parent.name) == 15 and p.parent.name[5:] <= iso_date(asof)]
    if not paths:
        return {}
    payload = json.loads(max(paths, key=lambda p: p.parent.name).read_text())
    result = {x['code']: dict(x) for x in payload['items']}
    quote_path = max(paths, key=lambda p: p.parent.name).parent / 'quote_audit.json'
    if quote_path.exists():
        details = json.loads(quote_path.read_text()).get('quote_details', {})
        for code, item in result.items():
            if code in details:
                item['quote_date'] = details[code].get('quote_date')
                item['quote_volume'] = details[code].get('volume')
    return result
