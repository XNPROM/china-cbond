"""Resolve wake/login triggers to the latest completed exchange session offline."""
import argparse
from datetime import date, datetime, time, timedelta
import json
from pathlib import Path
import sys
from zoneinfo import ZoneInfo

SHANGHAI = ZoneInfo('Asia/Shanghai')
READY_AT = time(16, 0)


def is_trading_day(day, calendar):
    year = calendar.get(str(day.year))
    if year is None:
        raise ValueError(f'exchange calendar for {day.year} is unavailable; update scripts/trading_calendar.json')
    if day.weekday() >= 5:
        return False
    return not any(date.fromisoformat(f'{day.year}-{start}') <= day <= date.fromisoformat(f'{day.year}-{end}')
                   for start, end in year['closures'])


def resolve_target(now, calendar, explicit=None):
    now = now.astimezone(SHANGHAI)
    if explicit:
        target = date.fromisoformat(explicit)
        if target > now.date() or (target == now.date() and now.time() < READY_AT):
            raise ValueError('requested session is not ready (requires 16:00 Asia/Shanghai)')
        return target if is_trading_day(target, calendar) else None
    target = now.date()
    if now.time() < READY_AT:
        target -= timedelta(days=1)
    while not is_trading_day(target, calendar):
        target -= timedelta(days=1)
    return target


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--trade-date')
    args = parser.parse_args()
    try:
        calendar = json.loads(Path(__file__).with_name('trading_calendar.json').read_text())
        now = datetime.now(SHANGHAI)
        target = resolve_target(now, calendar, args.trade_date)
    except (ValueError, OSError, KeyError) as exc:
        print(f'[fail] schedule: {exc}', file=sys.stderr)
        return 1
    if target is None:
        print(f'[skip] {args.trade_date} is an exchange closure', file=sys.stderr)
        return 3
    print(f'[schedule] trigger={now.isoformat()} target={target} mode={"manual" if args.trade_date else "automatic"}', file=sys.stderr)
    print(target)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
