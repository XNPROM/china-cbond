from datetime import datetime
import json
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
from _auto_schedule import resolve_target

CALENDAR = json.loads((Path(__file__).resolve().parents[1] / 'scripts/trading_calendar.json').read_text())


@pytest.mark.parametrize('now,target', [
    ('2026-10-01T01:19:00+08:00', '2026-09-30'),
    ('2026-10-07T18:00:00+08:00', '2026-09-30'),
    ('2026-10-08T15:59:59+08:00', '2026-09-30'),
    ('2026-10-08T16:00:00+08:00', '2026-10-08'),
    ('2026-10-10T17:00:00+08:00', '2026-10-09'),
    ('2026-09-28T08:00:00+08:00', '2026-09-24'),
    ('2026-09-30T08:00:00+00:00', '2026-09-30'),
    ('2026-01-01T17:00:00+08:00', '2025-12-31'),
])
def test_latest_completed_session(now, target):
    assert resolve_target(datetime.fromisoformat(now), CALENDAR).isoformat() == target


def test_unknown_calendar_fails_closed():
    with pytest.raises(ValueError, match='2027'):
        resolve_target(datetime.fromisoformat('2027-02-01T17:00:00+08:00'), CALENDAR)


def test_manual_date_cannot_request_unfinished_session():
    now = datetime.fromisoformat('2026-09-30T12:00:00+08:00')
    with pytest.raises(ValueError, match='not ready'):
        resolve_target(now, CALENDAR, '2026-09-30')
    assert resolve_target(now, CALENDAR, '2026-09-25') is None
