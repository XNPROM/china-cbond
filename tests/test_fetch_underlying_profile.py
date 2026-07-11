import os
import sys


sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from fetch_underlying_profile import select_refresh_codes


def test_select_refresh_codes_reuses_recent_and_refreshes_missing_or_expired():
    existing = {
        "000001.SZ": {"main_business": "recent", "updated_at": "2026-06-20T10:00:00"},
        "000002.SZ": {"main_business": "expired", "updated_at": "2026-05-01T10:00:00"},
        "000003.SZ": {"main_business": "", "updated_at": "2026-06-30T10:00:00"},
    }

    codes, missing, expired = select_refresh_codes(
        ["000001.SZ", "000002.SZ", "000003.SZ", "000004.SZ"],
        existing,
        "2026-07-01",
        30,
    )

    assert codes == ["000003.SZ", "000004.SZ", "000002.SZ"]
    assert missing == 2
    assert expired == 1


def test_select_refresh_codes_refreshes_invalid_timestamp():
    codes, missing, expired = select_refresh_codes(
        ["000001.SZ"],
        {"000001.SZ": {"main_business": "cached", "updated_at": "invalid"}},
        "2026-07-01",
        30,
    )

    assert codes == ["000001.SZ"]
    assert missing == 0
    assert expired == 1


def test_select_refresh_codes_accepts_database_profile_alias():
    codes, missing, expired = select_refresh_codes(
        ["000001.SZ"],
        {"000001.SZ": {"profile": "cached", "updated_at": "2026-06-20T10:00:00"}},
        "2026-07-01",
        30,
    )

    assert codes == []
    assert missing == 0
    assert expired == 0
