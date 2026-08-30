import os
import sys
import json


sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from daily_refresh import _latest_universe_snapshot


def _snapshot(root, date, complete=True):
    path = root / "data" / "raw" / f"asof={date}"
    path.mkdir(parents=True)
    code = "110001.SH"
    (path / "cbond_codes.txt").write_text(code + "\n")
    if complete:
        (path / "cbond_universe.json").write_text(
            json.dumps({"asof": date, "count": 1, "items": [{"code": code}]})
        )


def test_latest_snapshot_uses_newest_complete_date_not_after_trade_date(tmp_path):
    _snapshot(tmp_path, "2026-07-08")
    _snapshot(tmp_path, "2026-07-10")
    _snapshot(tmp_path, "2026-07-14")

    date, codes, universe = _latest_universe_snapshot(str(tmp_path), "2026-07-12")

    assert date == "2026-07-10"
    assert codes.endswith("asof=2026-07-10/cbond_codes.txt")
    assert universe.endswith("asof=2026-07-10/cbond_universe.json")


def test_latest_snapshot_requires_both_files(tmp_path):
    _snapshot(tmp_path, "2026-07-10", complete=False)

    try:
        _latest_universe_snapshot(str(tmp_path), "2026-07-12")
    except FileNotFoundError as exc:
        assert "--refresh-universe" in str(exc)
    else:
        raise AssertionError("expected missing complete snapshot error")
