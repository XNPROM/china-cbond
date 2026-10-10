import os
import sys
import json
import pytest


sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from daily_refresh import _latest_universe_snapshot


@pytest.mark.parametrize('policy,checked,expected', [
    (1, '2026-07-13', False),
    (2, '2026-07-12', False),
    (2, '2026-07-13', False),
    (3, '2026-07-13', True),
])
def test_reuse_requires_listing_and_stop_date_policy(tmp_path, policy, checked, expected):
    from daily_refresh import _tradability_verified
    path = tmp_path / 'universe.json'
    path.write_text(json.dumps({'tradability_policy': policy,
                               'tradability_checked_asof': checked}))
    assert _tradability_verified(('2026-07-13', '', str(path))) is expected


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


@pytest.mark.parametrize("rc,produced", [(1, False), (0, False), (1, True), (0, True)])
def test_optional_backtest_never_reuses_old_or_failed_curve(tmp_path, monkeypatch, rc, produced):
    import daily_refresh
    artifact=tmp_path/"backtest.json";artifact.write_text("old")
    overview=tmp_path/"overview.md";overview.write_text("report")
    def run(*args, **kwargs):
        assert not artifact.exists()
        assert kwargs["required"] is False
        if produced:artifact.write_text("new")
        return rc
    monkeypatch.setattr(daily_refresh, "_run_step", run)
    result=daily_refresh._optional_backtest("2026-09-28", ["fixture"], str(tmp_path), str(artifact), str(overview))
    assert result is (rc==0 and produced)
    assert artifact.exists() is result
    assert ("省略收益曲线" in overview.read_text()) is (not result)
