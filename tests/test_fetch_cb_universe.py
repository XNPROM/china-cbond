import os
import sys


sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

import duckdb
import pytest

from fetch_cb_universe import _delete_universe_orphans, _recovery_candidates


def test_historical_universe_excludes_only_future_listings():
    from fetch_cb_universe import _filter_unlisted_bonds
    bonds = [{'code': '113702.SH', 'listed': '2026/05/11'},
             {'code': '123267.SZ', 'listed': '2026-04-22'},
             {'code': '110075.SH', 'listed': ''}]
    active, excluded = _filter_unlisted_bonds(bonds, '20260422')
    assert [b['code'] for b in active] == ['123267.SZ', '110075.SH']
    assert excluded == [{'code': '113702.SH', 'name': '', 'listed': '20260511',
                         'reason': 'not_yet_listed'}]


def test_invalid_listing_date_fails_instead_of_silently_dropping_bond():
    from fetch_cb_universe import _filter_unlisted_bonds
    with pytest.raises(RuntimeError, match='invalid listing date'):
        _filter_unlisted_bonds([{'code': '113702.SH', 'listed': 'bad'}], '20260422')


def test_stop_date_filters_on_first_closed_day_not_last_trading_day():
    from fetch_cb_universe import _filter_stopped_bonds
    bonds = [{'code': '123258.SZ', 'redemp_stop_date': '2026/09/30'},
             {'code': '110075.SH', 'redemp_stop_date': ''}]
    assert len(_filter_stopped_bonds(bonds, '20260929')[0]) == 2
    active, excluded = _filter_stopped_bonds(bonds, '20260930')
    assert [b['code'] for b in active] == ['110075.SH']
    assert excluded[0]['code'] == '123258.SZ'


def test_stop_date_check_keeps_null_and_excludes_confirmed_stop(monkeypatch):
    import fetch_cb_universe as module
    indicator = 'ths_redemp_stop_trading_date_bond'
    def response(codes, fields):
        assert fields == [{'indicator': indicator, 'indiparams': ['']}]
        return {'errorcode': 0, 'tables': [
            {'thscode': '123258.SZ', 'table': {indicator: ['2026-09-30']}},
            {'thscode': '110075.SH', 'table': {indicator: [None]}}]}
    monkeypatch.setattr(module, 'basic_data', response)
    bonds = [{'code': '123258.SZ'}, {'code': '110075.SH'}]
    module._attach_stop_dates(bonds)
    assert [b['code'] for b in module._filter_stopped_bonds(bonds, max('20260930', bonds[0]['lifecycle_known_on'].replace('-', '')))[0]] == ['110075.SH']


@pytest.mark.parametrize('tables', [[], [{'thscode': '110075.SH', 'table': {}}],
    [{'thscode': '110075.SH', 'table': {'ths_redemp_stop_trading_date_bond': ['invalid']}}]])
def test_incomplete_or_invalid_stop_date_check_cannot_shrink_universe(monkeypatch, tables):
    import fetch_cb_universe as module
    monkeypatch.setattr(module, 'basic_data', lambda *args: {'errorcode': 0, 'tables': tables})
    with pytest.raises(RuntimeError):
        module._attach_stop_dates([{'code': '110075.SH'}])


def test_recovery_candidates_only_include_date_eligible_omissions():
    rows = [
        ("111012.SH", "福新转债", "605488.SH", "福莱新材", "20230207", "20290103"),
        ("113575.SH", "东时转债", "603377.SH", "ST东时", "20200430", "20260408"),
        ("118999.SH", "未来转债", "688999.SH", "未来股份", "20260711", "20320710"),
        ("110073.SH", "国投转债", "600886.SH", "国投电力", "20200820", "20260724"),
        ("110815.SH", "九丰定01", "605090.SH", "九丰能源", "20230101", "20290101"),
    ]

    result = _recovery_candidates(rows, {"110073.SH"}, "20260710")

    assert [row["code"] for row in result] == ["111012.SH"]


def test_delete_universe_orphans_keeps_exact_active_snapshot():
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE universe (code TEXT PRIMARY KEY)")
    active_codes = [f"11{i:04d}.SH" for i in range(10)]
    con.executemany(
        "INSERT INTO universe VALUES (?)",
        [(code,) for code in active_codes] + [("orphan",)],
    )

    deleted = _delete_universe_orphans(con, active_codes + [active_codes[-1]])

    assert deleted == ["orphan"]
    assert [row[0] for row in con.execute(
        "SELECT code FROM universe ORDER BY code"
    ).fetchall()] == active_codes


def test_delete_universe_orphans_skips_partial_snapshot():
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE universe (code TEXT PRIMARY KEY)")
    existing_codes = [f"11{i:04d}.SH" for i in range(10)]
    con.executemany(
        "INSERT INTO universe VALUES (?)",
        [(code,) for code in existing_codes],
    )

    deleted = _delete_universe_orphans(con, existing_codes[:8])

    assert deleted is None
    assert con.execute("SELECT count(*) FROM universe").fetchone()[0] == 10


def test_delete_universe_orphans_rejects_empty_snapshot():
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE universe (code TEXT PRIMARY KEY)")

    with pytest.raises(ValueError, match="empty active code set"):
        _delete_universe_orphans(con, [])
