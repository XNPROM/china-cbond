import os
import sys


sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

import duckdb
import pytest

from fetch_cb_universe import _delete_universe_orphans, _recovery_candidates


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
    con.executemany(
        "INSERT INTO universe VALUES (?)",
        [("111012.SH",), ("113575.SH",), ("110815.SH",)],
    )

    deleted = _delete_universe_orphans(
        con, ["111012.SH", "113575.SH", "113575.SH"]
    )

    assert deleted == ["110815.SH"]
    assert con.execute("SELECT code FROM universe ORDER BY code").fetchall() == [
        ("111012.SH",),
        ("113575.SH",),
    ]


def test_delete_universe_orphans_rejects_empty_snapshot():
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE universe (code TEXT PRIMARY KEY)")

    with pytest.raises(ValueError, match="empty active code set"):
        _delete_universe_orphans(con, [])
