"""Recovery must use the same units as initial fetch and preserve sparse data."""
from pathlib import Path
import sys

import duckdb
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import refresh_data


@pytest.mark.parametrize("reverse", [False, True])
def test_refresh_converts_iv_units_and_preserves_missing_fields(tmp_path, monkeypatch, reverse):
    path = str(tmp_path / "snapshot.duckdb")
    con = duckdb.connect(path)
    con.execute("CREATE TABLE valuation_daily (trade_date TEXT, code TEXT, implied_vol DOUBLE, pb DOUBLE, PRIMARY KEY(trade_date, code))")
    con.execute("INSERT INTO valuation_daily VALUES ('2026-09-28', '110075.SH', 50, 1), ('2026-09-28', '110076.SH', 20, 2)")
    con.close()
    monkeypatch.setattr(refresh_data, "connect", lambda: duckdb.connect(path))
    monkeypatch.setattr(refresh_data, "_fields_to_fetch", lambda *a, **k: (2, {}, [
        ("implied_vol", "ths_implied_volatility_cbond", ["2026-09-28", "1", "1"]),
        ("pb", "ths_stock_pb_cbond", ["2026-09-28"]),
    ]))
    tables = [
        {"thscode": "110075.SH", "table": {"ths_implied_volatility_cbond": [0.5904], "ths_stock_pb_cbond": [None]}},
        {"thscode": "110076.SH", "table": {"ths_implied_volatility_cbond": [None], "ths_stock_pb_cbond": [3]}},
    ]
    monkeypatch.setattr(refresh_data, "basic_data", lambda *a: {"tables": list(reversed(tables)) if reverse else tables})
    monkeypatch.setattr(refresh_data.time, "sleep", lambda *a: None)
    assert refresh_data.refresh("2026-09-28", force=True) == 2
    con = duckdb.connect(path, read_only=True)
    rows = con.execute("SELECT code, implied_vol, pb FROM valuation_daily ORDER BY code").fetchall()
    con.close()
    assert rows == [("110075.SH", 59.04, 1.0), ("110076.SH", 20.0, 3.0)]


def test_45_percent_iv_gap_requests_only_missing_codes_and_updates_csv(tmp_path, monkeypatch):
    import csv
    db=str(tmp_path/'iv.duckdb')
    con=duckdb.connect(db)
    con.execute('CREATE TABLE valuation_daily(trade_date TEXT,code TEXT,implied_vol DOUBLE,PRIMARY KEY(trade_date,code))')
    codes=[f'110{i:03d}.SH' for i in range(20)]
    con.executemany("INSERT INTO valuation_daily VALUES ('2026-09-28',?,?)",[(c,None if i<9 else 30.) for i,c in enumerate(codes)])
    con.close()
    monkeypatch.setattr(refresh_data,'connect',lambda:duckdb.connect(db))
    monkeypatch.setattr(refresh_data,'ALL_FIELDS',[('implied_vol','ths_implied_volatility_cbond',[None,'1','1'])])
    monkeypatch.setattr(refresh_data.time,'sleep',lambda *a:None)
    seen=[]
    def api(batch, params):
        seen.extend(batch)
        return {'errorcode':0,'tables':[{'thscode':c,'table':{'ths_implied_volatility_cbond':[0.42]}} for c in batch]}
    monkeypatch.setattr(refresh_data,'basic_data',api)
    path=tmp_path/'valuation.csv'
    with path.open('w',newline='') as f:
        w=csv.writer(f);w.writerow(['转债代码','隐含波动率(%)'])
        w.writerows([(c,'' if i<9 else '30.0') for i,c in enumerate(codes)])
    assert refresh_data.refresh('2026-09-28',csv_path=path,audit_path=tmp_path/'audit.json')==9
    assert seen==codes[:9]
    con=duckdb.connect(db)
    assert con.execute('SELECT count(*) FROM valuation_daily WHERE implied_vol=42').fetchone()[0]==9
    con.close()
    with path.open() as f:
        assert [float(r['隐含波动率(%)']) for r in csv.DictReader(f)]==[42.]*9+[30.]*11


@pytest.mark.parametrize('value',[None,'','--','NaN',float('inf')])
def test_bad_numeric_response_cannot_blank_cache(value):
    assert refresh_data._convert('implied_vol',value) is None
