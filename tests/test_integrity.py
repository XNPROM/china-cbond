import json
from pathlib import Path
import subprocess
import sys

import duckdb
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'scripts'))
import _db
import bs_pricing
import daily_refresh
import validate_snapshot
from _snapshot_policy import classify_sector
from report_view_model import derive_sector, build_backtest_payload
from strategy_score import _classify_sector
from backtest_weekly import classify_sector as backtest_sector, compute_risk_metrics
from render_markdown_parser import compute_kpi_metrics


def test_new_schema_has_iv_and_sparse_upserts_preserve_values(tmp_path):
    con = duckdb.connect(str(tmp_path / 'new.duckdb'))
    _db.init_schema(con)
    _db.init_schema(con)
    con.execute("INSERT INTO valuation_daily(trade_date,code,price,implied_vol) VALUES ('2026-09-28','110075.SH',100,30), ('2026-09-28','110076.SH',110,40)")
    _db.upsert(con, 'valuation_daily', [
        {'trade_date': '2026-09-28', 'code': '110075.SH', 'price': 101},
        {'trade_date': '2026-09-28', 'code': '110076.SH', 'implied_vol': 41},
    ], ['trade_date', 'code'])
    assert con.execute('SELECT price,implied_vol FROM valuation_daily ORDER BY code').fetchall() == [(101,30),(110,41)]
    con.close()


@pytest.mark.parametrize('value,expected', [(None,'未分类'),(float('nan'),'未分类'),(0.29,'偏债'),(0.3,'平衡'),(0.59,'平衡'),(0.6,'偏股'),(0.66,'偏股')])
def test_sector_consistent_everywhere(value, expected):
    assert {fn(value) for fn in (classify_sector, derive_sector, _classify_sector, backtest_sector)} == {expected}


def test_even_number_median_and_fallback_benchmark_label():
    cards=[{'price':'100','conv':'10%','relative_value':'0.8','delta':'0.35'},
           {'price':'110','conv':'20%','relative_value':'1.2','delta':'0.65'}]
    kpi=compute_kpi_metrics({'sections':[{'cards':cards}]})
    assert kpi['median_conv']==15 and kpi['median_rv']==1.0
    assert kpi['n_balanced']==1 and kpi['n_equity']==1
    assert build_backtest_payload({'benchmark':'equal_weight'})['summary']['benchmark_label']=='全市场等权'


def test_failed_etl_step_logged_once(monkeypatch):
    logs=[]
    monkeypatch.setattr(daily_refresh, '_log_run', lambda *a,**k: logs.append((a,k)))
    monkeypatch.setattr(daily_refresh.subprocess, 'run', lambda *a,**k: subprocess.CompletedProcess([], 1, 'failed', ''))
    with pytest.raises(RuntimeError):
        daily_refresh._run_step('2026-09-28','fetch_valuation',['test'],'.')
    assert len(logs)==1 and logs[0][0][4]=='failed'


def test_invalid_pricing_input_clears_old_estimates(tmp_path, monkeypatch):
    db=str(tmp_path/'db.duckdb')
    con=duckdb.connect(db);_db.init_schema(con)
    con.execute("INSERT INTO valuation_daily(trade_date,code,bs_value,relative_value,bs_delta) VALUES ('2026-09-28','110075.SH',120,0.9,0.8)")
    con.close()
    dataset=tmp_path/'dataset.json'
    dataset.write_text(json.dumps({'items':[{'code':'110075.SH','latest':110,'conv_prem':10,'vol_20d':None,'bs_value':120,'relative_value':0.9,'bs_delta':0.8}]}))
    monkeypatch.setattr(bs_pricing, 'connect', lambda:duckdb.connect(db))
    monkeypatch.setattr(sys,'argv',['bs_pricing','--trade-date','2026-09-28','--dataset',str(dataset)])
    bs_pricing.main()
    assert json.loads(dataset.read_text())['items'][0]['bs_delta'] is None
    con=duckdb.connect(db)
    assert con.execute('SELECT bs_value,relative_value,bs_delta FROM valuation_daily').fetchone()==(None,None,None)
    con.close()


def test_annualized_returns_normalize_starting_equity():
    assert compute_risk_metrics([2,2.2],252)['ann_return']==pytest.approx(0.1)


@pytest.fixture
def valid_snapshot(tmp_path, monkeypatch):
    date='2026-09-28'; codes=['110075.SH','110076.SH']
    db=str(tmp_path/'db.duckdb');con=duckdb.connect(db);_db.init_schema(con)
    items=[]
    for i,code in enumerate(codes):
        con.execute('INSERT INTO universe(code) VALUES (?)',[code])
        row={'trade_date':date,'code':code,'price':100.,'conv_prem_pct':10.,'pure_prem_pct':10.,'pure_bond_value':90.,'maturity_call_price':110.,'change_pct':0.,'implied_vol':30.,'pe_ttm':20.,'total_mv_yi':10.,'relative_value':0.9,'bs_delta':0.6}
        _db.upsert(con,'valuation_daily',[row],['trade_date','code'])
        con.execute("INSERT INTO themes(trade_date,code,business_rewrite) VALUES (?,?,'主营制造')",[date,code])
        con.execute("INSERT INTO strategy_picks(trade_date,code,strategy) VALUES (?,?,'双低')",[date,code])
        items.append({'code':code,'name':'测试转债','latest':100.,'conv_prem':10.,'pure_prem':10.,'pure_bond_value':90.,'maturity_call_price':110.,'profile':'主营制造','vol_20d':30.,'relative_value':0.9})
    con.close()
    (tmp_path/'cbond_codes.txt').write_text('\n'.join(codes))
    (tmp_path/'dataset.json').write_text(json.dumps({'trade_date':date,'count':2,'items':items}))
    (tmp_path/'quote_audit.json').write_text(json.dumps({'trade_date':date,'expected_count':2,'returned_count':2,'missing_count':0,'expected_codes':codes,'returned_codes':codes,'missing_codes':[],'blank_codes':[],'batch_errors':[]}))
    monkeypatch.setattr(validate_snapshot,'connect',lambda **kw:duckdb.connect(db,**kw))
    def validate():
        return validate_snapshot.validate(date,str(tmp_path/'dataset.json'),strict=True,codes_path=str(tmp_path/'cbond_codes.txt'))
    return tmp_path,db,validate


def test_valid_snapshot_passes_without_relying_on_current_universe_count(valid_snapshot):
    _,_,validate=valid_snapshot
    assert validate()==0


@pytest.mark.parametrize('damage',['missing_codes','missing_dataset','missing_audit','audit_error','audit_date','dataset_extra','price_nan','null_extra','failed_etl'])
def test_validation_fails_closed(valid_snapshot, damage):
    root,db,validate=valid_snapshot
    if damage.startswith('missing_'):
        name={'missing_codes':'cbond_codes.txt','missing_dataset':'dataset.json','missing_audit':'quote_audit.json'}[damage]
        (root/name).unlink()
    elif damage.startswith('audit_'):
        p=root/'quote_audit.json';data=json.loads(p.read_text())
        data['batch_errors' if damage=='audit_error' else 'trade_date']=['timeout'] if damage=='audit_error' else '2026-09-27'
        p.write_text(json.dumps(data))
    elif damage=='dataset_extra':
        p=root/'dataset.json';data=json.loads(p.read_text());data['items'][0]['code']='113999.SH';p.write_text(json.dumps(data))
    else:
        con=duckdb.connect(db)
        if damage=='price_nan': con.execute("UPDATE valuation_daily SET price='NaN' WHERE code='110075.SH'")
        elif damage=='null_extra': con.execute("INSERT INTO valuation_daily(trade_date,code) VALUES ('2026-09-28','113999.SH')")
        else: con.execute("INSERT INTO etl_runs(run_id,trade_date,step,started_at,status) VALUES ('bad','2026-09-28','fetch_valuation','2026-09-28T16:00:00Z','failed')")
        con.close()
    assert validate()==1


def test_recovered_step_does_not_erase_history_or_block_current_success(valid_snapshot):
    _,db,validate=valid_snapshot
    con=duckdb.connect(db)
    con.execute("INSERT INTO etl_runs(run_id,trade_date,step,started_at,status) VALUES ('bad','2026-09-28','fetch_valuation','2026-09-28T16:00:00Z','failed'),('good','2026-09-28','fetch_valuation','2026-09-28T17:00:00Z','ok')")
    con.close()
    assert validate()==0


def test_implied_vol_reaches_html_view_model():
    from render_markdown_parser import parse_markdown
    from report_view_model import build_dashboard_view_model
    text='''# 可转债概览 · 2026-09-29
## 测试题材
### 测试转债 (110075.SH)
| 正股 | 价格 | 20日年化σ | 隐含波动率 | Delta |
|---|---|---|---|---|
| 测试 (600000.SH) | 100 | 30% | 42% | 0.66 |
**主营**：测试。
'''
    vm=build_dashboard_view_model(parse_markdown(text),'2026-09-29',None)
    assert vm['explorer']['items'][0]['implied_vol']['value']==42
    assert vm['explorer']['items'][0]['sector']=='偏股'


def test_highlights_follow_their_named_metrics():
    from report_view_model import build_highlights
    items=[]
    for i in range(4):
        items.append({'bond_name':str(i),'bond_code':str(i),'theme_group':'test','sector':'偏股',
                      'relative_value':{'value':0.9+i/10,'text':str(i)},
                      'conv':{'value':4-i,'text':str(i)},
                      'delta':{'value':0.8 if i==1 else 0.4,'text':str(i)},
                      'vol':{'value':50 if i==2 else 30,'text':str(i)}})
    assert [r['bond_code'] for r in build_highlights(items)]==['0','3','1','2']


def test_report_escapes_embedded_script_and_html(tmp_path):
    import os
    inp=tmp_path/'input.md';out=tmp_path/'out.html'
    payload='</script><script>alert(1)</script>'
    inp.write_text('# '+payload+'\n## 摘要\n- '+payload+'\n')
    proc=subprocess.run([sys.executable,str(ROOT/'scripts/render_html.py'),'--in',str(inp),'--out',str(out),'--title',payload],capture_output=True,text=True)
    assert proc.returncode==0,proc.stderr
    html=out.read_text()
    assert payload not in html
    assert '\\u003c/script>' in html


def test_backtest_includes_first_holding_period_and_real_calendar():
    from types import SimpleNamespace
    from backtest_weekly import print_summary
    args=SimpleNamespace(rebalance='weekly',slippage_bps=10,commission_bps=2,top=10)
    info=print_summary(args,[{'date':'20260904','start_date':'20260901','cum_dl':0.1}],
                       {'dl':[1,1.1],'bench':[1,1.05]}, {'dl':[1]},
                       ['20260901','20260904'],5,['dl'],{'dl':'双低','bench':'指数'},False,
                       calendar_dates=['20260901','20260902','20260903','20260904'])
    assert info['actual_start']=='20260901'
    assert info['n_actual_days']==3 and info['observed_trading_days']==1
    assert info['calendar_basis']=='index'


def test_offline_backtest_does_not_call_index_api(monkeypatch):
    import backtest_weekly
    monkeypatch.setattr(backtest_weekly,'history',lambda *a,**k:pytest.fail('unexpected API call'))
    # A future date also ensures an existing cache cannot claim full coverage.
    _,_,fallback=backtest_weekly.load_benchmark('20990101','20990102',['20990102'],allow_fetch=False)
    assert fallback


def test_markdown_preserves_delta_sector_boundaries(valid_snapshot, monkeypatch):
    import build_overview_md
    from render_markdown_parser import parse_markdown
    from report_view_model import build_dashboard_view_model
    root,db,_=valid_snapshot
    conn=duckdb.connect(db)
    conn.execute('UPDATE strategy_picks SET rank_overall=1')
    conn.close()
    path=root/'dataset.json';data=json.loads(path.read_text())
    for item,delta in zip(data['items'],[0.5998,0.2999]):
        item.update(bs_delta=delta,ucode='600000.SH',uname='测试股份')
    path.write_text(json.dumps(data))
    monkeypatch.setattr(build_overview_md,'connect',lambda:duckdb.connect(db))
    output=root/'report.md'
    monkeypatch.setattr(sys,'argv',['build','--dataset',str(path),'--trade-date','2026-09-28',
                                  '--title-date','2026-09-28','--out',str(output)])
    build_overview_md.main()
    vm=build_dashboard_view_model(parse_markdown(output.read_text()),'2026-09-28',None)
    assert {i['bond_code']:i['sector'] for i in vm['explorer']['items']}=={'110075.SH':'平衡','110076.SH':'偏债'}
