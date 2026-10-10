import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
import bs_pricing
import backtest_weekly as bt
from _lifecycle import annotate, lifecycle, pricing_exclusion, recommendation_reason


def ordinary(**changes):
    return {'code':'118053.SH', 'maturity':'2031-03-18',
            'conversion_end_date':'2031-03-17', 'latest':115.132,
            'conv_prem':2.0837066667, 'vol_20d':162.2298,
            'maturity_call_price':112., 'pure_bond_value':100.04147478379,
            'surplus_years':4.6, **changes}


def test_official_event_overrides_contract_expiry_and_stops_ordinary_model():
    item = ordinary()
    assert lifecycle(item, '2026-08-03')['conversion_end_date']=='2026-08-12'
    assert lifecycle(item, '2026-08-03')['redemption_price']==100.1622
    assert lifecycle(item, '2026-08-03')['redemption_payment_date']=='2026-08-13'
    assert pricing_exclusion(item,'2026-08-03')=='forced_redemption_event'
    assert bs_pricing.bond_metrics(item,asof='2026-08-03') is None
    assert recommendation_reason(item,'2026-08-03')=='forced_redemption_event'


def test_official_announcement_does_not_change_previous_day():
    item = ordinary()
    assert pricing_exclusion(item,'2026-07-16') is None
    assert bs_pricing.bond_metrics(item,asof='2026-07-16') is not None
    assert lifecycle(item,'2026-07-16')['conversion_end_date']=='2031-03-17'


@pytest.mark.parametrize('code,decision,implementation', [
    ('118053.SH','2026-07-17','2026-08-03'),
    ('113658.SH','2026-07-16','2026-07-23'),
    ('113697.SH','2026-08-06','2026-08-14'),
    ('113667.SH','2026-08-08','2026-08-18'),
    ('127067.SZ','2026-08-18','2026-08-20'),
])
def test_decision_blocks_model_before_implementation_cash_terms(code,decision,implementation):
    item=ordinary(code=code)
    facts=lifecycle(item,decision)
    assert facts['lifecycle_known_on']==decision
    assert facts['redemption_price'] is None
    assert facts['redemption_payment_date'] is None
    assert facts['conversion_end_date']=='2031-03-17'
    assert pricing_exclusion(item,decision)=='forced_redemption_event'
    assert bs_pricing.bond_metrics(item,asof=decision) is None
    implemented=lifecycle(item,implementation)
    assert implemented['lifecycle_known_on']==implementation
    assert implemented['redemption_price'] is not None
    assert implemented['redemption_payment_date'] is not None


def test_unregistered_early_stop_cannot_produce_fake_long_term_undervaluation():
    item = ordinary(code='118999.SH', redemp_stop_date='20260810', lifecycle_known_on='2026-08-03')
    assert pricing_exclusion(item,'2026-08-03')=='unverified_redemption_terms'
    assert bs_pricing.bond_metrics(item,asof='2026-08-03') is None
    assert recommendation_reason(item,'2026-08-03')=='unverified_redemption_terms'
    assert pricing_exclusion(item,'2026-07-31') is None


def test_sparse_decision_does_not_backdate_later_observed_stop():
    item=ordinary(stop_trading_date='2026-08-10',lifecycle_known_on='2026-07-20',
                  lifecycle_source='iFinD snapshot')
    observed=lifecycle(item,'2026-07-20')
    assert observed['redemption_kind']=='forced_redemption'
    assert observed['lifecycle_known_on']=='2026-07-20'
    assert observed['stop_trading_date']=='2026-08-10'
    earlier=lifecycle(item,'2026-07-17')
    assert earlier['redemption_kind']=='forced_redemption'
    assert earlier['lifecycle_known_on']=='2026-07-17'
    assert earlier['stop_trading_date'] is None
    assert earlier['redemption_price'] is None


def test_ordinary_maturity_and_temporary_suspension_not_misclassified():
    item = ordinary(code='118999.SH', maturity='2026-08-12', conversion_end_date='2026-08-12',
                    redemp_stop_date='20260810', redemption_kind='maturity_redemption')
    assert pricing_exclusion(item,'2026-08-03') is None
    assert bs_pricing.bond_metrics(item,asof='2026-08-03')['model_term_years']==9/365
    pause=ordinary(code='118999.SH', trading_suspensions=[{'known_on':'2026-08-03','start':'2026-08-05','end':'2026-08-06'}])
    assert pricing_exclusion(pause,'2026-08-03') is None


def test_report_recompute_clears_old_bs_fields_in_file_and_database(tmp_path, monkeypatch):
    item = ordinary(**dict.fromkeys(['bs_value','relative_value','bs_delta','bs_gamma','bs_theta','bs_vega'],9.))
    path=tmp_path/'dataset.json';path.write_text(json.dumps({'items':[item]}))
    class Connection:
        def close(self): pass
    written=[]
    monkeypatch.setattr(bs_pricing,'connect',lambda:Connection())
    monkeypatch.setattr(bs_pricing,'db_upsert',lambda con,table,rows,keys: written.extend(rows) or len(rows))
    monkeypatch.setattr(sys,'argv',['bs_pricing','--dataset',str(path),'--trade-date','2026-08-03'])
    bs_pricing.main()
    result=json.loads(path.read_text())['items'][0]
    for key in ['bs_value','relative_value','bs_delta','bs_gamma','bs_theta','bs_vega']:
        assert result[key] is None
        assert written[0][key] is None
    assert result['recommendation_eligible'] is False
    assert result['conversion_end_date']=='2026-08-12'


def test_backtest_uses_same_terminal_pricing_guard(monkeypatch):
    item=ordinary()
    monkeypatch.setattr(bt,'snapshot_metadata',lambda day:{item['code']:item})
    row=bt.build_day_bonds({item['code']:{'20260803':item['latest']}},
                           {item['code']:item},'20260803')[0]
    assert row['relative_value'] is None and row['delta'] is None
    assert bt.select_low_rv([row]*5)==[]


@pytest.mark.parametrize('code,day,end,price',[
    ('113658.SH','2026-07-28','2026-08-11',101.3562),
    ('113697.SH','2026-08-14','2026-08-27',100.094),
    ('113667.SH','2026-08-18','2026-08-28',100.686),
])
def test_verified_terms_have_source_and_payment_separate_from_conversion(code,day,end,price):
    facts=lifecycle(ordinary(code=code),day)
    assert facts['conversion_end_date']==end
    assert facts['redemption_price']==price
    assert facts['redemption_payment_date']>end
    assert facts['lifecycle_source'].startswith('https://')
    assert facts['redemption_kind']=='forced_redemption'
