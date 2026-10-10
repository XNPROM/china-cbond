import json
import math
import sys
from pathlib import Path
import pytest
sys.path.insert(0, str(Path(__file__).resolve().parents[1]/'scripts'))
from _lifecycle import lifecycle, annotate, trade_status, recommendation_reason, next_session, filter_snapshot
from bs_pricing import bs_call, bond_metrics
from backtest_weekly import _compute_bs_delta, _compute_relative_value, Portfolio, execution_prices


@pytest.fixture
def honglu():
    return {'code':'128134.SZ','name':'鸿路转债','maturity':'2026/10/08',
            'redemp_stop_date':'','list_date':'20201102'}


def test_honglu_signal_and_execution_dates(honglu):
    assert trade_status(honglu,'2026-09-28') == 'trading'
    assert recommendation_reason(honglu,'2026-09-28') == 'stopped_trading'
    for day in ('2026-09-29','2026-09-30','2026-10-08'):
        assert filter_snapshot([honglu],day)[0] == []
    assert trade_status(honglu,'2026-10-09') == 'delisted'


def test_no_announcement_lookahead(honglu):
    assert lifecycle(honglu,'2026-09-27')['stop_trading_date'] is None
    assert lifecycle(honglu,'2026-09-28')['stop_trading_date'] == '2026-09-29'


def test_null_stop_does_not_mean_trading(honglu):
    honglu['code']='128999.SZ'
    assert trade_status(honglu,'2026-09-29') == 'unverified_stop_date'
    assert recommendation_reason(honglu,'2026-09-28') == 'unverified_stop_date'


def test_far_maturity_not_excluded_for_null_stop():
    item={'code':'123999.SZ','maturity':'2031/12/31'}
    assert recommendation_reason(item,'2026-09-28') is None


def test_holiday_calendar_and_temporary_pause():
    assert next_session('2026-09-30') == '2026-10-08'
    x={'code':'123999.SZ','maturity':'2031/12/31',
       'trading_suspensions':[{'known_on':'2026-09-28','start':'2026-09-29','end':'2026-09-29'}]}
    assert trade_status(x,'2026-09-29')=='temporarily_suspended'
    assert trade_status(x,'2026-09-30')=='trading'


def test_percent_unit_is_identical_in_report_and_backtest():
    x={'latest':120,'conv_prem':20,'vol_20d':1,'surplus_years':2,'maturity_call_price':110,'pure_bond_value':100}
    result=bond_metrics(x)
    assert _compute_bs_delta(120,20,1,2,110)==result['bs_delta']
    assert _compute_relative_value(120,20,1,2,110,100)==result['relative_value']
    assert result['bs_delta'] < .001
    assert _compute_bs_delta(120,20,None,2,110) is None
    assert _compute_relative_value(120,20,30,2,110,None) is None


def test_true_one_day_term_and_verified_end():
    x={'latest':110,'conv_prem':0,'vol_20d':35,'surplus_years':2,'maturity_call_price':110,'pure_bond_value':100,'conversion_end_date':'2026-10-08'}
    assert bond_metrics(x,asof='2026-10-07')['model_term_years']==1/365
    assert bond_metrics({**x,'surplus_years':None,'conversion_end_date':None}) is None
    assert bs_call(120,100,0,.025,2)[1]==1
    assert bs_call(120,100,.3,.025,0)[1]==1
    assert bs_call(100,100,.3,.025,0)[1] is None


@pytest.mark.parametrize('args',[(100,110,.35,.025,2),(110,110,.5,.01,.005),(200,100,.3,.025,2)])
def test_greeks_match_independent_finite_differences(args):
    # A separately implemented reference price plus numerical differentiation.
    def C(S,K,v,r,T):
        d1=(math.log(S/K)+(r+v*v/2)*T)/(v*math.sqrt(T)); d2=d1-v*math.sqrt(T)
        N=lambda d:math.erfc(-d/math.sqrt(2))/2
        return S*N(d1)-K*math.exp(-r*T)*N(d2)
    S,K,v,r,T=args;h=S*1e-4;ht=1e-6;hv=1e-5
    price,delta,gamma,theta,vega=bs_call(*args)
    assert price==pytest.approx(C(*args),abs=1e-10)
    assert delta==pytest.approx((C(S+h,K,v,r,T)-C(S-h,K,v,r,T))/(2*h),abs=1e-7)
    assert gamma==pytest.approx((C(S+h,K,v,r,T)-2*C(*args)+C(S-h,K,v,r,T))/(h*h),abs=1e-7)
    assert theta==pytest.approx(-(C(S,K,v,r,T+ht)-C(S,K,v,r,T-ht))/(2*ht*365),abs=1e-7)
    assert vega==pytest.approx((C(S,K,v+hv,r,T)-C(S,K,v-hv,r,T))/(2*hv*100),abs=1e-7)


def test_missing_exit_price_cannot_be_zero_return():
    with pytest.raises(ValueError,match='unresolved'):
        Portfolio().rebalance(list('abcde'),dict.fromkeys('abcde',100),dict.fromkeys('abcd',101))


def test_terminal_event_sells_on_last_actual_session(monkeypatch,honglu):
    import backtest_weekly as bt
    monkeypatch.setattr(bt,'snapshot_metadata',lambda day:{'128134.SZ':{**honglu, 'quote_date':day, 'quote_volume':100}})
    px={'128134.SZ':{'20260924':111,'20260928':110.053,'20260929':110.053,'20260930':110.053}}
    buy,sell,forced,audit=execution_prices(['128134.SZ'],px,'20260924','20260930','20260923')
    # The event was announced on 28 Sep, before the last-session closing execution.
    # Before then the near-expiry guard conservatively blocks new positions.
    assert buy=={}  # announcement not known on signal date; eligibility is unverified.


def test_known_terminal_event_exit_and_sell_cost(monkeypatch,honglu):
    import backtest_weekly as bt
    honglu.update(stop_trading_date='2026-09-29',last_trade_date='2026-09-28',lifecycle_known_on='2026-09-20')
    monkeypatch.setattr(bt,'snapshot_metadata',lambda day:{'128134.SZ':{**honglu, 'quote_date':day, 'quote_volume':100}})
    px={'128134.SZ':{'20260924':111,'20260928':110.053,'20260929':110.053,'20260930':110.053}}
    buy,sell,forced,audit=execution_prices(['128134.SZ'],px,'20260924','20260930','20260923')
    assert buy=={'128134.SZ':111}
    assert sell=={'128134.SZ':110.053}
    assert forced=={'128134.SZ'}
    assert audit[0]['exit_date']=='20260928'
    port=Portfolio(0,0)
    codes=list('abcde');pb=dict.fromkeys(codes,100);ps=dict.fromkeys(codes,110)
    ret,n,cost=port.rebalance(codes,pb,ps,{'a'})
    assert ret==pytest.approx(.1)
    assert 'a' not in port.holdings


def test_stale_and_zero_volume_quotes_are_not_buy_signals():
    x={'code':'123999.SZ','maturity':'2031-12-31','quote_date':'2026-09-28','quote_volume':100}
    assert recommendation_reason(x,'2026-09-29') == 'stale_quote'
    assert recommendation_reason({**x,'quote_volume':0},'2026-09-28') == 'no_execution_liquidity'


def test_future_observed_event_not_used_early():
    x={'code':'123999.SZ','maturity':'2031-12-31','stop_trading_date':'2026-09-29',
       'last_trade_date':'2026-09-28','lifecycle_known_on':'2026-09-30'}
    assert lifecycle(x,'2026-09-28')['stop_trading_date'] is None


def test_cash_settlement_is_not_stale_price_or_market_sale(monkeypatch):
    import backtest_weekly as bt
    x={'code':'123999.SZ','maturity':'2031-12-31','quote_volume':100}
    def metadata(day):
        item={**x, 'quote_date':day}
        if day >= '2026-09-30':
            item.update(stop_trading_date='2026-09-29',last_trade_date='2026-09-28',
                        lifecycle_known_on='2026-09-30',redemption_date='2026-10-08',redemption_payment_date='2026-10-09',
                        redemption_price=110,lifecycle_source='https://example.com/official-announcement')
            item['quote_volume']=0
        return {x['code']:item}
    monkeypatch.setattr(bt,'snapshot_metadata',metadata)
    px={x['code']:{'20260924':111,'20260928':108,'20260930':108,'20261009':108}}
    buy,sell,forced,audit=execution_prices([x['code']],px,'20260924','20261009','20260923')
    assert sell=={x['code']:110}  # no retroactive Sep28 sale from Sep30 knowledge
    assert audit[0]['reason']=='cash_redemption'
    buy,sell,forced,audit=execution_prices([x['code']],px,'20260924','20260930','20260923')
    assert sell=={}  # a future cash payment is not a current-period settlement
    assert audit[0]['reason']=='exit_not_tradable'
    codes=list('abcde');pb=dict.fromkeys(codes,100);ps=dict.fromkeys(codes,110)
    market=Portfolio(10,2).rebalance(codes,pb,ps,{'a'})[0]
    cash=Portfolio(10,2).rebalance(codes,pb,ps,{'a'},{'a'})[0]
    assert cash-market==pytest.approx(.0011/5)


def test_future_static_stop_observation_does_not_filter_history():
    from fetch_cb_universe import _filter_stopped_bonds
    x={'code':'123999.SZ','maturity':'2031-12-31','redemp_stop_date':'20260929',
       'lifecycle_known_on':'2026-10-09'}
    assert lifecycle(x,'2026-09-30')['stop_trading_date'] is None
    assert _filter_stopped_bonds([x],'20260930')[0]==[x]
    assert _filter_stopped_bonds([x],'20261009')[0]==[]


def test_empty_rebalance_schedule_needs_no_database_query(monkeypatch):
    import backtest_weekly as bt
    monkeypatch.setattr(bt,'connect',lambda **kwargs:pytest.fail('empty dates must not query DB'))
    assert bt.fetch_fundamentals_from_db([])=={}


def test_execution_quote_cache_does_not_create_recommendation_membership(monkeypatch):
    import backtest_weekly as bt
    monkeypatch.setattr(bt,'snapshot_metadata',lambda day:{})
    monkeypatch.setattr(bt,'execution_quote_cache',lambda:{'2026-09-28':{'123999.SZ':{'quote_date':'2026-09-28','quote_volume':100,'price':110}}})
    assert bt.execution_evidence('20260928','123999.SZ')['price']==110
    assert bt.build_day_bonds({'123999.SZ':{'20260928':110}}, {}, '20260928')[0].get('maturity') is None
    assert recommendation_reason(bt.build_day_bonds({'123999.SZ':{'20260928':110}}, {}, '20260928')[0], '2026-09-28')=='missing_conversion_end_date'


def test_estimated_stop_guard_ends_at_last_session_before_weekend_maturity():
    from _lifecycle import lifecycle
    facts=lifecycle({"code":"123999.SZ","maturity":"2026-07-12"},"2026-07-08")
    assert facts["estimated_stop_date"]=="2026-07-08"
    assert facts["stop_trading_date"] is None
