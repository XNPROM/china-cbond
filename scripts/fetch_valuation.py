"""Batch-fetch convertible-bond valuation snapshot.

For every bond code, pull:
  - ths_conversion_premium_rate_cbond (转股溢价率, %)
  - ths_pure_bond_premium_rate_cbond (纯债溢价率, %)
  - ths_bond_balance_cbond (余额, 亿元)
  - ths_issue_credit_rating_cbond (发行评级)
  - latest price + changeRatio (实时价 + 当日涨跌幅)
  - 强赎: no_call_start/end, call_trigger_days, call_trigger_ratio
  - 下修: has_down_revision, down_trigger_ratio
  - 转股价
  - 正股PB (ths_stock_pb_cbond)
  - 同花顺行业 (ths_the_ths_industry_cbond)

Then for every underlying stock code, pull:
  - ths_pe_ttm (正股滚动市盈率)
  - ths_market_value_stock (正股总市值, 元 → 亿元)

Usage:
  python3 fetch_valuation.py \
      --codes cbond_codes.txt \
      --universe cbond_universe.json \
      --date 2026-04-22 \
      --out cbond_valuation_20260422.csv
"""
import argparse, csv, json, os, sys, time

sys.path.insert(0, os.path.dirname(__file__))
from _ifind import basic_data, realtime, batched
from _db import connect, init_schema, upsert as db_upsert


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--codes", required=True, help="codes.txt, one per line")
    ap.add_argument("--universe", required=True, help="cbond_universe.json for code→ucode mapping")
    ap.add_argument("--date", required=True, help="YYYY-MM-DD snapshot date")
    ap.add_argument("--out", required=True, help="output CSV path")
    ap.add_argument("--batch-size", type=int, default=40)
    args = ap.parse_args()

    codes = [l.strip() for l in open(args.codes) if l.strip()]
    print(f"[codes] {len(codes)}")

    # Load code→ucode mapping from universe
    uni = json.load(open(args.universe, encoding="utf-8"))
    code_to_ucode = {r["code"]: r["ucode"] for r in uni.get("items", []) if r.get("ucode")}
    ucode_to_codes = {}
    for c, uc in code_to_ucode.items():
        ucode_to_codes.setdefault(uc, []).append(c)
    ucodes = sorted(set(code_to_ucode.values()))
    print(f"[ucodes] {len(ucodes)} unique underlying stocks")

    indipara = [
        {"indicator": "ths_conversion_premium_rate_cbond", "indiparams": [args.date]},
        {"indicator": "ths_pure_bond_premium_rate_cbond", "indiparams": [args.date]},
        {"indicator": "ths_bond_balance_cbond", "indiparams": [args.date]},
        {"indicator": "ths_issue_credit_rating_cbond", "indiparams": [""]},
        {"indicator": "ths_maturity_date_bond", "indiparams": [""]},
        {"indicator": "ths_redemp_stop_trading_date_bond", "indiparams": [""]},
        # 强赎
        {"indicator": "ths_not_compulsory_redemp_startdate_cbond", "indiparams": [""]},
        {"indicator": "ths_not_compulsory_redemp_enddate_cbond_bond", "indiparams": [""]},
        {"indicator": "ths_conditionalredemption_triggercumulativedays_cbond", "indiparams": [args.date]},
        {"indicator": "ths_redemp_trigger_ratio_cbond", "indiparams": [""]},
        # 下修
        {"indicator": "ths_is_special_down_correct_clause_cbond", "indiparams": [""]},
        {"indicator": "ths_trigger_ratio_cbond", "indiparams": [""]},
        # 转股价
        {"indicator": "ths_conversion_price_cbond", "indiparams": [args.date]},
        # 正股PB
        {"indicator": "ths_stock_pb_cbond", "indiparams": [args.date]},
        # 同花顺行业
        {"indicator": "ths_the_ths_industry_cbond", "indiparams": [""]},
        # 隐含波动率
        {"indicator": "ths_implied_volatility_cbond", "indiparams": [args.date]},
        # 纯债到期收益率
        {"indicator": "ths_pure_bond_ytm_cbond", "indiparams": [args.date]},
        # iFinD双低值
        {"indicator": "ths_convertible_debt_doublelow_cbond", "indiparams": [args.date]},
        # 期权价值
        {"indicator": "ths_option_value_cbond", "indiparams": [args.date]},
        # 剩余期限(天)
        {"indicator": "ths_surplus_term_d_cbond", "indiparams": [args.date]},
        # 剩余期限(年)
        {"indicator": "ths_remain_duration_y_cbond", "indiparams": [args.date]},
        # 累计转股比例
        {"indicator": "ths_accum_conversion_ratio_cbond", "indiparams": [args.date]},
        # 转股稀释比例
        {"indicator": "ths_conversion_dlt_ratio_cbond", "indiparams": [args.date]},
        # 纯债价值
        {"indicator": "ths_pure_bond_value_cbond", "indiparams": [args.date]},
        # 到期赎回价
        {"indicator": "ths_maturity_redemp_price_cbond", "indiparams": [""]},
    ]

    rows = {}
    for b in batched(codes, args.batch_size):
        try:
            r = basic_data(b, indipara)
            for t in r.get("tables", []):
                tbl = t.get("table", {})
                rows[t["thscode"]] = {
                    "conv_prem": (tbl.get("ths_conversion_premium_rate_cbond") or [None])[0],
                    "pure_prem": (tbl.get("ths_pure_bond_premium_rate_cbond") or [None])[0],
                    "balance":   (tbl.get("ths_bond_balance_cbond") or [None])[0],
                    "rating":    (tbl.get("ths_issue_credit_rating_cbond") or [""])[0],
                    "maturity":  (tbl.get("ths_maturity_date_bond") or [""])[0],
                    "redemp_stop_date": (tbl.get("ths_redemp_stop_trading_date_bond") or [""])[0],
                    "no_call_start": (tbl.get("ths_not_compulsory_redemp_startdate_cbond") or [None])[0],
                    "no_call_end":   (tbl.get("ths_not_compulsory_redemp_enddate_cbond_bond") or [None])[0],
                    "call_trigger_days": (tbl.get("ths_conditionalredemption_triggercumulativedays_cbond") or [None])[0],
                    "call_trigger_ratio": (tbl.get("ths_redemp_trigger_ratio_cbond") or [None])[0],
                    "has_down_revision":  (tbl.get("ths_is_special_down_correct_clause_cbond") or [""])[0],
                    "down_trigger_ratio": (tbl.get("ths_trigger_ratio_cbond") or [None])[0],
                    "conv_price":  (tbl.get("ths_conversion_price_cbond") or [None])[0],
                    "pb":          (tbl.get("ths_stock_pb_cbond") or [None])[0],
                    "ths_industry": (tbl.get("ths_the_ths_industry_cbond") or [""])[0],
                    "implied_vol": (tbl.get("ths_implied_volatility_cbond") or [None])[0],
                    "pure_bond_ytm": (tbl.get("ths_pure_bond_ytm_cbond") or [None])[0],
                    "ifind_doublelow": (tbl.get("ths_convertible_debt_doublelow_cbond") or [None])[0],
                    "option_value": (tbl.get("ths_option_value_cbond") or [None])[0],
                    "surplus_days": (tbl.get("ths_surplus_term_d_cbond") or [None])[0],
                    "surplus_years": (tbl.get("ths_remain_duration_y_cbond") or [None])[0],
                    "accum_conv_ratio": (tbl.get("ths_accum_conversion_ratio_cbond") or [None])[0],
                    "dilution_ratio": (tbl.get("ths_conversion_dlt_ratio_cbond") or [None])[0],
                    "pure_bond_value": (tbl.get("ths_pure_bond_value_cbond") or [None])[0],
                    "maturity_call_price": (tbl.get("ths_maturity_redemp_price_cbond") or [None])[0],
                }
        except Exception as e:
            print(f"[warn] valuation batch err: {e}")
        time.sleep(0.15)

    # latest price + day change
    for b in batched(codes, 80):
        try:
            r = realtime(b, "latest,changeRatio")
            for t in r.get("tables", []):
                table = t.get("table", {})
                rows.setdefault(t["thscode"], {})["latest"] = (table.get("latest") or [None])[0]
                rows.setdefault(t["thscode"], {})["change_pct"] = (table.get("changeRatio") or [None])[0]
        except Exception as e:
            print(f"[warn] price batch err: {e}")
        time.sleep(0.12)

    # Second pass: stock-side PE_TTM + total market cap
    stock_data = {}
    stock_fields = [
        {"indicator": "ths_pe_ttm", "indiparams": [args.date]},
        {"indicator": "ths_market_value_stock", "indiparams": [args.date]},
    ]
    for b in batched(ucodes, args.batch_size):
        try:
            r = basic_data(b, stock_fields)
            for t in r.get("tables", []):
                tbl = t.get("table", {})
                pe = (tbl.get("ths_pe_ttm") or [None])[0]
                mv_raw = (tbl.get("ths_market_value_stock") or [None])[0]
                mv_yi = round(mv_raw / 1e8, 2) if mv_raw else None
                stock_data[t["thscode"]] = {"pe_ttm": pe, "total_mv_yi": mv_yi}
        except Exception as e:
            print(f"[warn] stock batch err: {e}")
        time.sleep(0.15)
    print(f"[stock] PE non-null: {sum(1 for v in stock_data.values() if v.get('pe_ttm') is not None)}, "
          f"MV non-null: {sum(1 for v in stock_data.values() if v.get('total_mv_yi') is not None)}")

    with open(args.out, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["转债代码", "最新价", "当日涨跌幅(%)", "转股溢价率(%)", "纯债溢价率(%)",
                    "余额(亿元)", "评级", "到期日", "强赎停止交易日", "转股价", "正股PB",
                    "不强赎起始日", "不强赎截止日", "强赎累计触发天数", "强赎触发比例(%)",
                    "是否有下修条款", "下修触发比例(%)", "同花顺行业",
                    "正股PE_TTM", "正股总市值(亿)", "隐含波动率(%)",
                    "纯债YTM(%)", "iFinD双低", "期权价值", "剩余期限(天)", "剩余期限(年)",
                    "累计转股比例(%)", "转股稀释比例(%)", "纯债价值", "到期赎回价"])
        for c in codes:
            r = rows.get(c, {})
            uc = code_to_ucode.get(c, "")
            sd = stock_data.get(uc, {})
            w.writerow([c, r.get("latest", ""), r.get("change_pct", ""),
                        r.get("conv_prem", ""), r.get("pure_prem", ""),
                        r.get("balance", ""), r.get("rating", ""), r.get("maturity", ""),
                        r.get("redemp_stop_date", ""),
                        r.get("conv_price", ""), r.get("pb", ""),
                        r.get("no_call_start", ""), r.get("no_call_end", ""),
                        r.get("call_trigger_days", ""), r.get("call_trigger_ratio", ""),
                        r.get("has_down_revision", ""), r.get("down_trigger_ratio", ""),
                        r.get("ths_industry", ""),
                        sd.get("pe_ttm", ""), sd.get("total_mv_yi", ""),
                        r.get("implied_vol", ""),
                        r.get("pure_bond_ytm", ""), r.get("ifind_doublelow", ""),
                        r.get("option_value", ""), r.get("surplus_days", ""),
                        r.get("surplus_years", ""), r.get("accum_conv_ratio", ""),
                        r.get("dilution_ratio", ""),
                        r.get("pure_bond_value", ""), r.get("maturity_call_price", "")])
    print(f"[done] {len(rows)}/{len(codes)} rows → {args.out}")

    # upsert to DuckDB
    def _f(v):
        try: return float(v)
        except: return None
    def _i(v):
        try: return int(v)
        except: return None

    db_rows = [
        {
            "trade_date": args.date,
            "code": c,
            "price": _f(rows.get(c, {}).get("latest")),
            "change_pct": _f(rows.get(c, {}).get("change_pct")),
            "conv_prem_pct": _f(rows.get(c, {}).get("conv_prem")),
            "pure_prem_pct": _f(rows.get(c, {}).get("pure_prem")),
            "outstanding_yi": _f(rows.get(c, {}).get("balance")),
            "rating": rows.get(c, {}).get("rating", ""),
            "maturity_date": rows.get(c, {}).get("maturity", ""),
            "conv_price": _f(rows.get(c, {}).get("conv_price")),
            "no_call_start": rows.get(c, {}).get("no_call_start") or "",
            "no_call_end": rows.get(c, {}).get("no_call_end") or "",
            "call_trigger_days": _i(rows.get(c, {}).get("call_trigger_days")),
            "call_trigger_ratio": _f(rows.get(c, {}).get("call_trigger_ratio")),
            "has_down_revision": rows.get(c, {}).get("has_down_revision", ""),
            "down_trigger_ratio": _f(rows.get(c, {}).get("down_trigger_ratio")),
            "ths_industry": rows.get(c, {}).get("ths_industry", ""),
            "pb": _f(rows.get(c, {}).get("pb")),
            "redemp_stop_date": rows.get(c, {}).get("redemp_stop_date") or "",
            "implied_vol": _f(rows.get(c, {}).get("implied_vol")),
            "pure_bond_ytm": _f(rows.get(c, {}).get("pure_bond_ytm")),
            "ifind_doublelow": _f(rows.get(c, {}).get("ifind_doublelow")),
            "option_value": _f(rows.get(c, {}).get("option_value")),
            "surplus_days": _i(rows.get(c, {}).get("surplus_days")),
            "surplus_years": _f(rows.get(c, {}).get("surplus_years")),
            "accum_conv_ratio": _f(rows.get(c, {}).get("accum_conv_ratio")),
            "dilution_ratio": _f(rows.get(c, {}).get("dilution_ratio")),
            "pure_bond_value": _f(rows.get(c, {}).get("pure_bond_value")),
            "maturity_call_price": _f(rows.get(c, {}).get("maturity_call_price")),
            "pe_ttm": stock_data.get(code_to_ucode.get(c, ""), {}).get("pe_ttm"),
            "total_mv_yi": stock_data.get(code_to_ucode.get(c, ""), {}).get("total_mv_yi"),
        }
        for c in codes
    ]
    con = connect()
    init_schema(con)
    n = db_upsert(con, "valuation_daily", db_rows, ["trade_date", "code"])
    con.close()
    print(f"[db] valuation_daily upserted {n} rows (trade_date={args.date})")


if __name__ == "__main__":
    main()
