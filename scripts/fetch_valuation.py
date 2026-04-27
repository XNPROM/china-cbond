"""Batch-fetch convertible-bond valuation snapshot (extended).

For every bond code, pull:
  - 估值: conv_prem / pure_prem / balance / rating / maturity / conv_price
  - 强赎: no_call_start/end, call_trigger_days, call_trigger_ratio
  - 下修: has_down_revision, down_trigger_ratio
  - 正股衍生: stock_pb_cbond, ths_industry
  - 期权/债值: implied_vol, pure_bond_ytm, ifind_doublelow, option_value
  - 期限: surplus_days / surplus_years
  - 稀释: accum_conv_ratio / dilution_ratio
  - 兑付: pure_bond_value, maturity_call_price, redemp_stop_date

Then for every underlying stock code, pull:
  - ths_pe_ttm (正股滚动市盈率)
  - ths_market_value_stock (正股总市值, 元 → 亿元)

价格取法：cmd_history_quotation close (官方净价，= Wind 口径)
          NOT realtime latest (盘中快照，会偏)

Usage:
  python3 fetch_valuation.py \\
      --codes    data/raw/asof=YYYY-MM-DD/cbond_codes.txt \\
      --universe data/raw/asof=YYYY-MM-DD/cbond_universe.json \\
      --date     YYYY-MM-DD \\
      --out      data/raw/asof=YYYY-MM-DD/valuation.csv
"""
import argparse, csv, json, os, sys, time

sys.path.insert(0, os.path.dirname(__file__))
from _ifind import basic_data, history, batched
from _db import connect, init_schema, upsert as db_upsert


def _f(v):
    try:
        return float(v)
    except Exception:
        return None


def _i(v):
    try:
        return int(v)
    except Exception:
        return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--codes", required=True, help="codes.txt, one per line")
    ap.add_argument("--universe", required=True, help="cbond_universe.json for code→ucode map")
    ap.add_argument("--date", required=True, help="YYYY-MM-DD snapshot date")
    ap.add_argument("--out", required=True, help="output CSV path")
    ap.add_argument("--batch-size", type=int, default=40)
    args = ap.parse_args()

    codes = [l.strip() for l in open(args.codes) if l.strip()]
    print(f"[codes] {len(codes)}")

    # 读 universe 得到 code→ucode 映射
    uni = json.load(open(args.universe, encoding="utf-8"))
    code_to_ucode = {r["code"]: r["ucode"] for r in uni.get("items", []) if r.get("ucode")}
    ucodes = sorted(set(code_to_ucode.values()))
    print(f"[ucodes] {len(ucodes)} unique underlying stocks")

    indipara = [
        {"indicator": "ths_conversion_premium_rate_cbond",              "indiparams": [args.date]},
        {"indicator": "ths_pure_bond_premium_rate_cbond",               "indiparams": [args.date]},
        {"indicator": "ths_bond_balance_cbond",                         "indiparams": [args.date]},
        {"indicator": "ths_issue_credit_rating_cbond",                  "indiparams": [""]},
        {"indicator": "ths_maturity_date_bond",                         "indiparams": [""]},
        {"indicator": "ths_redemp_stop_trading_date_bond",              "indiparams": [""]},
        # 强赎
        {"indicator": "ths_not_compulsory_redemp_startdate_cbond",      "indiparams": [""]},
        {"indicator": "ths_not_compulsory_redemp_enddate_cbond_bond",   "indiparams": [""]},
        {"indicator": "ths_conditionalredemption_triggercumulativedays_cbond", "indiparams": [args.date]},
        {"indicator": "ths_redemp_trigger_ratio_cbond",                 "indiparams": [""]},
        # 下修
        {"indicator": "ths_is_special_down_correct_clause_cbond",       "indiparams": [""]},
        {"indicator": "ths_trigger_ratio_cbond",                        "indiparams": [""]},
        # 转股价
        {"indicator": "ths_conversion_price_cbond",                     "indiparams": [args.date]},
        # 正股 PB + 行业
        {"indicator": "ths_stock_pb_cbond",                             "indiparams": [args.date]},
        {"indicator": "ths_the_ths_industry_cbond",                     "indiparams": [""]},
        # 期权/债值
        {"indicator": "ths_implied_volatility_cbond",                   "indiparams": [args.date, "1", "1"]},
        {"indicator": "ths_pure_bond_ytm_cbond",                        "indiparams": [args.date]},
        {"indicator": "ths_convertible_debt_doublelow_cbond",           "indiparams": [args.date]},
        {"indicator": "ths_option_value_cbond",                         "indiparams": [args.date]},
        # 期限
        {"indicator": "ths_surplus_term_d_cbond",                       "indiparams": [args.date]},
        {"indicator": "ths_remain_duration_y_cbond",                    "indiparams": [args.date]},
        # 稀释
        {"indicator": "ths_accum_conversion_ratio_cbond",               "indiparams": [args.date]},
        {"indicator": "ths_conversion_dlt_ratio_cbond",                 "indiparams": [args.date]},
        # 兑付
        {"indicator": "ths_pure_bond_value_cbond",                      "indiparams": [args.date]},
        {"indicator": "ths_maturity_redemp_price_cbond",                "indiparams": [""]},
    ]

    rows = {}
    for b in batched(codes, args.batch_size):
        try:
            r = basic_data(b, indipara)
            for t in r.get("tables", []):
                tbl = t.get("table", {})
                rows[t["thscode"]] = {
                    "conv_prem":           (tbl.get("ths_conversion_premium_rate_cbond") or [None])[0],
                    "pure_prem":           (tbl.get("ths_pure_bond_premium_rate_cbond") or [None])[0],
                    "balance":             (tbl.get("ths_bond_balance_cbond") or [None])[0],
                    "rating":              (tbl.get("ths_issue_credit_rating_cbond") or [""])[0],
                    "maturity":            (tbl.get("ths_maturity_date_bond") or [""])[0],
                    "redemp_stop_date":    (tbl.get("ths_redemp_stop_trading_date_bond") or [""])[0],
                    "no_call_start":       (tbl.get("ths_not_compulsory_redemp_startdate_cbond") or [None])[0],
                    "no_call_end":         (tbl.get("ths_not_compulsory_redemp_enddate_cbond_bond") or [None])[0],
                    "call_trigger_days":   (tbl.get("ths_conditionalredemption_triggercumulativedays_cbond") or [None])[0],
                    "call_trigger_ratio":  (tbl.get("ths_redemp_trigger_ratio_cbond") or [None])[0],
                    "has_down_revision":   (tbl.get("ths_is_special_down_correct_clause_cbond") or [""])[0],
                    "down_trigger_ratio":  (tbl.get("ths_trigger_ratio_cbond") or [None])[0],
                    "conv_price":          (tbl.get("ths_conversion_price_cbond") or [None])[0],
                    "pb":                  (tbl.get("ths_stock_pb_cbond") or [None])[0],
                    "ths_industry":        (tbl.get("ths_the_ths_industry_cbond") or [""])[0],
                    "implied_vol":         (tbl.get("ths_implied_volatility_cbond") or [None])[0],
                    "pure_bond_ytm":       (tbl.get("ths_pure_bond_ytm_cbond") or [None])[0],
                    "ifind_doublelow":     (tbl.get("ths_convertible_debt_doublelow_cbond") or [None])[0],
                    "option_value":        (tbl.get("ths_option_value_cbond") or [None])[0],
                    "surplus_days":        (tbl.get("ths_surplus_term_d_cbond") or [None])[0],
                    "surplus_years":       (tbl.get("ths_remain_duration_y_cbond") or [None])[0],
                    "accum_conv_ratio":    (tbl.get("ths_accum_conversion_ratio_cbond") or [None])[0],
                    "dilution_ratio":      (tbl.get("ths_conversion_dlt_ratio_cbond") or [None])[0],
                    "pure_bond_value":     (tbl.get("ths_pure_bond_value_cbond") or [None])[0],
                    "maturity_call_price": (tbl.get("ths_maturity_redemp_price_cbond") or [None])[0],
                }
        except Exception as e:
            print(f"[warn] valuation batch err: {e}")
        time.sleep(0.15)

    # 官方收盘价 + 涨跌幅（= Wind 口径，替换掉 realtime latest）
    for b in batched(codes, 80):
        try:
            r = history(b, "close,changeRatio", args.date, args.date)
            for t in r.get("tables", []):
                table = t.get("table", {})
                rows.setdefault(t["thscode"], {})["latest"] = (table.get("close") or [None])[0]
                rows.setdefault(t["thscode"], {})["change_pct"] = (table.get("changeRatio") or [None])[0]
        except Exception as e:
            print(f"[warn] price batch err: {e}")
        time.sleep(0.12)

    # 正股 PE_TTM + 总市值
    stock_data = {}
    stock_fields = [
        {"indicator": "ths_pe_ttm",              "indiparams": [args.date]},
        {"indicator": "ths_market_value_stock",  "indiparams": [args.date]},
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

    # CSV 输出（30 列）
    with open(args.out, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow([
            "转债代码", "最新价", "当日涨跌幅(%)", "转股溢价率(%)", "纯债溢价率(%)",
            "余额(亿元)", "评级", "到期日", "强赎停止交易日", "转股价", "正股PB",
            "不强赎起始日", "不强赎截止日", "强赎累计触发天数", "强赎触发比例(%)",
            "是否有下修条款", "下修触发比例(%)", "同花顺行业",
            "正股PE_TTM", "正股总市值(亿)", "隐含波动率(%)",
            "纯债YTM(%)", "iFinD双低", "期权价值", "剩余期限(天)", "剩余期限(年)",
            "累计转股比例(%)", "转股稀释比例(%)", "纯债价值", "到期赎回价",
        ])
        for c in codes:
            r = rows.get(c, {})
            uc = code_to_ucode.get(c, "")
            sd = stock_data.get(uc, {})
            iv_raw = r.get("implied_vol")
            iv_pct = round(iv_raw * 100, 2) if iv_raw is not None else ""
            w.writerow([
                c, r.get("latest", ""), r.get("change_pct", ""),
                r.get("conv_prem", ""), r.get("pure_prem", ""),
                r.get("balance", ""), r.get("rating", ""), r.get("maturity", ""),
                r.get("redemp_stop_date", ""),
                r.get("conv_price", ""), r.get("pb", ""),
                r.get("no_call_start", ""), r.get("no_call_end", ""),
                r.get("call_trigger_days", ""), r.get("call_trigger_ratio", ""),
                r.get("has_down_revision", ""), r.get("down_trigger_ratio", ""),
                r.get("ths_industry", ""),
                sd.get("pe_ttm", ""), sd.get("total_mv_yi", ""),
                iv_pct,
                r.get("pure_bond_ytm", ""), r.get("ifind_doublelow", ""),
                r.get("option_value", ""), r.get("surplus_days", ""),
                r.get("surplus_years", ""), r.get("accum_conv_ratio", ""),
                r.get("dilution_ratio", ""),
                r.get("pure_bond_value", ""), r.get("maturity_call_price", ""),
            ])
    print(f"[done] {len(rows)}/{len(codes)} rows → {args.out}")

    # DuckDB upsert
    def _iv_pct(c):
        raw = _f(rows.get(c, {}).get("implied_vol"))
        return round(raw * 100, 2) if raw is not None else None

    db_rows = [
        {
            "trade_date":          args.date,
            "code":                c,
            "price":               _f(rows.get(c, {}).get("latest")),
            "change_pct":          _f(rows.get(c, {}).get("change_pct")),
            "conv_prem_pct":       _f(rows.get(c, {}).get("conv_prem")),
            "pure_prem_pct":       _f(rows.get(c, {}).get("pure_prem")),
            "outstanding_yi":      _f(rows.get(c, {}).get("balance")),
            "rating":              rows.get(c, {}).get("rating", ""),
            "maturity_date":       rows.get(c, {}).get("maturity", ""),
            "conv_price":          _f(rows.get(c, {}).get("conv_price")),
            "no_call_start":       rows.get(c, {}).get("no_call_start") or "",
            "no_call_end":         rows.get(c, {}).get("no_call_end") or "",
            "call_trigger_days":   _i(rows.get(c, {}).get("call_trigger_days")),
            "call_trigger_ratio":  _f(rows.get(c, {}).get("call_trigger_ratio")),
            "has_down_revision":   rows.get(c, {}).get("has_down_revision", ""),
            "down_trigger_ratio":  _f(rows.get(c, {}).get("down_trigger_ratio")),
            "ths_industry":        rows.get(c, {}).get("ths_industry", ""),
            "pb":                  _f(rows.get(c, {}).get("pb")),
            "redemp_stop_date":    rows.get(c, {}).get("redemp_stop_date") or "",
            "implied_vol":         _iv_pct(c),
            "pure_bond_ytm":       _f(rows.get(c, {}).get("pure_bond_ytm")),
            "ifind_doublelow":     _f(rows.get(c, {}).get("ifind_doublelow")),
            "option_value":        _f(rows.get(c, {}).get("option_value")),
            "surplus_days":        _i(rows.get(c, {}).get("surplus_days")),
            "surplus_years":       _f(rows.get(c, {}).get("surplus_years")),
            "accum_conv_ratio":    _f(rows.get(c, {}).get("accum_conv_ratio")),
            "dilution_ratio":      _f(rows.get(c, {}).get("dilution_ratio")),
            "pure_bond_value":     _f(rows.get(c, {}).get("pure_bond_value")),
            "maturity_call_price": _f(rows.get(c, {}).get("maturity_call_price")),
            "pe_ttm":              stock_data.get(code_to_ucode.get(c, ""), {}).get("pe_ttm"),
            "total_mv_yi":         stock_data.get(code_to_ucode.get(c, ""), {}).get("total_mv_yi"),
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
