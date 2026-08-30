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


def _present(v):
    return v is not None and v != ""


def _norm_code(v):
    return str(v).strip().upper() if v else ""


def _load_existing_rows(path, expected_codes):
    """Load same-date CSV values for an incremental repair.

    The old CSV can contain retired/unexpected codes. Keep only the current
    expected set and normalize the public CSV columns back to the internal
    field names used by this fetcher.
    """
    if not path or not os.path.isfile(path):
        return {}
    rows = {}
    try:
        with open(path, encoding="utf-8-sig", newline="") as f:
            for source in csv.DictReader(f):
                code = _norm_code(source.get("转债代码"))
                if code not in expected_codes:
                    continue
                row = {
                    "latest": source.get("最新价", "").strip(),
                    "change_pct": source.get("当日涨跌幅(%)", "").strip(),
                    "conv_prem": source.get("转股溢价率(%)", "").strip(),
                    "pure_prem": source.get("纯债溢价率(%)", "").strip(),
                    "balance": source.get("余额(亿元)", "").strip(),
                    "rating": source.get("评级", "").strip(),
                    "maturity": source.get("到期日", "").strip(),
                    "redemp_stop_date": source.get("强赎停止交易日", "").strip(),
                    "conv_price": source.get("转股价", "").strip(),
                    "pb": source.get("正股PB", "").strip(),
                    "no_call_start": source.get("不强赎起始日", "").strip(),
                    "no_call_end": source.get("不强赎截止日", "").strip(),
                    "call_trigger_days": source.get("强赎累计触发天数", "").strip(),
                    "call_trigger_ratio": source.get("强赎触发比例(%)", "").strip(),
                    "has_down_revision": source.get("是否有下修条款", "").strip(),
                    "down_trigger_ratio": source.get("下修触发比例(%)", "").strip(),
                    "ths_industry": source.get("同花顺行业", "").strip(),
                    "pure_bond_ytm": source.get("纯债YTM(%)", "").strip(),
                    "ifind_doublelow": source.get("iFinD双低", "").strip(),
                    "option_value": source.get("期权价值", "").strip(),
                    "surplus_days": source.get("剩余期限(天)", "").strip(),
                    "surplus_years": source.get("剩余期限(年)", "").strip(),
                    "accum_conv_ratio": source.get("累计转股比例(%)", "").strip(),
                    "dilution_ratio": source.get("转股稀释比例(%)", "").strip(),
                    "pure_bond_value": source.get("纯债价值", "").strip(),
                    "maturity_call_price": source.get("到期赎回价", "").strip(),
                    "pe_ttm": source.get("正股PE_TTM", "").strip(),
                    "total_mv_yi": source.get("正股总市值(亿)", "").strip(),
                }
                implied_vol = _f(source.get("隐含波动率(%)", ""))
                row["implied_vol"] = implied_vol / 100 if implied_vol is not None else ""
                rows[code] = row
    except (OSError, csv.Error):
        return {}
    return rows


def _consume_quote_response(response, allowed_codes, rows, quote_codes, quote_blank_codes):
    """Merge one history response and return the number of returned tables."""
    if response.get("errorcode", 0) != 0:
        raise RuntimeError(response.get("errmsg", "unknown history error"))
    tables = response.get("tables", [])
    if not tables:
        raise RuntimeError("empty history response")
    for table_data in tables:
        table = table_data.get("table", {})
        code = _norm_code(table_data.get("thscode"))
        if code not in allowed_codes:
            continue
        close = (table.get("close") or [None])[0]
        change_pct = (table.get("changeRatio") or [None])[0]
        rows.setdefault(code, {})["latest"] = close
        rows.setdefault(code, {})["change_pct"] = change_pct
        if _present(close):
            quote_codes.add(code)
        else:
            quote_blank_codes.add(code)
    return len(tables)


def _write_quote_audit(path, payload):
    """Persist the expected-vs-returned quote set, including failed batches."""
    if not path:
        return
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--codes", required=True, help="codes.txt, one per line")
    ap.add_argument("--universe", required=True, help="cbond_universe.json for code→ucode map")
    ap.add_argument("--date", required=True, help="YYYY-MM-DD snapshot date")
    ap.add_argument("--out", required=True, help="output CSV path")
    ap.add_argument("--batch-size", type=int, default=40)
    ap.add_argument(
        "--reuse-existing", action="store_true",
        help="Reuse valid rows from the same-date output and fetch only missing/blank data",
    )
    ap.add_argument(
        "--quote-batch-size", type=int, default=40,
        help="batch size for historical close/return quote audit",
    )
    args = ap.parse_args()

    codes = list(dict.fromkeys(
        _norm_code(l) for l in open(args.codes) if _norm_code(l)
    ))
    print(f"[codes] {len(codes)}")

    # 读 universe 得到 code→ucode 映射
    uni = json.load(open(args.universe, encoding="utf-8"))
    code_to_ucode = {
        _norm_code(r.get("code")): _norm_code(r.get("ucode"))
        for r in uni.get("items", [])
        if _norm_code(r.get("code")) and _norm_code(r.get("ucode"))
    }
    ucodes = sorted(set(code_to_ucode.values()))
    print(f"[ucodes] {len(ucodes)} unique underlying stocks")

    expected_codes = set(codes)
    required_fields = [
        "latest", "conv_prem", "pure_prem", "pure_bond_value",
        "maturity_call_price",
    ]
    rows = _load_existing_rows(args.out, expected_codes) if args.reuse_existing else {}
    if args.reuse_existing:
        print(f"[reuse] same-date valuation rows={len(rows)}/{len(codes)} from {args.out}")

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

    valuation_batches_ok = 0
    basic_codes = [
        c for c in codes
        if c not in rows or any(not _present(rows[c].get(field)) for field in required_fields)
    ]
    print(f"[basic] request={len(basic_codes)} reuse={len(codes) - len(basic_codes)}")
    for b in batched(basic_codes, args.batch_size):
        try:
            r = basic_data(b, indipara)
            tables = r.get("tables", [])
            if not tables:
                raise RuntimeError("empty basic_data response")
            for t in tables:
                code = _norm_code(t.get("thscode"))
                if code not in codes:
                    continue
                tbl = t.get("table", {})
                rows[code] = {
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
            valuation_batches_ok += 1
        except Exception as e:
            print(f"[warn] valuation batch err: {e}")
        time.sleep(0.15)

    # 官方收盘价 + 涨跌幅（= Wind 口径，替换掉 realtime latest）。
    # The data-pool result above is the authoritative expected set for this
    # run.  Keep a per-code audit here instead of relying on an aggregate 95%
    # threshold: one missing active bond must be visible and block publishing.
    quote_codes = {
        c for c in codes
        if _present(rows.get(c, {}).get("latest"))
    }
    quote_blank_codes = set()
    quote_batch_errors = []
    quote_batches_recovered = []
    quote_batches_ok = 0
    quote_request_codes = [c for c in codes if c not in quote_codes]
    print(f"[quote] request={len(quote_request_codes)} reuse={len(quote_codes)}")
    for b in batched(quote_request_codes, args.quote_batch_size):
        try:
            r = history(b, "close,changeRatio", args.date, args.date)
            _consume_quote_response(r, set(codes), rows, quote_codes, quote_blank_codes)
            quote_batches_ok += 1
        except Exception as e:
            print(f"[warn] price batch err: {e}")
            # A transient SSL/proxy failure for a small missing batch should
            # not force another full-universe call. Retry this failed batch
            # one code at a time; unrecovered failures remain hard errors.
            fallback_errors = []
            for code in b:
                try:
                    single = history([code], "close,changeRatio", args.date, args.date)
                    _consume_quote_response(
                        single, {code}, rows, quote_codes, quote_blank_codes
                    )
                except Exception as single_error:
                    fallback_errors.append({"code": code, "error": str(single_error)})
                time.sleep(0.15)
            if fallback_errors:
                quote_batch_errors.append({
                    "codes": b,
                    "error": str(e),
                    "fallback_errors": fallback_errors,
                })
            else:
                quote_batches_recovered.append({"codes": b, "error": str(e)})
        time.sleep(0.12)

    missing_quote_codes = [c for c in codes if c not in quote_codes]
    quote_audit = {
        "trade_date": args.date,
        "source": "iFinD cmd_history_quotation",
        "field": "close",
        "expected_count": len(codes),
        "returned_count": len(quote_codes),
        "missing_count": len(missing_quote_codes),
        "expected_codes": codes,
        "returned_codes": sorted(quote_codes),
        "reused_codes": sorted(quote_codes - set(quote_request_codes)),
        "missing_codes": missing_quote_codes,
        "blank_codes": sorted(quote_blank_codes),
        "batch_count": (len(codes) + args.quote_batch_size - 1) // args.quote_batch_size,
        "batches_ok": quote_batches_ok,
        "batches_recovered": quote_batches_recovered,
        "batch_errors": quote_batch_errors,
    }
    audit_path = os.path.join(os.path.dirname(args.out), "quote_audit.json")
    _write_quote_audit(audit_path, quote_audit)
    print(
        f"[quote-quality] expected={len(codes)} returned={len(quote_codes)} "
        f"missing={len(missing_quote_codes)} audit={audit_path}"
    )
    if missing_quote_codes:
        print("[quote-quality] missing close codes: " + ", ".join(missing_quote_codes))

    # 正股 PE_TTM + 总市值
    stock_data = {}
    if args.reuse_existing:
        for code, row in rows.items():
            ucode = code_to_ucode.get(code, "")
            if not ucode:
                continue
            stock_data.setdefault(ucode, {
                "pe_ttm": row.get("pe_ttm"),
                "total_mv_yi": row.get("total_mv_yi"),
            })
    stock_batches_ok = 0
    stock_fields = [
        {"indicator": "ths_pe_ttm",              "indiparams": [args.date]},
        {"indicator": "ths_market_value_stock",  "indiparams": [args.date]},
    ]
    stock_request_ucodes = [
        u for u in ucodes
        if u not in stock_data
        or not _present(stock_data[u].get("pe_ttm"))
        or not _present(stock_data[u].get("total_mv_yi"))
    ]
    print(f"[stock] request={len(stock_request_ucodes)} reuse={len(ucodes) - len(stock_request_ucodes)}")
    for b in batched(stock_request_ucodes, args.batch_size):
        try:
            r = basic_data(b, stock_fields)
            tables = r.get("tables", [])
            if not tables:
                raise RuntimeError("empty stock basic_data response")
            for t in tables:
                tbl = t.get("table", {})
                pe = (tbl.get("ths_pe_ttm") or [None])[0]
                mv_raw = (tbl.get("ths_market_value_stock") or [None])[0]
                mv_yi = round(mv_raw / 1e8, 2) if mv_raw else None
                stock_data[_norm_code(t["thscode"])] = {"pe_ttm": pe, "total_mv_yi": mv_yi}
            stock_batches_ok += 1
        except Exception as e:
            print(f"[warn] stock batch err: {e}")
        time.sleep(0.15)
    print(f"[stock] PE non-null: {sum(1 for v in stock_data.values() if v.get('pe_ttm') is not None)}, "
          f"MV non-null: {sum(1 for v in stock_data.values() if v.get('total_mv_yi') is not None)}")

    # Do not write a blank snapshot when iFinD is unavailable. Previously the
    # batch exceptions were swallowed, then NULL rows were upserted into
    # DuckDB and a report was rendered before validation could stop publish.
    min_rows = max(1, int(len(codes) * 0.95))
    coverage = {
        field: sum(_present(row.get(field)) for row in rows.values())
        for field in required_fields
    }
    print(f"[quality] valuation_batches={valuation_batches_ok} "
          f"stock_batches={stock_batches_ok} rows={len(rows)}/{len(codes)} "
          f"coverage={coverage}")
    bad = [f"{field}={n}/{len(codes)}" for field, n in coverage.items() if n < min_rows]
    if missing_quote_codes or quote_batch_errors or len(rows) < min_rows or bad:
        raise RuntimeError(
            "valuation snapshot incomplete; refusing to overwrite DuckDB or publish: "
            f"rows={len(rows)}/{len(codes)}, required>={min_rows}, "
            f"missing_quotes={len(missing_quote_codes)}, "
            f"quote_batch_errors={len(quote_batch_errors)}, "
            f"bad_fields={', '.join(bad) or 'none'}"
        )

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
    if codes:
        placeholders = ",".join("?" for _ in codes)
        con.execute(
            f"DELETE FROM valuation_daily WHERE trade_date = ? AND code NOT IN ({placeholders})",
            [args.date, *codes],
        )
    con.close()
    print(f"[db] valuation_daily upserted {n} rows and cleaned out-of-universe rows (trade_date={args.date})")


if __name__ == "__main__":
    main()
