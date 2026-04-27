"""Fetch full CB universe from iFinD data_pool API (p05479).

Replaces discover_universe.py (seed+range scan) with a single authoritative
API call that returns ALL tradable convertible bonds plus 申万 L1/L2/L3
industry, ratings, conv price, prospectus, etc.

Condition decoding:
  jyzt=2       交易状态=正常交易
  sfdb=1       是否担保（筛选位，实际不过滤）
  jysc=1       交易所市场
  sszt=213001  证券类型=可转债
  edate=       截止日期 (YYYYMMDD)
  gnfl=0       公募/私募

Usage:
  python3 scripts/fetch_cb_universe.py --date 2026-04-20
"""
import argparse
import csv
import json
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(__file__))
from _db import connect, init_schema, upsert as db_upsert
from _ifind import ths_dr


FIELDS = (
    "jydm:Y,jydm_mc:Y,"
    + ",".join(f"p05479_f{i:03d}:Y" for i in [
        1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
        18, 19, 20, 21, 22, 25, 26, 27, 28, 29, 30, 31, 32, 33,
        35, 36, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55
    ])
)


def _safe(arr, idx, default=""):
    if idx >= len(arr):
        return default
    v = arr[idx]
    return v if v and v != "--" else default


def _date_norm(s):
    """Normalize 2030/04/01 / 2030-04-01 → 20300401."""
    if not s:
        return ""
    return s.replace("/", "").replace("-", "")


def fetch_universe(date_ymd):
    condition = f"jyzt=2;sfdb=1;jysc=1;sszt=213001;edate={date_ymd};gnfl=0"
    r = ths_dr("p05479", condition, FIELDS)

    if r.get("errorcode") != 0:
        raise RuntimeError(f"iFinD data_pool error: {r.get('errmsg', 'unknown')}")

    tables = r.get("tables", [])
    if not tables:
        raise RuntimeError("No data returned from iFinD data_pool")

    tbl = tables[0]["table"]
    n = len(tbl.get("jydm", []))
    print(f"[fetch] {n} bonds from data_pool (edate={date_ymd})")

    bonds = []
    for i in range(n):
        bonds.append({
            "code":          _safe(tbl.get("jydm", []), i),
            "name":          _safe(tbl.get("jydm_mc", []), i),
            "ucode":         _safe(tbl.get("p05479_f021", []), i),   # 正股代码
            "uname":         _safe(tbl.get("p05479_f022", []), i),   # 正股简称
            "conv_price":    _safe(tbl.get("p05479_f009", []), i),   # 转股价
            "issue_date":    _safe(tbl.get("p05479_f007", []), i),   # 发行日
            "maturity":      _safe(tbl.get("p05479_f008", []), i),   # 到期日
            "listed":        _safe(tbl.get("p05479_f019", []), i),   # 上市日
            "face_value":    _safe(tbl.get("p05479_f001", []), i),   # 面值
            "coupon":        _safe(tbl.get("p05479_f005", []), i),   # 票面利率
            "rating_issuer": _safe(tbl.get("p05479_f025", []), i),   # 主体评级
            "rating_bond":   _safe(tbl.get("p05479_f026", []), i),   # 债项评级
            "sw_l1":         _safe(tbl.get("p05479_f041", []), i),   # 申万一级
            "sw_l2":         _safe(tbl.get("p05479_f042", []), i),   # 申万二级
            "sw_l3":         _safe(tbl.get("p05479_f043", []), i),   # 申万三级
            "guarantee":     _safe(tbl.get("p05479_f004", []), i),   # 是否担保
            "prospectus":    _safe(tbl.get("p05479_f036", []), i),   # 募集说明摘要
        })
    return bonds


def save_to_db(bonds, date_ymd):
    con = connect()
    init_schema(con)

    now = datetime.utcnow().isoformat()
    universe_rows = [{
        "code":          b["code"],
        "name":          b["name"],
        "ucode":         b["ucode"],
        "uname":         b["uname"],
        "list_date":     _date_norm(b["listed"]),
        "maturity_date": _date_norm(b["maturity"]),
        "updated_at":    now,
    } for b in bonds]
    n_u = db_upsert(con, "universe", universe_rows, ["code"])
    print(f"[db] universe upserted {n_u} rows")

    # themes table: 申万行业作为一级主题兜底，正式题材仍由 generate_themes_* 覆写
    theme_rows = [{
        "trade_date": f"{date_ymd[:4]}-{date_ymd[4:6]}-{date_ymd[6:]}",
        "code":       b["code"],
        "theme_l1":   b["sw_l2"] or b["sw_l1"] or "其他综合",
        "all_themes_json": json.dumps(
            [t for t in [b["sw_l1"], b["sw_l2"], b["sw_l3"]] if t],
            ensure_ascii=False
        ),
        "business_rewrite": (b["prospectus"] or "")[:200],
        "industry": b["sw_l1"] or "",
    } for b in bonds]
    n_t = db_upsert(con, "themes", theme_rows, ["trade_date", "code"])
    print(f"[db] themes(申万兜底) upserted {n_t} rows")

    con.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True, help="YYYY-MM-DD or YYYYMMDD")
    ap.add_argument("--out-json",  default="", help="output JSON path")
    ap.add_argument("--out-csv",   default="", help="output CSV path")
    ap.add_argument("--out-codes", default="", help="output codes.txt path")
    ap.add_argument("--skip-db",   action="store_true")
    args = ap.parse_args()

    date_ymd = args.date.replace("-", "")
    bonds = fetch_universe(date_ymd)

    asof = f"{date_ymd[:4]}-{date_ymd[4:6]}-{date_ymd[6:]}"
    base_dir = f"data/raw/asof={asof}"

    json_path = args.out_json or f"{base_dir}/cbond_universe.json"
    os.makedirs(os.path.dirname(json_path), exist_ok=True)
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump({"asof": asof, "count": len(bonds), "items": bonds},
                  f, ensure_ascii=False, indent=2)
    print(f"[json] → {json_path}")

    csv_path = args.out_csv or f"{base_dir}/cbond_universe.csv"
    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["代码", "名称", "正股代码", "正股简称",
                    "申万一级", "申万二级", "申万三级",
                    "转股价", "到期日", "上市日",
                    "主体评级", "债项评级", "是否担保"])
        for b in bonds:
            w.writerow([b["code"], b["name"], b["ucode"], b["uname"],
                        b["sw_l1"], b["sw_l2"], b["sw_l3"],
                        b["conv_price"], b["maturity"], b["listed"],
                        b["rating_issuer"], b["rating_bond"], b["guarantee"]])
    print(f"[csv]  → {csv_path}")

    codes_path = args.out_codes or f"{base_dir}/cbond_codes.txt"
    with open(codes_path, "w") as f:
        for b in bonds:
            f.write(b["code"] + "\n")
    print(f"[txt]  → {codes_path}")

    if not args.skip_db:
        save_to_db(bonds, date_ymd)

    # 申万一级分布
    sw1 = {}
    for b in bonds:
        k = b["sw_l1"] or "未知"
        sw1[k] = sw1.get(k, 0) + 1
    print(f"\n=== 申万一级行业分布 ({len(bonds)} 只) ===")
    for k, c in sorted(sw1.items(), key=lambda x: -x[1]):
        print(f"  {k}: {c}")

    print(f"\n[done] {len(bonds)} bonds for {asof}")


if __name__ == "__main__":
    main()
