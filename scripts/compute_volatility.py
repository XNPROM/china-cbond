"""Compute 20-day annualized volatility for each underlying stock.

vol_annual = std(log(close_t / close_{t-1})) × sqrt(252)  over last N=20 trading days.

For stocks with <20 sessions in the window, uses what's available and flags N.

Usage:
  python3 compute_volatility.py \\
      --universe cbond_universe.json \\
      --asof 2026-04-20 \\
      --lookback-days 45 \\
      --out cbond_vol_20d.csv
"""
import argparse, csv, json, math, os, sys, time
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(__file__))
from _ifind import history, batched
from _db import connect, init_schema, upsert as db_upsert


def _annualized_vol(closes):
    if len(closes) < 2:
        return None, 0
    rets = [math.log(closes[i] / closes[i - 1]) for i in range(1, len(closes))]
    last = rets[-20:]
    if len(last) < 2:
        return None, len(last)
    m = sum(last) / len(last)
    var = sum((x - m) ** 2 for x in last) / (len(last) - 1)
    return (var ** 0.5) * (252 ** 0.5), len(last)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--universe", required=True)
    ap.add_argument("--asof", required=True, help="YYYY-MM-DD")
    ap.add_argument("--lookback-days", type=int, default=45,
                    help="calendar days to pull (need ≥ 30 to get 20 trading days)")
    ap.add_argument("--out", required=True)
    ap.add_argument("--batch-size", type=int, default=15,
                    help="history request batch size (keep small; server returns per-code array)")
    args = ap.parse_args()

    asof = datetime.strptime(args.asof, "%Y-%m-%d").date()
    start = (asof - timedelta(days=args.lookback_days)).isoformat()
    end = args.asof

    uni = json.load(open(args.universe, encoding="utf-8"))
    ucodes = sorted({r["ucode"] for r in uni["items"] if r.get("ucode")})
    print(f"[stocks] {len(ucodes)} underlying; window {start}..{end}")

    vols = {}
    for b in batched(ucodes, args.batch_size):
        try:
            r = history(b, "close", start, end, {"Interval": "D", "Fill": "Omit"})
            for t in r.get("tables", []):
                code = t["thscode"]
                tbl = t.get("table", {})
                closes = tbl.get("close") or []
                closes = [c for c in closes if isinstance(c, (int, float)) and c > 0]
                vol, n = _annualized_vol(closes)
                vols[code] = {"vol": vol, "n": n}
        except Exception as e:
            print(f"[warn] history batch err: {e}")
        time.sleep(0.2)

    # emit per-bond (dup volatility across bonds sharing ucode)
    with open(args.out, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["转债代码", "正股代码", "正股简称", "20日年化波动率(%)", "样本数"])
        for r in uni["items"]:
            v = vols.get(r["ucode"], {})
            vol = v.get("vol")
            w.writerow([r["code"], r["ucode"], r["uname"],
                        f"{vol*100:.2f}" if vol is not None else "",
                        v.get("n", 0)])
    print(f"[done] vol for {sum(1 for v in vols.values() if v.get('vol') is not None)}"
          f"/{len(ucodes)} stocks → {args.out}")

    # upsert to DuckDB (by ucode)
    db_rows = [
        {
            "trade_date": args.asof,
            "ucode": ucode,
            "vol_20d_pct": vols[ucode].get("vol"),
            "n_samples": vols[ucode].get("n", 0),
        }
        for ucode in vols
    ]
    con = connect()
    init_schema(con)
    n = db_upsert(con, "vol_daily", db_rows, ["trade_date", "ucode"])
    con.close()
    print(f"[db] vol_daily upserted {n} rows (trade_date={args.asof})")


if __name__ == "__main__":
    main()
