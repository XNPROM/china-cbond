"""Batch-fetch underlying stock profile (main business) + industry.

For each underlying stock code, pull:
  - ths_corp_profile (公司简介 / 主营业务)
  - industry (try iFinD basic_data first; fall back to Eastmoney public quote API)

Input is the cbond_universe.json (needs underlying stock codes).

Usage:
  python3 fetch_underlying_profile.py \\
      --universe cbond_universe.json \\
      --out cbond_underlying_profile.json
"""
import argparse, json, os, ssl, sys, time, urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(__file__))
from _ifind import basic_data, batched
from _db import connect, init_schema, upsert as db_upsert


STOCK_FIELDS = [
    {"indicator": "ths_corp_profile", "indiparams": [""]},
    {"indicator": "ths_industry", "indiparams": [""]},  # 行业 (may return empty; supplementary)
]


def _parse_updated_at(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).replace(tzinfo=None)
    except (TypeError, ValueError):
        return None


def select_refresh_codes(ucodes, existing, asof, max_age_days):
    """Return missing or expired profile codes and counts for logging."""
    cutoff = datetime.strptime(asof, "%Y-%m-%d") - timedelta(days=max_age_days)
    missing = []
    expired = []
    for code in ucodes:
        row = existing.get(code)
        updated_at = _parse_updated_at(row.get("updated_at")) if row else None
        business = (row.get("main_business") or row.get("profile") or "") if row else ""
        if not business.strip():
            missing.append(code)
        elif updated_at is None or updated_at < cutoff:
            expired.append(code)
    return missing + expired, len(missing), len(expired)


def _to_secid(code: str) -> str:
    if code.endswith(".SH"):
        return "1." + code.split(".")[0]
    if code.endswith(".SZ"):
        return "0." + code.split(".")[0]
    return ""


def _fetch_em_industry(code: str, retries: int = 3) -> str:
    secid = _to_secid(code)
    if not secid:
        return ""
    url = f"https://push2.eastmoney.com/api/qt/stock/get?secid={secid}&fields=f127"
    last_err = None
    for i in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=15, context=ssl.create_default_context()) as r:
                payload = json.loads(r.read().decode("utf-8"))
            value = ((payload or {}).get("data") or {}).get("f127")
            return value.strip() if isinstance(value, str) else ""
        except Exception as e:
            last_err = e
            time.sleep(0.5 * (2 ** i))
    print(f"[warn] eastmoney industry err: {code}: {last_err}")
    return ""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--universe", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--batch-size", type=int, default=30)
    ap.add_argument("--asof", default=datetime.now().strftime("%Y-%m-%d"))
    ap.add_argument("--max-age-days", type=int, default=30)
    args = ap.parse_args()

    uni = json.load(open(args.universe))
    items = uni["items"]
    ucodes = sorted({r["ucode"] for r in items if r.get("ucode")})
    print(f"[stocks] {len(ucodes)} unique underlying codes")

    con = connect()
    init_schema(con)
    existing_rows = con.execute(
        "SELECT ucode, uname, industry, main_business, updated_at FROM underlying_profile"
    ).fetchall()
    existing = {
        row[0]: {
            "uname": row[1] or "",
            "industry": row[2] or "",
            "profile": row[3] or "",
            "updated_at": row[4] or "",
        }
        for row in existing_rows
    }
    refresh_codes, missing_count, expired_count = select_refresh_codes(
        ucodes, existing, args.asof, args.max_age_days
    )
    reused_count = len(ucodes) - len(refresh_codes)
    print(
        f"[cache] reuse={reused_count} refresh={len(refresh_codes)} "
        f"(missing={missing_count}, expired={expired_count}, max_age={args.max_age_days}d)"
    )

    profiles = dict(existing)
    refreshed = set()
    for b in batched(refresh_codes, args.batch_size):
        try:
            r = basic_data(b, STOCK_FIELDS)
            for t in r.get("tables", []):
                tbl = t.get("table", {})
                code = t["thscode"]
                old = profiles.get(code, {})
                fetched_profile = (tbl.get("ths_corp_profile") or [""])[0]
                profile = fetched_profile or old.get("profile", "")
                industry = (tbl.get("ths_industry") or [""])[0] or old.get("industry", "")
                if profile:
                    profiles[code] = {**old, "profile": profile, "industry": industry}
                if fetched_profile:
                    refreshed.add(code)
        except Exception as e:
            print(f"[warn] profile batch err: {e}")
        time.sleep(0.18)

    missing_industry = [
        code for code in refreshed
        if not (profiles.get(code, {}).get("industry") or "").strip()
    ]
    if missing_industry:
        print(f"[industry-fallback] eastmoney for {len(missing_industry)} stocks")
        with ThreadPoolExecutor(max_workers=8) as pool:
            futs = {pool.submit(_fetch_em_industry, code): code for code in missing_industry}
            for fut in as_completed(futs):
                code = futs[fut]
                industry = fut.result() or ""
                profiles.setdefault(code, {})["industry"] = industry

    # Attach profile onto each bond record (keyed by ucode)
    out_items = []
    for r in items:
        p = profiles.get(r["ucode"], {})
        out_items.append({
            **r,
            "profile": p.get("profile", ""),
            "industry": p.get("industry", ""),
        })
    json.dump({"asof": uni["asof"], "count": len(out_items), "items": out_items},
              open(args.out, "w"), ensure_ascii=False, indent=2)
    covered = sum(1 for code in ucodes if (profiles.get(code, {}).get("profile") or "").strip())
    print(f"[done] profile coverage={covered}/{len(ucodes)} → {args.out}")

    # upsert to DuckDB
    now = datetime.now().isoformat()
    db_rows = [
        {
            "ucode": ucode,
            "uname": next((r["uname"] for r in items if r.get("ucode") == ucode), ""),
            "industry": profiles.get(ucode, {}).get("industry", ""),
            "main_business": profiles.get(ucode, {}).get("profile", ""),
            "updated_at": now,
        }
        for ucode in refreshed
    ]
    n = db_upsert(con, "underlying_profile", db_rows, ["ucode"])
    con.close()
    failed = len(refresh_codes) - len(refreshed)
    print(f"[db] underlying_profile refreshed={n} reused={reused_count} failed={failed}")
    if failed:
        print("[warn] failed refreshes kept their latest cached value when available")
    if covered < len(ucodes) * 0.95:
        raise RuntimeError(
            f"underlying profile coverage too low: {covered}/{len(ucodes)}"
        )


if __name__ == "__main__":
    main()
