"""LLM-based theme classification via Claude CLI subprocess.

Classifies convertible bond underlying companies into investment themes using
Claude. Results are cached per company code with a configurable TTL (default
7 days), so daily runs are cheap — only new or stale bonds call the LLM.

Usage:
  # First run or weekly refresh (only calls LLM for stale bonds):
  python3.12 scripts/generate_themes_llm.py --dataset data/raw/asof=2026-04-27/dataset.json --trade-date 2026-04-27

  # Force reclassify everything:
  python3.12 scripts/generate_themes_llm.py --dataset ... --trade-date ... --force
"""
import argparse
import json
import re
import subprocess
import time
from datetime import datetime, timezone, timedelta

import sys, os
sys.path.insert(0, os.path.dirname(__file__))
from _db import connect, init_schema, upsert as db_upsert


CACHE_TTL_DAYS = 7
BATCH_SIZE = 10
INTER_BATCH_DELAY = 3.0   # seconds between batches to avoid CLI rate limits
MAX_RETRIES = 2            # retries per batch on transient failure

PROMPT = """\
你是专业的A股可转债投资研究员。请为以下 {n} 家上市公司各提炼投资题材标签。

{companies}

对每家公司输出：
- theme_l1：最核心的一个投资题材（用A股投资者熟悉的语言，例如"AI算力"、"创新药"、"锂电材料"、"光伏"、"美妆个护"）
- all_themes：最多3个题材标签，从最相关到次相关排列（JSON数组）
- business_rewrite：一句话描述核心主营业务（≤40字，突出主要产品/服务）

严格只输出一个 JSON 数组，不要任何说明文字：
[{{"code":"...","theme_l1":"...","all_themes":["..."],"business_rewrite":"..."}}, ...]"""


def _ensure_cache_table(con):
    con.execute("""
        CREATE TABLE IF NOT EXISTS llm_theme_cache (
            code        VARCHAR PRIMARY KEY,
            updated_at  VARCHAR NOT NULL,
            theme_l1    VARCHAR,
            all_themes_json VARCHAR,
            business_rewrite VARCHAR,
            industry    VARCHAR
        )
    """)


def _stale_codes(codes: list[str], con, ttl_days: int) -> list[str]:
    if not codes:
        return []
    cutoff = (datetime.now(timezone.utc) - timedelta(days=ttl_days)).isoformat()
    rows = con.execute(
        "SELECT code FROM llm_theme_cache WHERE code IN ({}) AND updated_at >= ?".format(
            ",".join("?" * len(codes))
        ),
        codes + [cutoff],
    ).fetchall()
    fresh = {r[0] for r in rows}
    return [c for c in codes if c not in fresh]


def _call_claude(prompt: str, timeout: int = 90) -> str:
    proc = subprocess.run(
        ["claude", "-p", prompt, "--output-format", "json"],
        capture_output=True, text=True, timeout=timeout,
    )
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr[:500] or f"exit {proc.returncode}")
    outer = json.loads(proc.stdout)
    result = outer.get("result") or ""
    if not result:
        raise RuntimeError(f"empty result (is_error={outer.get('is_error')})")
    return result


def _extract_json_array(text: str) -> list:
    # Strip leading prose (e.g. "cccc.\n\n") and find the JSON array
    m = re.search(r"\[.*\]", text, re.DOTALL)
    if not m:
        raise ValueError(f"No JSON array in response: {text[:300]}")
    return json.loads(m.group())


def _process_batch(batch: list[dict], con, trade_date: str):
    companies_text = "\n".join(
        f"{i+1}. code={b['code']} 公司={b['uname']} 申万行业={b.get('industry','未知')}\n"
        f"   主营业务：{(b.get('profile') or b['uname'])[:300]}"
        for i, b in enumerate(batch)
    )
    prompt = PROMPT.format(n=len(batch), companies=companies_text)

    raw = _call_claude(prompt)
    results = _extract_json_array(raw)

    # Build a lookup by code for easy join
    code_to_item = {b["code"]: b for b in batch}
    now = datetime.now(timezone.utc).isoformat()
    cache_rows = []
    for r in results:
        code = r.get("code", "")
        item = code_to_item.get(code, {})
        themes = r.get("all_themes") or [r.get("theme_l1", "其他综合")]
        cache_rows.append({
            "code": code,
            "updated_at": now,
            "theme_l1": r.get("theme_l1") or (themes[0] if themes else "其他综合"),
            "all_themes_json": json.dumps(themes, ensure_ascii=False),
            "business_rewrite": r.get("business_rewrite", ""),
            "industry": item.get("industry", ""),
        })

    db_upsert(con, "llm_theme_cache", cache_rows, ["code"])
    return len(cache_rows)


def apply_cache_to_themes(con, trade_date: str) -> int:
    """Copy llm_theme_cache into the themes table for a specific trade_date."""
    rows = con.execute("""
        SELECT c.code, c.theme_l1, c.all_themes_json, c.business_rewrite, c.industry
        FROM llm_theme_cache c
    """).fetchall()

    theme_rows = [
        {
            "trade_date": trade_date,
            "code": r[0],
            "theme_l1": r[1],
            "all_themes_json": r[2],
            "business_rewrite": r[3],
            "industry": r[4],
        }
        for r in rows
    ]
    if theme_rows:
        db_upsert(con, "themes", theme_rows, ["trade_date", "code"])
    return len(theme_rows)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset", required=True, help="Path to dataset.json")
    ap.add_argument("--trade-date", required=True, help="YYYY-MM-DD")
    ap.add_argument("--force", action="store_true", help="Reclassify all bonds, ignore cache")
    ap.add_argument("--ttl-days", type=int, default=CACHE_TTL_DAYS)
    ap.add_argument("--batch-size", type=int, default=BATCH_SIZE)
    args = ap.parse_args()

    con = connect()
    init_schema(con)
    _ensure_cache_table(con)

    dataset = json.load(open(args.dataset, encoding="utf-8"))
    items = dataset["items"]
    all_codes = [it["code"] for it in items]

    if args.force:
        to_classify = items
    else:
        stale = set(_stale_codes(all_codes, con, args.ttl_days))
        to_classify = [it for it in items if it["code"] in stale]

    print(f"[llm_themes] {len(items)} bonds total — {len(to_classify)} need LLM classification")

    failed = 0
    for i in range(0, len(to_classify), args.batch_size):
        batch = to_classify[i : i + args.batch_size]
        batch_num = i // args.batch_size + 1
        total_batches = (len(to_classify) + args.batch_size - 1) // args.batch_size
        for attempt in range(1, MAX_RETRIES + 2):
            try:
                n = _process_batch(batch, con, args.trade_date)
                print(f"[llm_themes] batch {batch_num}/{total_batches}: saved {n}")
                break
            except Exception as exc:
                if attempt <= MAX_RETRIES:
                    print(f"[retry {attempt}/{MAX_RETRIES}] batch {batch_num}: {exc} — waiting 5s")
                    time.sleep(5)
                else:
                    failed += 1
                    codes = [b["code"] for b in batch]
                    print(f"[warn] batch {batch_num}/{total_batches} failed ({codes}): {exc}")

        if i + args.batch_size < len(to_classify):
            time.sleep(INTER_BATCH_DELAY)

    # Apply everything in cache to this trade_date's themes table
    n_applied = apply_cache_to_themes(con, args.trade_date)
    print(f"[llm_themes] applied {n_applied} theme rows to themes table for {args.trade_date}")
    if failed:
        print(f"[llm_themes] {failed} batch(es) failed — those bonds will fall back to rule-based or previous-day themes")

    con.close()


if __name__ == "__main__":
    main()
