"""LLM-based main-business extractor.

Reads underlying_profile.main_business (raw iFinD 公司简介), calls Claude Sonnet 4.6
via `claude -p` (subscription auth), parses structured JSON, upserts to
underlying_business table. Skips rows whose profile_hash is unchanged.

Usage:
    python3 scripts/analyze_business_llm.py            # incremental
    python3 scripts/analyze_business_llm.py --force    # re-run all
    python3 scripts/analyze_business_llm.py --limit 5  # smoke test
    python3 scripts/analyze_business_llm.py --only 600061.SH 002594.SZ

Failed rows are appended to data/logs/business_llm_failed_<ts>.jsonl for retry.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
from _db import connect  # noqa: E402

MODEL = "claude-sonnet-4-6"

SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "main_business": {"type": "string", "description": "一句话概括主营业务，≤40字，不含公司名"},
        "products": {"type": "array", "items": {"type": "string"}, "maxItems": 6},
        "applications": {"type": "array", "items": {"type": "string"}, "maxItems": 6},
        "customers": {"type": "array", "items": {"type": "string"}, "maxItems": 6},
        "position_evidence": {"type": "string", "description": "龙头/单项冠军/专精特新等资质，否则空"},
        "revenue_structure": {
            "type": "array",
            "maxItems": 8,
            "items": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "segment": {"type": "string"},
                    "pct": {"type": "number"},
                },
                "required": ["segment"],
            },
        },
    },
    "required": ["main_business", "products", "applications", "customers", "position_evidence", "revenue_structure"],
}

USER_TMPL = """你是公司主营业务结构化抽取器。严格依据下面公司简介抽取字段，不得编造简介之外的信息。
只输出符合下面 Schema 的单个 JSON 对象，不要 markdown 围栏、不要解释文字。
字符串值内禁止使用英文双引号 \" ；如需引用专有名词请改用中文引号「」。

Schema:
{{
  "main_business":     一句中文概括，≤40字，不含公司名,
  "products":          ["string"] 核心产品/服务短词，≤6 项，按重要性,
  "applications":      ["string"] 下游应用领域，≤6 项，简介未提及给 [],
  "customers":         ["string"] 客户类型/群体，≤6 项，简介未提及给 [],
  "position_evidence": 龙头/单项冠军/专精特新/国家级/技术中心等资质短语，否则 "",
  "revenue_structure": [{{"segment":"...","pct":数值}}] 仅当简介明确给出营收占比，否则 []
}}

公司名：{uname}

公司简介：
{profile}"""


def profile_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


def _extract_json(text: str) -> dict:
    text = text.strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    m = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, re.DOTALL)
    if m:
        return json.loads(m.group(1))
    start = text.find("{")
    end = text.rfind("}")
    if 0 <= start < end:
        return json.loads(text[start : end + 1])
    raise ValueError(f"no JSON in output: {text[:200]!r}")


def _invoke_claude(prompt: str, timeout: int) -> str:
    cmd = [
        "claude", "-p",
        "--model", MODEL,
        prompt,
    ]
    proc = subprocess.run(
        cmd, capture_output=True, text=True, timeout=timeout,
        stdin=subprocess.DEVNULL,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"claude exit={proc.returncode}: {proc.stderr.strip()[:200]}")
    return proc.stdout


def call_llm(uname: str, profile: str, timeout: int = 90, retries: int = 2) -> dict:
    prompt = USER_TMPL.format(uname=uname, profile=profile)
    last_err: Exception | None = None
    for attempt in range(retries + 1):
        try:
            out = _invoke_claude(prompt, timeout)
            if not out.strip():
                raise RuntimeError("empty stdout")
            return _extract_json(out)
        except Exception as e:
            last_err = e
            if attempt < retries:
                time.sleep(2 + attempt * 3)
                continue
    raise last_err  # type: ignore[misc]


UPSERT_SQL = """
INSERT INTO underlying_business
    (ucode, uname, main_business, products_json, applications_json,
     customers_json, position_evidence, revenue_json,
     profile_hash, llm_model, updated_at)
VALUES (?,?,?,?,?,?,?,?,?,?,?)
ON CONFLICT (ucode) DO UPDATE SET
    uname=excluded.uname,
    main_business=excluded.main_business,
    products_json=excluded.products_json,
    applications_json=excluded.applications_json,
    customers_json=excluded.customers_json,
    position_evidence=excluded.position_evidence,
    revenue_json=excluded.revenue_json,
    profile_hash=excluded.profile_hash,
    llm_model=excluded.llm_model,
    updated_at=excluded.updated_at
"""


def upsert(con, ucode: str, uname: str, data: dict, p_hash: str) -> None:
    con.execute(
        UPSERT_SQL,
        [
            ucode,
            uname,
            data.get("main_business", "").strip(),
            json.dumps(data.get("products", []), ensure_ascii=False),
            json.dumps(data.get("applications", []), ensure_ascii=False),
            json.dumps(data.get("customers", []), ensure_ascii=False),
            data.get("position_evidence", "").strip(),
            json.dumps(data.get("revenue_structure", []), ensure_ascii=False),
            p_hash,
            MODEL,
            datetime.now().isoformat(timespec="seconds"),
        ],
    )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--force", action="store_true", help="re-run even if profile_hash matches")
    ap.add_argument("--limit", type=int, default=0, help="cap number processed (0=all)")
    ap.add_argument("--only", nargs="+", default=None, help="only these ucodes")
    ap.add_argument("--min-profile-len", type=int, default=15)
    ap.add_argument("--retry-failed", type=str, default=None, help="path to a failed jsonl to retry")
    args = ap.parse_args()

    con = connect()
    rows = con.execute(
        """
        SELECT up.ucode, up.uname, up.main_business, ub.profile_hash
        FROM underlying_profile up
        LEFT JOIN underlying_business ub ON up.ucode = ub.ucode
        WHERE up.main_business IS NOT NULL AND length(up.main_business) >= ?
        ORDER BY up.ucode
        """,
        [args.min_profile_len],
    ).fetchall()

    if args.only:
        keep = set(args.only)
        rows = [r for r in rows if r[0] in keep]

    if args.retry_failed:
        ids = set()
        with open(args.retry_failed) as f:
            for line in f:
                if line.strip():
                    ids.add(json.loads(line)["ucode"])
        rows = [r for r in rows if r[0] in ids]

    # Filter unchanged rows unless --force
    todo = []
    skipped = 0
    for ucode, uname, profile, prev_hash in rows:
        h = profile_hash(profile)
        if not args.force and prev_hash == h:
            skipped += 1
            continue
        todo.append((ucode, uname, profile, h))

    if args.limit:
        todo = todo[: args.limit]

    total = len(todo)
    print(f"[plan] total_profiles={len(rows)} skip_unchanged={skipped} to_process={total} model={MODEL}", flush=True)

    if total == 0:
        print("[done] nothing to do", flush=True)
        return 0

    log_dir = ROOT.parent / "data" / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    failed_path = log_dir / f"business_llm_failed_{ts}.jsonl"
    progress_path = log_dir / f"business_llm_progress_{ts}.log"

    ok = 0
    fail = 0
    t0 = time.time()
    with open(progress_path, "w") as plog, open(failed_path, "w") as flog:
        for i, (ucode, uname, profile, h) in enumerate(todo, 1):
            t_start = time.time()
            try:
                data = call_llm(uname, profile)
                upsert(con, ucode, uname, data, h)
                dt = time.time() - t_start
                ok += 1
                mb = data.get("main_business", "")[:50]
                line = f"[{i}/{total}] ✓ {ucode} {uname:<10} ({dt:5.1f}s) {mb}"
            except Exception as e:
                dt = time.time() - t_start
                fail += 1
                err = str(e).replace("\n", " ")[:200]
                line = f"[{i}/{total}] ✗ {ucode} {uname:<10} ({dt:5.1f}s) ERROR: {err}"
                flog.write(json.dumps({"ucode": ucode, "uname": uname, "error": err}, ensure_ascii=False) + "\n")
                flog.flush()
            elapsed = time.time() - t0
            eta = elapsed / i * (total - i) if i else 0
            tail = f"  | ok={ok} fail={fail} elapsed={elapsed/60:4.1f}m eta={eta/60:4.1f}m"
            print(line + tail, flush=True)
            plog.write(line + tail + "\n")
            plog.flush()

    print(f"[done] ok={ok} fail={fail} total={total} took={(time.time()-t0)/60:.1f}m", flush=True)
    print(f"[log]  progress: {progress_path}", flush=True)
    if fail:
        print(f"[log]  failed:   {failed_path}  (retry with --retry-failed)", flush=True)
    else:
        # empty failed file → remove
        failed_path.unlink(missing_ok=True)
    return 0 if fail == 0 else 2


if __name__ == "__main__":
    sys.exit(main())
