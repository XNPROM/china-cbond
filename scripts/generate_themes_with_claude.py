"""Generate business rewrites and theme labels in batches via Codex CLI."""
import argparse
import json
import re
import subprocess
import sys
import tempfile
import time
from datetime import datetime
from pathlib import Path

from _db import connect, init_schema, upsert as db_upsert


def _load_whitelist(vocab_path: Path):
    text = vocab_path.read_text(encoding="utf-8")
    start = text.index("## 词表")
    end = text.index("## 打标注意事项")
    section = text[start:end].strip()
    whitelist = set()
    for line in section.splitlines():
        line = line.strip()
        m = re.match(r"- `([^`]+)`", line)
        if m:
            whitelist.add(m.group(1))
    return section, whitelist


def _load_done(out_path: Path):
    done = {}
    if not out_path.exists():
        return done
    with out_path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            row = json.loads(line)
            done[row["code"]] = row
    return done


def _log_progress(progress_path: Path, payload: dict):
    with progress_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False) + "\n")


def _build_prompt(batch, whitelist_section):
    payload = [
        {
            "code": item["code"],
            "name": item["name"],
            "ucode": item["ucode"],
            "uname": item["uname"],
            "industry": item.get("industry", ""),
            "profile": item.get("profile", ""),
        }
        for item in batch
    ]
    return f"""你是一位资深转债研究员。任务：为下列转债的正股生成 ① 主营业务改写（4-6 句，约 120-180 字，一段话，不换行） ② 高阶题材标签（1-4 个） ③ 一级行业。

【硬约束】
1. 题材标签必须从下方白名单中选，不得自创；每只最多 4 个。
2. 主营改写目标读者是专业投资人。必须涵盖：主要产品线/技术路线 + 下游应用/客户类型 + 公司在细分行业的位置（如果知道）。避免空话。
3. 优先打产品/技术级标签，不要泛化到无信息量的下游概念。
4. `industry` 输出一个简洁一级行业名称，例如：轻工制造、基础化工、电力设备、电子、计算机、机械设备、医药生物、交通运输、公用事业、环保、银行、非银金融、建筑材料、建筑装饰、汽车、通信、传媒、国防军工、农林牧渔、食品饮料、家用电器、商贸零售、有色金属、钢铁、房地产、石油石化、煤炭、综合、社会服务、美容护理、纺织服饰。
5. 如果输入里的 `industry` 为空，你需要基于主营文本自行判断；如果不确定，给出最接近的一级行业。
6. 严格输出 JSON 数组，不要加解释文字，不要包 markdown 代码块。

【题材白名单】
{whitelist_section}

【输入数据】
{json.dumps(payload, ensure_ascii=False, indent=2)}

【输出 schema】
[
  {{
    "code": "113632.SH",
    "industry": "轻工制造",
    "business_rewrite": "一段中文，不换行",
    "themes": ["特种纸-造纸", "食品饮料-大众品"]
  }}
]
"""


def _call_model(prompt, model, effort):
    project_root = Path(__file__).resolve().parent.parent
    with tempfile.NamedTemporaryFile(mode="w+", encoding="utf-8", suffix=".txt") as f:
        cmd = [
            "codex",
            "exec",
            "--full-auto",
            "--skip-git-repo-check",
            "-C",
            str(project_root),
            "-m",
            model,
            "-c",
            f"model_reasoning_effort={json.dumps(effort)}",
            "-o",
            f.name,
            "-",
        ]
        proc = subprocess.run(cmd, input=prompt, capture_output=True, text=True)
        f.seek(0)
        text = f.read().strip()
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or proc.stdout.strip() or "claude call failed")
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        start = text.find("[")
        end = text.rfind("]")
        if start >= 0 and end >= start:
            return json.loads(text[start:end + 1])
        raise


def _validate_rows(rows, expected_codes, whitelist):
    seen = set()
    if len(rows) != len(expected_codes):
        raise ValueError(f"row count mismatch: expected {len(expected_codes)}, got {len(rows)}")
    for row in rows:
        code = row.get("code")
        if code not in expected_codes:
            raise ValueError(f"unexpected code: {code}")
        if code in seen:
            raise ValueError(f"duplicate code: {code}")
        seen.add(code)
        themes = row.get("themes") or []
        if not themes or len(themes) > 4:
            raise ValueError(f"{code}: invalid theme count")
        bad = [t for t in themes if t not in whitelist]
        if bad:
            raise ValueError(f"{code}: themes not in whitelist: {bad}")
        if not (row.get("business_rewrite") or "").strip():
            raise ValueError(f"{code}: empty business_rewrite")
        if not (row.get("industry") or "").strip():
            raise ValueError(f"{code}: empty industry")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset", required=True)
    ap.add_argument("--vocab", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--trade-date", default="", help="YYYY-MM-DD; when set, each batch is upserted to DuckDB")
    ap.add_argument("--batch-size", type=int, default=40)
    ap.add_argument("--model", default="claude-sonnet-4-6")
    ap.add_argument("--effort", default="medium")
    ap.add_argument("--max-batches", type=int, default=0, help="0 means all")
    ap.add_argument("--overwrite", action="store_true", help="ignore existing out file and regenerate from scratch")
    ap.add_argument("--progress-log", default="", help="optional JSONL progress log path")
    args = ap.parse_args()

    dataset = json.load(open(args.dataset, encoding="utf-8"))
    whitelist_section, whitelist = _load_whitelist(Path(args.vocab))
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if args.overwrite and out_path.exists():
        out_path.unlink()
    done = _load_done(out_path)
    progress_path = Path(args.progress_log) if args.progress_log else out_path.with_name(out_path.stem + "_claude_progress.jsonl")
    progress_path.parent.mkdir(parents=True, exist_ok=True)
    pending = [item for item in dataset["items"] if item["code"] not in done]
    print(f"[pending] {len(pending)} of {len(dataset['items'])}")
    if not pending:
        print("[done] nothing to do")
        return

    batches = [pending[i:i + args.batch_size] for i in range(0, len(pending), args.batch_size)]
    if args.max_batches > 0:
        batches = batches[:args.max_batches]

    con = None
    if args.trade_date:
        con = connect()
        init_schema(con)

    with out_path.open("a", encoding="utf-8") as out:
        for idx, batch in enumerate(batches, start=1):
            prompt = _build_prompt(batch, whitelist_section)
            expected_codes = {item["code"] for item in batch}
            print(f"[batch {idx}/{len(batches)}] {len(batch)} items")
            last_err = None
            for attempt in range(1, 4):
                try:
                    rows = _call_model(prompt, args.model, args.effort)
                    _validate_rows(rows, expected_codes, whitelist)
                    for row in rows:
                        out.write(json.dumps(row, ensure_ascii=False) + "\n")
                    out.flush()
                    if con:
                        db_rows = [
                            {
                                "trade_date": args.trade_date,
                                "code": row["code"],
                                "theme_l1": (row.get("themes") or [""])[0] if row.get("themes") else "",
                                "all_themes_json": json.dumps(row.get("themes") or [], ensure_ascii=False),
                                "business_rewrite": row.get("business_rewrite", ""),
                                "industry": row.get("industry", ""),
                            }
                            for row in rows
                        ]
                        saved = db_upsert(con, "themes", db_rows, ["trade_date", "code"])
                    else:
                        saved = len(rows)
                    _log_progress(progress_path, {
                        "ts": datetime.utcnow().isoformat(),
                        "trade_date": args.trade_date,
                        "batch": idx,
                        "batches_total": len(batches),
                        "batch_size": len(batch),
                        "saved_rows": saved,
                        "status": "saved",
                    })
                    print(f"[batch {idx}] ok")
                    break
                except Exception as e:
                    last_err = e
                    _log_progress(progress_path, {
                        "ts": datetime.utcnow().isoformat(),
                        "trade_date": args.trade_date,
                        "batch": idx,
                        "batches_total": len(batches),
                        "attempt": attempt,
                        "status": "retry",
                        "error": str(e),
                    })
                    print(f"[batch {idx}] retry {attempt}/3: {e}", file=sys.stderr)
                    time.sleep(2 ** attempt)
            else:
                _log_progress(progress_path, {
                    "ts": datetime.utcnow().isoformat(),
                    "trade_date": args.trade_date,
                    "batch": idx,
                    "batches_total": len(batches),
                    "status": "failed",
                    "error": str(last_err),
                })
                raise RuntimeError(f"batch {idx} failed: {last_err}")

    if con:
        con.close()


if __name__ == "__main__":
    main()
