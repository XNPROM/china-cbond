"""Run the daily convertible-bond refresh pipeline.

Default path fetches fresh iFinD data. For local rebuilds, skip network-heavy
steps and rebuild downstream artifacts from existing DB/raw files.

Examples:
  python3.12 scripts/daily_refresh.py --trade-date 2026-04-24
  python3.12 scripts/daily_refresh.py --trade-date 2026-04-23 --skip-fetch --skip-valuation --skip-vol
"""
import argparse
import os
import subprocess
import sys
import time
import traceback
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(__file__))
from _db import connect, init_schema, upsert as db_upsert


PY = "/usr/local/bin/python3.12"


def _now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _run_id(trade_date, step):
    return f"{trade_date}:{step}:{int(time.time() * 1000)}"


def _log_run(trade_date, step, started_at, finished_at, status, row_count=0, note=""):
    con = connect()
    init_schema(con)
    db_upsert(con, "etl_runs", [{
        "run_id": _run_id(trade_date, step),
        "trade_date": trade_date,
        "step": step,
        "started_at": started_at,
        "finished_at": finished_at,
        "row_count": row_count,
        "status": status,
        "note": note[:1000],
    }], ["run_id"])
    con.close()


def _run_step(trade_date, step, cmd, cwd, required=True):
    started = _now()
    print(f"\n[{step}] {' '.join(cmd)}")
    try:
        proc = subprocess.run(cmd, cwd=cwd, text=True, capture_output=True)
        if proc.stdout:
            print(proc.stdout, end="")
        if proc.stderr:
            print(proc.stderr, end="", file=sys.stderr)
        status = "ok" if proc.returncode == 0 else "failed"
        _log_run(trade_date, step, started, _now(), status, note=(proc.stderr or proc.stdout or ""))
        if proc.returncode != 0 and required:
            raise RuntimeError(f"{step} failed with exit code {proc.returncode}")
        return proc.returncode
    except Exception as exc:
        _log_run(trade_date, step, started, _now(), "failed", note=traceback.format_exc())
        if required:
            raise
        print(f"[warn] optional step {step} failed: {exc}")
        return 1


def _count_rows(trade_date, table):
    con = connect()
    try:
        n = con.execute(
            f"SELECT count(*) FROM {table} WHERE trade_date = ?", [trade_date]
        ).fetchone()[0]
    finally:
        con.close()
    return n


def _ensure_themes(trade_date):
    started = _now()
    con = connect()
    try:
        current = con.execute(
            "SELECT count(*) FROM themes WHERE trade_date = ?", [trade_date]
        ).fetchone()[0]
        if current:
            note = f"themes already present: {current}"
            _log_run(trade_date, "ensure_themes", started, _now(), "ok", current, note)
            print(f"[ensure_themes] {note}")
            return current

        src = con.execute(
            "SELECT max(trade_date) FROM themes WHERE trade_date < ?", [trade_date]
        ).fetchone()[0]
        if not src:
            note = "no historical themes available"
            _log_run(trade_date, "ensure_themes", started, _now(), "failed", 0, note)
            raise RuntimeError(note)

        rows = con.execute(
            """
            SELECT ?, code, theme_l1, all_themes_json, business_rewrite, industry
              FROM themes
             WHERE trade_date = ?
            """,
            [trade_date, src],
        ).fetchall()
        con.executemany(
            """
            INSERT INTO themes (trade_date, code, theme_l1, all_themes_json, business_rewrite, industry)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT (trade_date, code) DO UPDATE SET
              theme_l1 = excluded.theme_l1,
              all_themes_json = excluded.all_themes_json,
              business_rewrite = excluded.business_rewrite,
              industry = excluded.industry
            """,
            rows,
        )
        note = f"copied {len(rows)} themes from {src}"
        _log_run(trade_date, "ensure_themes", started, _now(), "ok", len(rows), note)
        print(f"[ensure_themes] {note}")
        return len(rows)
    finally:
        con.close()


def _latest_codes_file(cwd, trade_date):
    today = os.path.join(cwd, "data", "raw", f"asof={trade_date}", "cbond_codes.txt")
    if os.path.exists(today):
        return today
    fallback = os.path.join(cwd, "data", "raw", "asof=2026-04-20", "cbond_codes.txt")
    return fallback


def _latest_universe_file(cwd, trade_date):
    today = os.path.join(cwd, "data", "raw", f"asof={trade_date}", "cbond_universe.json")
    if os.path.exists(today):
        return today
    fallback = os.path.join(cwd, "data", "raw", "asof=2026-04-20", "cbond_universe.json")
    return fallback


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--trade-date", required=True, help="YYYY-MM-DD")
    ap.add_argument("--skip-fetch", action="store_true", help="Skip cbond universe fetch")
    ap.add_argument("--skip-valuation", action="store_true", help="Skip valuation fetch and refresh")
    ap.add_argument("--skip-vol", action="store_true", help="Skip volatility fetch")
    ap.add_argument("--skip-backtest", action="store_true", help="Skip backtest computation")
    ap.add_argument("--backtest-days", type=int, default=90, help="Calendar days to look back for backtest")
    ap.add_argument("--allow-validate-warnings", action="store_true")
    args = ap.parse_args()

    cwd = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    trade_date = args.trade_date
    raw_dir = os.path.join(cwd, "data", "raw", f"asof={trade_date}")
    report_dir = os.path.join(cwd, "reports", trade_date)
    os.makedirs(raw_dir, exist_ok=True)
    os.makedirs(report_dir, exist_ok=True)

    codes = _latest_codes_file(cwd, trade_date)
    universe = _latest_universe_file(cwd, trade_date)
    dataset = os.path.join(raw_dir, "dataset.json")
    valuation = os.path.join(raw_dir, "valuation.csv")
    vol = os.path.join(raw_dir, "vol_20d.csv")
    overview_md = os.path.join(report_dir, "cbond_overview.md")
    overview_html = os.path.join(report_dir, "cbond_overview.html")
    strategy_jsonl = os.path.join(raw_dir, "strategy_picks.jsonl")
    strategy_md = os.path.join(report_dir, "cbond_strategy.md")
    # backtest_weekly.py writes to asof=YYYYMMDD (no dashes), matching upstream
    backtest_json = os.path.join(cwd, "data", "raw", f"asof={trade_date.replace('-', '')}", "backtest_weekly.json")

    con = connect()
    init_schema(con)
    con.close()

    if not args.skip_fetch:
        _run_step(trade_date, "fetch_cb_universe", [
            PY, "scripts/fetch_cb_universe.py",
            "--date", trade_date,
            "--out-json", os.path.join(raw_dir, "cbond_universe.json"),
            "--out-csv", os.path.join(raw_dir, "cbond_universe.csv"),
            "--out-codes", os.path.join(raw_dir, "cbond_codes.txt"),
        ], cwd)
        codes = _latest_codes_file(cwd, trade_date)
        universe = _latest_universe_file(cwd, trade_date)

    if not args.skip_valuation:
        _run_step(trade_date, "fetch_valuation", [
            PY, "scripts/fetch_valuation.py",
            "--codes", codes,
            "--universe", universe,
            "--date", trade_date,
            "--out", valuation,
        ], cwd)
        _run_step(trade_date, "refresh_data", [
            PY, "scripts/refresh_data.py",
            "--trade-date", trade_date,
            "--fix",
        ], cwd, required=False)

    if not args.skip_vol:
        _run_step(trade_date, "compute_volatility", [
            PY, "scripts/compute_volatility.py",
            "--universe", universe,
            "--asof", trade_date,
            "--lookback-days", "45",
            "--out", vol,
        ], cwd)

    # Assemble once before BS, update dataset in-place with BS, then continue.
    _run_step(trade_date, "assemble_dataset", [
        PY, "scripts/assemble_dataset.py",
        "--trade-date", trade_date,
        "--out", dataset,
    ], cwd)

    _run_step(trade_date, "bs_pricing", [
        PY, "scripts/bs_pricing.py",
        "--dataset", dataset,
        "--trade-date", trade_date,
    ], cwd)

    _run_step(trade_date, "strategy_score", [
        PY, "scripts/strategy_score.py",
        "--dataset", dataset,
        "--trade-date", trade_date,
        "--out", strategy_jsonl,
    ], cwd)

    _ensure_themes(trade_date)

    _run_step(trade_date, "build_overview_md", [
        PY, "scripts/build_overview_md.py",
        "--dataset", dataset,
        "--trade-date", trade_date,
        "--out", overview_md,
        "--title-date", trade_date,
    ], cwd)

    if not args.skip_backtest:
        from datetime import datetime, timedelta
        bt_end = trade_date
        bt_start = (datetime.strptime(trade_date, "%Y-%m-%d") - timedelta(days=args.backtest_days)).strftime("%Y-%m-%d")
        _run_step(trade_date, "backtest_weekly", [
            PY, "scripts/backtest_weekly.py",
            "--start-date", bt_start,
            "--end-date", bt_end,
            "--from-db",   # data is already in DB after valuation steps
        ], cwd, required=False)

    render_cmd = [
        PY, "scripts/render_html.py",
        "--in", overview_md,
        "--out", overview_html,
        "--title", f"可转债概览 · {trade_date}",
        "--trade-date", trade_date,
    ]
    if os.path.exists(backtest_json):
        render_cmd += ["--backtest", backtest_json]
    render_cmd += ["--update-index"]
    _run_step(trade_date, "render_html", render_cmd, cwd)

    validate_cmd = [
        PY, "scripts/validate_snapshot.py",
        "--trade-date", trade_date,
        "--dataset", dataset,
    ]
    if not args.allow_validate_warnings:
        validate_cmd.append("--strict")
    _run_step(trade_date, "validate_snapshot", validate_cmd, cwd)

    print(f"\n[done] refreshed {trade_date}")
    print(f"  dataset: {dataset}")
    print(f"  overview: {overview_html}")
    print(f"  strategy picks: {_count_rows(trade_date, 'strategy_picks')}")


if __name__ == "__main__":
    main()
