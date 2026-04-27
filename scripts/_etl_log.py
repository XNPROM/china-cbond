"""ETL run logging utilities.

Usage in pipeline scripts:
    from _etl_log import etl_log

    with etl_log(trade_date, "fetch_valuation") as log:
        # ... do work ...
        log.set_row_count(n_rows)
        # on exception, status is auto-set to "error"
"""
import uuid
from datetime import datetime
from contextlib import contextmanager


@contextmanager
def etl_log(con, trade_date, step, note=""):
    """Context manager for logging ETL step runs to etl_runs table.

    Args:
        con: DuckDB connection
        trade_date: YYYY-MM-DD trade date
        step: step name (e.g. "fetch_valuation", "bs_pricing")
        note: optional note/message

    Usage:
        with etl_log(con, "2026-04-24", "fetch_valuation") as log:
            # ... processing ...
            log.set_row_count(335)
    """
    run_id = f"{trade_date}_{step}_{uuid.uuid4().hex[:8]}"
    started_at = datetime.now().isoformat()

    con.execute(
        "INSERT INTO etl_runs (run_id, trade_date, step, started_at, status) VALUES (?, ?, ?, ?, 'running')",
        [run_id, trade_date, step, started_at]
    )

    log = ETLLogEntry(con, run_id)
    try:
        yield log
        con.execute(
            "UPDATE etl_runs SET finished_at = ?, status = ?, row_count = ?, note = ? WHERE run_id = ?",
            [datetime.now().isoformat(), "success", log.row_count, note, run_id]
        )
    except Exception as e:
        con.execute(
            "UPDATE etl_runs SET finished_at = ?, status = ?, note = ? WHERE run_id = ?",
            [datetime.now().isoformat(), "error", str(e), run_id]
        )
        raise


class ETLLogEntry:
    def __init__(self, con, run_id):
        self.con = con
        self.run_id = run_id
        self.row_count = 0

    def set_row_count(self, n):
        self.row_count = n
