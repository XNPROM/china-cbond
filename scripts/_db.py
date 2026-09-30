"""DuckDB connection + upsert helper for cbond pipeline."""
import os
from collections import defaultdict
import duckdb

DB_PATH = os.environ.get("CBOND_DB_PATH") or os.path.join(os.path.dirname(__file__), "..", "data", "cbond.duckdb")

_schema_initialized = False


def connect(read_only=False):
    global _schema_initialized
    if read_only:
        return duckdb.connect(DB_PATH, read_only=True)
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    con = duckdb.connect(DB_PATH)
    if not _schema_initialized:
        init_schema(con)
        _schema_initialized = True
    return con


def init_schema(con):
    schema_path = os.path.join(os.path.dirname(__file__), "schema.sql")
    for stmt in open(schema_path, encoding="utf-8").read().split(";"):
        stmt = stmt.strip()
        if stmt:
            con.execute(stmt)


def upsert(con, table, rows, pk_cols):
    if not rows:
        return 0
    groups = defaultdict(list)
    for row in rows:
        if any(row.get(key) is None for key in pk_cols):
            raise ValueError(f"missing primary key for {table}")
        groups[tuple(sorted(row))].append(row)
    for cols, group in groups.items():
        col_list = ", ".join(cols)
        placeholders = ", ".join("?" * len(cols))
        conflict_cols = ", ".join(pk_cols)
        update_set = ", ".join(f"{c} = excluded.{c}" for c in cols if c not in pk_cols)
        action = f"DO UPDATE SET {update_set}" if update_set else "DO NOTHING"
        sql = (f"INSERT INTO {table} ({col_list}) VALUES ({placeholders})"
               f" ON CONFLICT ({conflict_cols}) {action}")
        con.executemany(sql, [[r[c] for c in cols] for r in group])
    return len(rows)
