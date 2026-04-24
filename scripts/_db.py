"""DuckDB connection + upsert helper for cbond pipeline."""
import os
import duckdb

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "cbond.duckdb")

_schema_initialized = False


def connect():
    global _schema_initialized
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
    cols = list(rows[0].keys())
    col_list = ", ".join(cols)
    placeholders = ", ".join("?" * len(cols))
    conflict_cols = ", ".join(pk_cols)
    update_set = ", ".join(f"{c} = excluded.{c}" for c in cols if c not in pk_cols)
    sql = (
        f"INSERT INTO {table} ({col_list}) VALUES ({placeholders})"
        f" ON CONFLICT ({conflict_cols}) DO UPDATE SET {update_set}"
    )
    data = [[r.get(c) for c in cols] for r in rows]
    con.executemany(sql, data)
    return len(data)
