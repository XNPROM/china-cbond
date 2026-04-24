"""Idempotent DB initialiser — safe to run multiple times."""
import os, sys
sys.path.insert(0, os.path.dirname(__file__))
from _db import connect, init_schema, DB_PATH

if __name__ == "__main__":
    con = connect()
    init_schema(con)
    con.close()
    print(f"[done] schema initialised → {DB_PATH}")
