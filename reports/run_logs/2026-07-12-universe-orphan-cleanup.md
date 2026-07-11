# Universe orphan cleanup progress

- Confirmed `fetch_cb_universe.py::save_to_db()` only upserted rows and never removed codes absent from the current authoritative snapshot.
- Compared `data/cbond.duckdb` with `data/raw/asof=2026-07-10/cbond_universe.json`: 357 database rows versus 315 snapshot rows, leaving 42 orphans.
- Added an exact-snapshot orphan cleanup helper with an empty-snapshot safety guard.
- Wrapped universe upsert, orphan deletion, and same-day theme fallback upsert in one transaction.
- Added regression tests for deletion, duplicate active codes, and empty-snapshot rejection.
- Applied the latest snapshot locally: deleted 42 orphan rows and reduced `universe` from 357 to 315 rows.
- Verified the database and snapshot code sets are identical (`orphans=[]`, `missing=[]`).
- Full test suite passed: 49 tests.
