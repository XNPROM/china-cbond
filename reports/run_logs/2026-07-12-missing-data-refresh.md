# Missing data refresh progress

- Audited recent daily snapshots and found `2026-07-09` missing valuation,
  dataset, and report outputs; `2026-07-07` had database data but no report.
- A direct `p05479` fetch for `2026-07-09` failed explicitly because the
  monthly iFinD data-pool quota was exhausted. No universe rows were deleted.
- Seeded `2026-07-09` from the validated 315-code `2026-07-10` universe and
  normalized it through `fetch_cb_universe.py --recover-existing`.
- Refreshed `2026-07-09`: 315 valuation rows, 312 volatility rows, 311 listed
  bonds in the assembled dataset, and 70 strategy picks. Strict validation
  passed and the HTML report was generated.
- Rebuilding `2026-07-07` exposed a `None` Delta formatting failure in
  `strategy_score.py`. Added an `N/A` display fallback without changing
  classification, ranking, or selection logic.
- Rebuilt `2026-07-07`: 299 valuation rows, 297 volatility rows, 296 dataset
  bonds, and 70 strategy picks. Strict validation passed and the report was
  generated.
- Verified every weekday from `2026-06-18` through `2026-07-10` now has both
  a valuation snapshot and an HTML report.
