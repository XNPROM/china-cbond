# Archived Scripts

These scripts are superseded or unused. Kept for reference only.

| Script | Replaced By | Reason |
|---|---|---|
| `discover_universe.py` | `fetch_cb_universe.py` | iFinD `data_pool` p05479 endpoint returns all listed CBs in one call; no need for seed+probe |
| `generate_themes_with_claude.py` | `generate_themes_direct.py` | Keyword rules + Shenwan industry from DB is faster and deterministic; Claude path requires external API |
| `load_themes.py` | `generate_themes_direct.py` | Themes are now upserted directly during generation; no separate load step |
| `sample_one.py` | — | Debug helper for testing single-bond iFinD queries; not part of the pipeline |
