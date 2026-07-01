#!/bin/bash
# Auto daily refresh + git push wrapper.
# Usage: auto_daily.sh [YYYY-MM-DD]
# - Defaults to today's date.
# - Runs daily_refresh.py, then commits & pushes the generated reports/<date>/
#   so the GitHub Pages workflow (deploy-pages.yml on developer-1) can rebuild.
set -u

REPO_ROOT="/Users/apple/cbond_monitor"
cd "$REPO_ROOT" || { echo "[fatal] cannot cd $REPO_ROOT"; exit 1; }

# Ensure PATH covers git / ssh under launchd's minimal environment.
export PATH="/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin:$PATH"
export GIT_SSH_COMMAND="ssh -i $HOME/.ssh/id_ed25519 -o StrictHostKeyChecking=accept-new"

DATE="${1:-$(date +%Y-%m-%d)}"
LOG_DIR="$REPO_ROOT/data/logs"
mkdir -p "$LOG_DIR"
LOG="$LOG_DIR/auto_${DATE}.log"

# Skip if today's report already exists with real data (prevents duplicate runs on wake/boot)
# HTML must be > 500KB to count as a valid report; smaller files = failed runs that need retry
if [ -z "${1:-}" ] && [ -f "reports/${DATE}/cbond_overview.html" ]; then
  SIZE=$(stat -f%z "reports/${DATE}/cbond_overview.html" 2>/dev/null || echo 0)
  if [ "$SIZE" -gt 524288 ]; then
    echo "[skip] $(date -Iseconds) auto_daily for $DATE — report already exists (${SIZE} bytes)" >> "$LOG"
    exit 0
  fi
  echo "[retry] $(date -Iseconds) auto_daily for $DATE — previous report too small (${SIZE} bytes), re-running" >> "$LOG"
fi

{
  echo "=========================================="
  echo "[start] $(date -Iseconds) auto_daily for $DATE"
  echo "=========================================="
} >> "$LOG"

# Skip weekends — A-share is closed; iFinD will return empty data.
DOW=$(date -j -f "%Y-%m-%d" "$DATE" +%u 2>/dev/null || date -d "$DATE" +%u)
if [ "$DOW" = "6" ] || [ "$DOW" = "7" ]; then
  echo "[skip] $DATE is weekend (DOW=$DOW)" >> "$LOG"
  exit 0
fi

# Run the pipeline with whole-run retry. The iFinD HTTP API is intermittently
# flaky: batch calls time out for minutes at a stretch, leaving bond-side fields
# NULL and tripping strict validation. The inner 3-retry/3.5s backoff in
# _ifind._post cannot ride out a multi-minute outage, so we retry the entire
# pipeline here, with a salvage pass in between.
PY=/usr/local/bin/python3.12
ATTEMPTS="${AUTO_DAILY_ATTEMPTS:-3}"
WAIT_SECONDS="${AUTO_DAILY_WAIT:-300}"
success=0

for attempt in $(seq 1 "$ATTEMPTS"); do
  echo "[attempt $attempt/$ATTEMPTS] daily_refresh for $DATE" >> "$LOG"

  # Primary path: full pipeline, strict validation (highest quality bar).
  if "$PY" scripts/daily_refresh.py --trade-date "$DATE" >> "$LOG" 2>&1; then
    success=1
    break
  fi
  echo "[warn] attempt $attempt: daily_refresh failed" >> "$LOG"

  # Salvage path: force re-fetch stale iFinD bond fields, then rebuild downstream
  # only (no re-fetch), tolerating residual warnings (e.g. partial stock PE/MV).
  echo "[recover] refresh_data --fix --force + downstream rebuild" >> "$LOG"
  "$PY" scripts/refresh_data.py --trade-date "$DATE" --fix --force >> "$LOG" 2>&1 || true
  if "$PY" scripts/daily_refresh.py --trade-date "$DATE" \
        --skip-fetch --skip-valuation --skip-vol \
        --allow-validate-warnings >> "$LOG" 2>&1; then
    echo "[ok] recovered $DATE via salvage pass" >> "$LOG"
    success=1
    break
  fi
  echo "[warn] attempt $attempt: salvage pass also failed" >> "$LOG"

  if [ "$attempt" -lt "$ATTEMPTS" ]; then
    echo "[wait] sleeping ${WAIT_SECONDS}s before retry (iFinD may be transiently down)" >> "$LOG"
    sleep "$WAIT_SECONDS"
  fi
done

if [ "$success" -ne 1 ]; then
  echo "[fail] daily_refresh failed after $ATTEMPTS attempts" >> "$LOG"
  exit 1
fi

MD="reports/${DATE}/cbond_overview.md"
if [ ! -s "$MD" ]; then
  echo "[skip] $MD missing or empty (likely non-trading day or fetch empty)" >> "$LOG"
  exit 0
fi

# Stage report + any updated index/latest pointers; ignore missing paths.
git add "reports/${DATE}/" 2>>"$LOG" || true
[ -f "reports/latest/cbond_overview.html" ] && git add "reports/latest/" 2>>"$LOG" || true
[ -f "data/last_asof.txt" ] && git add "data/last_asof.txt" 2>>"$LOG" || true

if git diff --cached --quiet; then
  echo "[skip] no staged changes for $DATE" >> "$LOG"
  exit 0
fi

if ! git commit -m "data: refresh ${DATE}" >> "$LOG" 2>&1; then
  echo "[fail] git commit failed" >> "$LOG"
  exit 1
fi

if ! git push origin HEAD:main >> "$LOG" 2>&1; then
  echo "[fail] git push origin failed" >> "$LOG"
  exit 1
fi
echo "[ok] pushed ${DATE} to origin/main" >> "$LOG"

# Mirror to xnprom (non-fatal if it fails — origin already has the canonical copy).
if git remote get-url xnprom >/dev/null 2>&1; then
  if git push xnprom HEAD:main >> "$LOG" 2>&1 \
     && git push xnprom HEAD:developer-1 >> "$LOG" 2>&1; then
    echo "[ok] mirrored ${DATE} to xnprom (main + developer-1)" >> "$LOG"
  else
    echo "[warn] xnprom mirror failed (non-fatal); origin push was successful" >> "$LOG"
  fi
fi
