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

# Run the 8-step pipeline.
if ! /usr/local/bin/python3.12 scripts/daily_refresh.py --trade-date "$DATE" >> "$LOG" 2>&1; then
  echo "[fail] daily_refresh exited non-zero" >> "$LOG"
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
  echo "[fail] git push failed" >> "$LOG"
  exit 1
fi

echo "[ok] pushed ${DATE} to origin/main" >> "$LOG"
