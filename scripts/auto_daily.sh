#!/bin/bash
# Incremental refresh, strict validation, then publish only the dated report.
set -u
REPO_ROOT="${CBOND_REPO_ROOT:-$(cd "$(dirname "$0")/.." && pwd)}"
cd "$REPO_ROOT" || exit 1
export PATH="${CBOND_TOOL_PATH:-/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin}:$PATH"
PY="${CBOND_PYTHON:-/usr/local/bin/python3.12}"
if [ "${CBOND_AUTO_LOCK_HELD:-}" != "1" ]; then
  exec "$PY" "$REPO_ROOT/scripts/_run_locked.py" "$REPO_ROOT/data/.auto_daily.flock" /bin/bash "$0" "$@"
fi
export GIT_SSH_COMMAND="${GIT_SSH_COMMAND:-ssh -i $HOME/.ssh/id_ed25519 -o StrictHostKeyChecking=accept-new}"
DATE="${1:-$(date +%Y-%m-%d)}"
LOG_DIR="$REPO_ROOT/data/logs"
mkdir -p "$LOG_DIR"
LOG="$LOG_DIR/auto_${DATE}.log"
RECEIPT="$LOG_DIR/published_${DATE}.commit"
DOW=$(date -j -f "%Y-%m-%d" "$DATE" +%u 2>/dev/null || date -d "$DATE" +%u) || exit 1
if [ "$DOW" = 6 ] || [ "$DOW" = 7 ]; then
  echo "[skip] $DATE is weekend" >> "$LOG"
  exit 0
fi
# A tracked or large HTML file is not proof that git push succeeded.
if [ -z "${1:-}" ] && [ -f "$RECEIPT" ] \
   && [ "$(cat "$RECEIPT")" = "$(git rev-parse HEAD)" ] \
   && git diff --quiet HEAD -- "reports/${DATE}/"; then
  echo "[skip] $DATE already validated and pushed" >> "$LOG"
  exit 0
fi
# Never include a user's staged changes in an automatic data commit.
if ! git diff --cached --quiet; then
  echo "[fail] staged changes exist; preserve them and stop automatic publication" >> "$LOG"
  exit 1
fi
ATTEMPTS="${AUTO_DAILY_ATTEMPTS:-3}"
WAIT_SECONDS="${AUTO_DAILY_WAIT:-300}"
case "$ATTEMPTS:$WAIT_SECONDS" in *[!0-9:]*|:*|*:) echo '[fail] invalid retry settings' >> "$LOG"; exit 1;; esac
if [ "$ATTEMPTS" -lt 1 ]; then exit 1; fi
success=0
for attempt in $(seq 1 "$ATTEMPTS"); do
  echo "[attempt $attempt/$ATTEMPTS] $(date -Iseconds) daily_refresh for $DATE" >> "$LOG"
  # Same path on every attempt: reuse valid data, request only missing fields.
  # No forced full fetch and no weaker downstream salvage path.
  if "$PY" scripts/daily_refresh.py --trade-date "$DATE" >> "$LOG" 2>&1; then
    success=1
    break
  fi
  if [ "$attempt" -lt "$ATTEMPTS" ]; then
    delay=$((WAIT_SECONDS * attempt))
    echo "[wait] incomplete snapshot; retry missing data in ${delay}s" >> "$LOG"
    sleep "$delay"
  fi
done
if [ "$success" -ne 1 ]; then
  echo "[fail] daily_refresh failed after $ATTEMPTS attempts" >> "$LOG"
  exit 1
fi
for file in cbond_overview.md cbond_overview.html index.html; do
  if [ ! -s "reports/${DATE}/${file}" ]; then
    echo "[fail] missing report artifact: $file" >> "$LOG"
    exit 1
  fi
done
if ! "$PY" scripts/validate_snapshot.py --trade-date "$DATE" \
    --dataset "data/raw/asof=${DATE}/dataset.json" \
    --codes "data/raw/asof=${DATE}/cbond_codes.txt" \
    --backtest "data/raw/asof=${DATE//-/}/backtest_weekly.json" \
    --strict >> "$LOG" 2>&1; then
  echo "[fail] strict pre-publication validation failed for $DATE" >> "$LOG"
  exit 1
fi
if ! git diff --cached --quiet; then
  echo '[fail] staging changed during refresh; preserve user changes' >> "$LOG"
  exit 1
fi
if ! git add -- "reports/${DATE}/cbond_overview.md" "reports/${DATE}/cbond_overview.html" "reports/${DATE}/index.html" 2>> "$LOG"; then
  echo '[fail] git add failed' >> "$LOG"
  exit 1
fi
if ! git diff --cached --quiet; then
  if ! git commit -m "data: refresh ${DATE}" >> "$LOG" 2>&1; then
    echo '[fail] git commit failed' >> "$LOG"
    exit 1
  fi
fi
# Always retry a pending push, even if the report has no further changes.
if ! git push origin HEAD:main >> "$LOG" 2>&1; then
  echo '[fail] git push origin failed; next run will retry' >> "$LOG"
  exit 1
fi
if git remote get-url xnprom >/dev/null 2>&1; then
  if ! git push xnprom HEAD:main >> "$LOG" 2>&1 \
     || ! git push xnprom HEAD:developer-1 >> "$LOG" 2>&1; then
    echo '[warn] origin pushed, mirror failed; receipt withheld for retry' >> "$LOG"
    exit 1
  fi
fi
git rev-parse HEAD > "${RECEIPT}.tmp" && mv "${RECEIPT}.tmp" "$RECEIPT"
echo "[ok] validated and pushed $DATE" >> "$LOG"
