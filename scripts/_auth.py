"""iFinD access_token loader.

Reads the cached access token written by the ifind-http-data-fetch skill.
Falls back to fetching fresh via refresh_token if cache is missing/stale.
"""
import json, os, time, subprocess, hashlib

CACHE_CANDIDATES = [
    "/Users/apple/Desktop/投资计划/.codex_logs/ifind_access_token_cache.json",
    os.path.expanduser("~/.codex_logs/ifind_access_token_cache.json"),
]
REFRESH_TOKEN_FILE = os.path.expanduser("~/.codex_logs/ifind_refresh_token.txt")
AUTH_URL = "https://quantapi.51ifind.com/api/v1/get_access_token"
MAX_AGE = 6 * 3600  # 6h; iFinD access_token TTL is ~8h


def _load_cached_token():
    for p in CACHE_CANDIDATES:
        if os.path.exists(p):
            try:
                d = json.load(open(p))
                age = time.time() - d.get("fetched_at_epoch", 0)
                if age < MAX_AGE and d.get("access_token"):
                    return d["access_token"], p
            except Exception:
                continue
    return None, None


def _fetch_fresh():
    if not os.path.exists(REFRESH_TOKEN_FILE):
        raise RuntimeError(f"refresh_token file not found: {REFRESH_TOKEN_FILE}")
    rt = open(REFRESH_TOKEN_FILE).read().strip()
    result = subprocess.run(
        ["curl", "-s", "-X", "POST", AUTH_URL,
         "-H", "Content-Type: application/json",
         "-H", f"refresh_token: {rt}",
         "--data", "{}",
         "--max-time", "30"],
        capture_output=True, text=True, timeout=35,
    )
    if result.returncode != 0:
        raise RuntimeError(f"curl auth error: {result.stderr.strip()}")
    resp = json.loads(result.stdout)
    tok = resp["data"]["access_token"]
    # write cache
    cache_path = CACHE_CANDIDATES[0]
    os.makedirs(os.path.dirname(cache_path), exist_ok=True)
    json.dump(
        {
            "version": 1,
            "refresh_token_fingerprint": hashlib.sha256(rt.encode()).hexdigest(),
            "access_token": tok,
            "fetched_at_epoch": int(time.time()),
        },
        open(cache_path, "w"),
    )
    return tok


def get_access_token():
    tok, src = _load_cached_token()
    if tok:
        return tok
    return _fetch_fresh()


if __name__ == "__main__":
    print(get_access_token()[:20] + "...")
