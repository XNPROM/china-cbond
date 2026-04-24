"""iFinD access_token loader.

Reads the cached access token written by the ifind-http-data-fetch skill.
Falls back to fetching fresh via refresh_token if cache is missing/stale.
"""
import json, os, time, urllib.request, ssl, hashlib

_CACHE_DIR = os.path.expanduser("~/.codex_logs")
CACHE_PATH = os.path.join(_CACHE_DIR, "ifind_access_token_cache.json")
REFRESH_TOKEN_FILE = os.path.join(_CACHE_DIR, "ifind_refresh_token.txt")
AUTH_URL = "https://quantapi.51ifind.com/api/v1/get_access_token"
MAX_AGE = 6 * 3600  # 6h; iFinD access_token TTL is ~8h


def _load_cached_token():
    if os.path.exists(CACHE_PATH):
        try:
            with open(CACHE_PATH, encoding="utf-8") as f:
                d = json.load(f)
            age = time.time() - d.get("fetched_at_epoch", 0)
            if age < MAX_AGE and d.get("access_token"):
                return d["access_token"]
        except Exception:
            pass
    return None


def _fetch_fresh():
    if not os.path.exists(REFRESH_TOKEN_FILE):
        raise RuntimeError(f"refresh_token file not found: {REFRESH_TOKEN_FILE}")
    with open(REFRESH_TOKEN_FILE, encoding="utf-8") as f:
        rt = f.read().strip()
    req = urllib.request.Request(
        AUTH_URL, method="POST",
        headers={"Content-Type": "application/json", "refresh_token": rt},
        data=b"{}",
    )
    with urllib.request.urlopen(req, timeout=30, context=ssl.create_default_context()) as r:
        resp = json.loads(r.read().decode("utf-8"))
    tok = resp["data"]["access_token"]
    # write cache
    os.makedirs(os.path.dirname(CACHE_PATH), exist_ok=True)
    with open(CACHE_PATH, "w", encoding="utf-8") as f:
        json.dump(
            {
                "version": 1,
                "refresh_token_fingerprint": hashlib.sha256(rt.encode()).hexdigest(),
                "access_token": tok,
                "fetched_at_epoch": int(time.time()),
            },
            f,
        )
    return tok


def get_access_token():
    tok = _load_cached_token()
    if tok:
        return tok
    return _fetch_fresh()


if __name__ == "__main__":
    print(get_access_token()[:20] + "...")
