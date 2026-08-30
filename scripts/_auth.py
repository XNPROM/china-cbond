"""iFinD access_token loader.

Reads the cached access token written by the ifind-http-data-fetch skill.
Falls back to fetching fresh via refresh_token if cache is missing/stale.
"""
import hashlib
import json
import os
import fcntl
import tempfile
import time

import requests

CACHE_CANDIDATES = [
    os.path.expanduser("~/.codex_logs/ifind_access_token_cache.json"),
    "/Users/apple/Desktop/投资计划/.codex_logs/ifind_access_token_cache.json",
]
REFRESH_TOKEN_FILE = os.path.expanduser("~/.codex_logs/ifind_refresh_token.txt")
AUTH_URL = "https://quantapi.51ifind.com/api/v1/get_access_token"
MAX_AGE = 6 * 3600  # 6h; iFinD access_token TTL is ~8h
AUTH_RETRIES = 3


def _cache_path():
    return CACHE_CANDIDATES[0]


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
    cache_path = _cache_path()
    cache_dir = os.path.dirname(cache_path)
    os.makedirs(cache_dir, exist_ok=True)
    lock_path = os.path.join(cache_dir, ".ifind_access_token.lock")

    # launchd, Codex and other local jobs can refresh the same token at once.
    # Serialize refreshes and re-check the cache after acquiring the lock.
    with open(lock_path, "a+") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        tok, _ = _load_cached_token()
        if tok:
            return tok

        session = requests.Session()
        session.trust_env = False
        last_exc = None
        for attempt in range(AUTH_RETRIES):
            try:
                response = session.post(
                    AUTH_URL,
                    headers={"Content-Type": "application/json", "refresh_token": rt},
                    json={},
                    timeout=30,
                )
                response.raise_for_status()
                resp = response.json()
                break
            except (requests.RequestException, ValueError) as exc:
                last_exc = exc
                if attempt < AUTH_RETRIES - 1:
                    time.sleep(2 ** attempt)
        else:
            detail = getattr(getattr(last_exc, "response", None), "text", "")[:300]
            raise RuntimeError(
                f"iFinD auth request failed after {AUTH_RETRIES} attempts: "
                f"{last_exc}; response={detail}"
            ) from last_exc

        if not ((resp.get("data") or {}).get("access_token")):
            raise RuntimeError(f"iFinD auth rejected refresh token: {resp}")
        tok = resp["data"]["access_token"]

        payload = {
            "version": 1,
            "refresh_token_fingerprint": hashlib.sha256(rt.encode()).hexdigest(),
            "access_token": tok,
            "fetched_at_epoch": int(time.time()),
        }
        fd, tmp_path = tempfile.mkstemp(prefix=".ifind_access_token.", dir=cache_dir)
        try:
            with os.fdopen(fd, "w") as f:
                json.dump(payload, f)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_path, cache_path)
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
        return tok


def get_access_token():
    tok, src = _load_cached_token()
    if tok:
        return tok
    return _fetch_fresh()


if __name__ == "__main__":
    print(get_access_token()[:20] + "...")
