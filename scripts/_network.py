"""Choose the iFinD network route for the current machine.

Clash TUN mode is a transparent route and should not be combined with the
HTTP(S)_PROXY variables exported by an older shell session. When TUN is
enabled in a supported Clash Verge configuration, requests are therefore
forced to direct mode so the TUN interface handles the traffic. Otherwise
the normal requests proxy environment is respected.
"""
import os
import re
from pathlib import Path
from urllib.parse import urlsplit


TRUE_VALUES = {"1", "true", "yes", "on", "enabled"}
FALSE_VALUES = {"0", "false", "no", "off", "disabled"}
PROXY_KEYS = (
    "HTTPS_PROXY", "https_proxy", "HTTP_PROXY", "http_proxy",
    "ALL_PROXY", "all_proxy",
)


def _as_bool(value):
    if value is None:
        return None
    value = str(value).strip().lower()
    if value in TRUE_VALUES:
        return True
    if value in FALSE_VALUES:
        return False
    return None


def _config_candidates():
    configured = os.getenv("IFIND_TUN_CONFIG", "").strip()
    if configured:
        candidates = [Path(p).expanduser() for p in configured.split(os.pathsep) if p]
    else:
        candidates = [
            Path("~/Library/Application Support/io.github.clash-verge-rev.clash-verge-rev/clash-verge.yaml").expanduser(),
            Path("~/Library/Application Support/io.github.clash-verge-rev.clash-verge-rev/verge.yaml").expanduser(),
            Path("~/.config/clash-verge/clash-verge.yaml").expanduser(),
            Path("~/.config/clash-verge/verge.yaml").expanduser(),
        ]
    seen = set()
    for path in candidates:
        if path not in seen:
            seen.add(path)
            yield path


def _yaml_key_bool(text, key):
    pattern = rf"(?m)^\s*{re.escape(key)}\s*:\s*(true|false|yes|no|on|off|1|0)\s*(?:#.*)?$"
    match = re.search(pattern, text, flags=re.IGNORECASE)
    return _as_bool(match.group(1)) if match else None


def _tun_block_enabled(text):
    """Read the simple ``tun: ... enable: true`` form without PyYAML."""
    lines = text.splitlines()
    for index, line in enumerate(lines):
        if line.lstrip().startswith("#"):
            continue
        match = re.match(r"^(\s*)tun\s*:\s*(?:#.*)?$", line, flags=re.IGNORECASE)
        if not match:
            continue
        base_indent = len(match.group(1).expandtabs(2))
        for child in lines[index + 1:]:
            if child.strip() and not child.lstrip().startswith("#"):
                indent = len(child) - len(child.lstrip(" "))
                if indent <= base_indent:
                    break
            enabled = _yaml_key_bool(child, "enable")
            if enabled is not None:
                return enabled
    return None


def tun_enabled():
    """Return whether Clash TUN is configured as enabled."""
    override = _as_bool(os.getenv("IFIND_TUN_ENABLED"))
    if override is not None:
        return override

    for path in _config_candidates():
        try:
            text = path.read_text(encoding="utf-8")
        except (OSError, UnicodeError):
            continue
        # clash-verge.yaml uses the mihomo nested tun.enable field, while
        # verge.yaml stores the app-level setting as enable_tun_mode.
        enabled = _tun_block_enabled(text)
        if enabled is None:
            enabled = _yaml_key_bool(text, "enable_tun_mode")
        if enabled is not None:
            # The first usable config is authoritative. This prevents a
            # stale secondary settings file from overriding an explicit
            # ``tun.enable: false`` in the active mihomo config.
            return enabled
    return False


def configured_proxy_key():
    for key in PROXY_KEYS:
        value = os.getenv(key, "").strip()
        if value:
            return key
    return None


def detect_network_route():
    """Return ``(mode, reason)`` where mode is ``direct`` or ``proxy``."""
    explicit = os.getenv("IFIND_NETWORK_MODE", "").strip().lower()
    if explicit in {"direct", "tun"}:
        return "direct", "IFIND_NETWORK_MODE"
    if explicit == "proxy":
        return "proxy", "IFIND_NETWORK_MODE"

    disabled = _as_bool(os.getenv("IFIND_DISABLE_PROXY"))
    if disabled is True:
        return "direct", "IFIND_DISABLE_PROXY"
    if tun_enabled():
        return "direct", "Clash TUN enabled"

    proxy_key = configured_proxy_key()
    if proxy_key:
        return "proxy", proxy_key
    return "direct", "no proxy configured"


def configure_session(session):
    """Configure a requests session without mixing TUN and proxy routes."""
    mode, _ = detect_network_route()
    session.trust_env = mode == "proxy"
    return session


def route_description():
    mode, reason = detect_network_route()
    if mode == "proxy":
        return f"proxy ({reason})"
    return f"direct/TUN ({reason})" if "TUN" in reason else f"direct ({reason})"


def proxy_host_hint():
    """Return a redacted host:port hint for diagnostics, if configured."""
    key = configured_proxy_key()
    if not key:
        return ""
    value = os.getenv(key, "").strip()
    try:
        parsed = urlsplit(value if "://" in value else f"http://{value}")
        return parsed.hostname + (f":{parsed.port}" if parsed.port else "")
    except (AttributeError, ValueError):
        return "configured"
