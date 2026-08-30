import os
import sys


sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from _network import configure_session, detect_network_route, tun_enabled


def _clear_route_overrides(monkeypatch):
    for key in ("IFIND_NETWORK_MODE", "IFIND_DISABLE_PROXY", "IFIND_TUN_ENABLED"):
        monkeypatch.delenv(key, raising=False)


def test_tun_route_has_priority_over_proxy_environment(tmp_path, monkeypatch):
    config = tmp_path / "clash.yaml"
    config.write_text("tun:\n  enable: true\n")
    monkeypatch.setenv("IFIND_TUN_CONFIG", str(config))
    monkeypatch.setenv("HTTP_PROXY", "http://127.0.0.1:7897")
    _clear_route_overrides(monkeypatch)

    assert tun_enabled() is True
    assert detect_network_route() == ("direct", "Clash TUN enabled")

    session = configure_session(type("Session", (), {})())
    assert session.trust_env is False


def test_proxy_route_is_used_when_tun_is_disabled(monkeypatch):
    monkeypatch.setenv("IFIND_TUN_CONFIG", "/path/that/does/not/exist")
    monkeypatch.setenv("IFIND_TUN_ENABLED", "0")
    monkeypatch.setenv("HTTPS_PROXY", "http://127.0.0.1:7897")
    monkeypatch.delenv("IFIND_NETWORK_MODE", raising=False)
    monkeypatch.delenv("IFIND_DISABLE_PROXY", raising=False)

    assert detect_network_route() == ("proxy", "HTTPS_PROXY")


def test_explicit_tun_false_does_not_fall_through_to_other_configs(tmp_path, monkeypatch):
    config = tmp_path / "clash.yaml"
    config.write_text("tun:\n  enable: false\n")
    monkeypatch.setenv("IFIND_TUN_CONFIG", str(config))
    monkeypatch.delenv("IFIND_TUN_ENABLED", raising=False)

    assert tun_enabled() is False
