"""Thin wrapper around iFinD HTTP quant endpoints.

Requests select a route through ``_network.py``: Clash TUN takes precedence
and forces direct requests, while a non-TUN process respects its proxy
environment. ``IFIND_NETWORK_MODE=direct|proxy`` can override auto-detection.
"""
import time

import requests

from _auth import get_access_token
from _network import configure_session, route_description

BASE = "https://quantapi.51ifind.com/api/v1"


def _configure_session(session):
    return configure_session(session)


_ROUTE_REPORTED = False


def _post(path, body, retries=3, timeout=60):
    global _ROUTE_REPORTED
    url = f"{BASE}/{path}"
    token = get_access_token()
    if not _ROUTE_REPORTED:
        print(f"[network] iFinD route={route_description()}")
        _ROUTE_REPORTED = True
    last_err = None
    for i in range(retries):
        try:
            session = _configure_session(requests.Session())
            response = session.post(
                url,
                headers={"Content-Type": "application/json", "access_token": token},
                json=body,
                timeout=timeout,
            )
            response.raise_for_status()
            return response.json()
        except (requests.RequestException, ValueError) as e:
            last_err = e
            if i < retries - 1:
                time.sleep(0.5 * (2 ** i))
    detail = getattr(getattr(last_err, "response", None), "text", "")[:300]
    raise RuntimeError(f"iFinD request failed after {retries} attempts: {last_err}; response={detail}") from last_err


def basic_data(codes, indipara):
    """basic_data_service — static/snapshot fields."""
    if isinstance(codes, list):
        codes = ",".join(codes)
    return _post("basic_data_service", {"codes": codes, "indipara": indipara})


def realtime(codes, indicators):
    """real_time_quotation."""
    if isinstance(codes, list):
        codes = ",".join(codes)
    if isinstance(indicators, list):
        indicators = ",".join(indicators)
    return _post("real_time_quotation", {"codes": codes, "indicators": indicators})


def history(codes, indicators, startdate, enddate, functionpara=None):
    """cmd_history_quotation — daily bars."""
    if isinstance(codes, list):
        codes = ",".join(codes)
    if isinstance(indicators, list):
        indicators = ",".join(indicators)
    fp = functionpara or {"Interval": "D", "Fill": "Omit"}
    return _post(
        "cmd_history_quotation",
        {"codes": codes, "indicators": indicators,
         "startdate": startdate, "enddate": enddate, "functionpara": fp},
    )


def batched(items, n):
    for i in range(0, len(items), n):
        yield items[i:i + n]


def ths_dr(table, condition, fields, fmt="dataframe"):
    """THS_DR — iFinD data pool (e.g. p05479 for CB universe).

    Args:
        table: data table ID (e.g. 'p05479' for convertible bonds)
        condition: semicolon-separated filter string (e.g. 'jyzt=2;edate=20260424')
        fields: comma-separated field list with :Y (e.g. 'jydm:Y,jydm_mc:Y')
        fmt: output format (default 'dataframe')
    """
    return _post("data_pool", {
        "reportname": table,
        "functionpara": dict(pair.split("=") for pair in condition.split(";") if "=" in pair),
        "outputpara": fields.replace(":Y", ""),
        "format": fmt,
    })
