"""Thin wrapper around iFinD HTTP quant endpoints.

Uses curl subprocess instead of urllib to work around LibreSSL 2.8.3 TLS
incompatibility with quantapi.51ifind.com.
"""
import json, subprocess, time
from _auth import get_access_token

BASE = "https://quantapi.51ifind.com/api/v1"


def _post(path, body, retries=3, timeout=60):
    url = f"{BASE}/{path}"
    token = get_access_token()
    payload = json.dumps(body)
    last_err = None
    for i in range(retries):
        try:
            result = subprocess.run(
                [
                    "curl", "-s", "-X", "POST", url,
                    "-H", "Content-Type: application/json",
                    "-H", f"access_token: {token}",
                    "--data", payload,
                    "--max-time", str(timeout),
                ],
                capture_output=True, text=True, timeout=timeout + 5,
            )
            if result.returncode != 0:
                raise RuntimeError(f"curl error: {result.stderr.strip()}")
            return json.loads(result.stdout)
        except Exception as e:
            last_err = e
            time.sleep(0.5 * (2 ** i))
    raise last_err


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
