"""Thin wrapper around iFinD HTTP quant endpoints."""
import json, urllib.request, ssl, time
from _auth import get_access_token

BASE = "https://quantapi.51ifind.com/api/v1"


def _post(path, body, retries=3, timeout=60):
    url = f"{BASE}/{path}"
    token = get_access_token()
    headers = {"Content-Type": "application/json", "access_token": token}
    data = json.dumps(body).encode("utf-8")
    last_err = None
    for i in range(retries):
        try:
            req = urllib.request.Request(url, data=data, headers=headers, method="POST")
            with urllib.request.urlopen(req, timeout=timeout, context=ssl.create_default_context()) as r:
                return json.loads(r.read().decode("utf-8"))
        except Exception as e:
            last_err = e
            time.sleep(0.5 * (2 ** i))
    raise last_err


def basic_data(codes, indipara):
    """basic_data_service — static/snapshot fields.

    codes: list[str] or comma-joined str
    indipara: list[{"indicator":..., "indiparams":[...]}]
    """
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
    """cmd_history_quotation — daily/weekly/monthly bars."""
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
