"""
Diagnostic script: test all 6 planned Electricity Maps signals for zone=NL.

Confirms each endpoint responds correctly and prints the real response
shape, before writing any actual dlt source code. Each signal is tested
independently -- one failure doesn't stop the others from being tested.

Small 2-day window used deliberately, since this is a shape/auth check,
not a re-test of the already-confirmed 10-day chunk limit.
"""

import tomllib
from datetime import datetime, timedelta, timezone

import requests

with open(".dlt/secrets.toml", "rb") as f:
    secrets = tomllib.load(f)

API_KEY = secrets["sources"]["electricity_maps"]["api_key"]
ZONE = "NL"

end = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
start = end - timedelta(days=2)

START_STR = start.isoformat().replace("+00:00", "Z")
END_STR = end.isoformat().replace("+00:00", "Z")

HEADERS = {"auth-token": API_KEY}


def test_endpoint(name: str, url: str, extra_params: dict | None = None):
    """Calls one endpoint and prints status, row count, and first row."""
    params = {
        "zone": ZONE,
        "start": START_STR,
        "end": END_STR,
        "temporalGranularity": "hourly",
    }
    if extra_params:
        params.update(extra_params)

    print(f"\n--- {name} ---")
    try:
        response = requests.get(url, headers=HEADERS, params=params, timeout=15)
        print(f"Status: {response.status_code}")
        data = response.json()

        if response.status_code != 200:
            print("Error response:", data)
            return

        rows = data.get("data", [])
        print(f"Rows returned: {len(rows)}")
        if rows:
            print("First row:", rows[0])

    except Exception as e:
        print(f"Request failed: {e}")


# 1. Carbon Intensity -- already confirmed working, re-testing for completeness
test_endpoint(
    "Carbon Intensity",
    "https://api.electricitymaps.com/v4/carbon-intensity/past-range",
)

# 2. Renewable Energy %
test_endpoint(
    "Renewable Energy %",
    "https://api.electricitymaps.com/v4/renewable-energy/past-range",
)

# 3. Carbon-Free Energy %
test_endpoint(
    "Carbon-Free Energy %",
    "https://api.electricitymaps.com/v4/carbon-free-energy/past-range",
)

# 4. Electricity Mix -- gas source type only. Different URL shape:
# sourceType is part of the PATH, not a query param.
test_endpoint(
    "Electricity Mix (gas)",
    "https://api.electricitymaps.com/v4/electricity-mix/past-range",
)

# 5. Total Load
test_endpoint(
    "Total Load",
    "https://api.electricitymaps.com/v4/total-load/past-range",
)

# 6. Day-Ahead Price -- flagged assumption: docs excerpt didn't explicitly
# restate the 10-day limit text for this endpoint like the others, but
# using the same small 2-day window here regardless, so that's not
# being tested either way in this run.
test_endpoint(
    "Day-Ahead Price",
    "https://api.electricitymaps.com/v4/price-day-ahead/past-range",
)