"""
Diagnostic script: test the RAW Electricity Maps API directly for
day_ahead_price, year 2018 -- bypassing dlt and BigQuery entirely.

Purpose: isolate whether the 2018 shortfall (747 rows in BigQuery vs.
8,760 expected) is a genuine API/source-data limitation, or something
lost between the API and BigQuery in our own pipeline.

Unlike the production source code (which silently retries day-by-day
and only logs a final skipped-days summary), this script prints EVERY
chunk's outcome individually, including full error details, so nothing
is hidden.
"""

import time
import tomllib
from datetime import date

from sources.electricity_maps import _date_chunks, _fetch_flat_range

with open(".dlt/secrets.toml", "rb") as f:
    secrets = tomllib.load(f)

API_KEY = secrets["sources"]["electricity_maps"]["api_key"]

YEAR = 2018
CHUNK_DAYS = 10

start = date(YEAR, 1, 1)
end = date(YEAR, 12, 31)

total_rows = 0
successful_chunks = 0
failed_chunks = 0

print(f"Testing raw API for day_ahead_price, {YEAR}, in {CHUNK_DAYS}-day chunks...\n")

for chunk_start, chunk_end in _date_chunks(start, end, CHUNK_DAYS):
    try:
        rows = _fetch_flat_range("price-day-ahead", API_KEY, chunk_start, chunk_end, "value")
        total_rows += len(rows)
        successful_chunks += 1
        status = "OK" if len(rows) > 0 else "OK but EMPTY (0 rows, no error)"
        print(f"{chunk_start} to {chunk_end}: {status} -- {len(rows)} rows")
    except Exception as e:
        failed_chunks += 1
        print(f"{chunk_start} to {chunk_end}: FAILED -- {e}")

    time.sleep(1)

print(f"\n=== SUMMARY for {YEAR} ===")
print(f"Total rows from RAW API: {total_rows}")
print(f"Successful chunks: {successful_chunks}")
print(f"Failed chunks: {failed_chunks}")
print(f"Expected (full year, no gaps): 8760")
print(f"Currently in BigQuery for {YEAR}: 747")