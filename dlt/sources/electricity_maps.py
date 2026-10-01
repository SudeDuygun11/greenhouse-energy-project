"""Electricity Maps signals source for dlt. Zone: NL only.

Loads 6 hourly signals for the Netherlands via the Electricity Maps
REST API: carbon intensity, renewable %, carbon-free %, electricity
mix (full, all generation sources), total load, and day-ahead price.

Requires an API key (Academic license in this project) -- see
.dlt/secrets.toml, section [sources.electricity_maps].

Known API constraints (see tests/test_electricity_maps.py):
    - past-range is limited to 10 days (240 hours) per call at hourly
      granularity. Documented by Electricity Maps, not discovered by
      testing (unlike EnergyZero's undocumented limits).
    - Historical availability differs by signal: most signals available
      from 2015-01-01, but Day-Ahead Price only from 2017-04-30.
    - The electricity-mix/gas/past-range endpoint (source-type-filtered)
      returned 401 for this token despite being listed as available on
      the coverage page. WORKAROUND: the unfiltered electricity-mix/
      past-range endpoint returns 200 and includes gas as one field
      inside a nested `mix` object, alongside every other generation
      source. This file uses the unfiltered endpoint and keeps the
      full mix, not just gas.
    - Whether the Academic license token actually honors the full
      2015 historical range in practice is tested directly in
      tests/test_electricity_maps.py, not assumed here.

Fixed after initial backfill (see data_quality_notes.md items #7, #10):
    - Flat signals now retain created_at/updated_at from the raw
      response, needed to eventually support dbt snapshot-based
      revision tracking (estimated -> measured transitions).
    - Electricity Mix now retains estimation_method, previously
      silently dropped.
"""

import time
from datetime import date, datetime, timedelta, timezone
from typing import Iterator

import dlt
import requests

BASE_URL = "https://api.electricitymaps.com/v4"
ZONE = "NL"
CHUNK_DAYS = 10  # documented hourly-granularity limit
CHUNK_RETRY_DELAY_SECONDS = 1
DAY_RETRY_DELAY_SECONDS = 0.5


def _date_chunks(start_date: date, end_date: date, chunk_days: int) -> Iterator[tuple[date, date]]:
    """Splits a date range into fixed-size, non-overlapping chunks."""
    current = start_date
    while current <= end_date:
        chunk_end = min(current + timedelta(days=chunk_days - 1), end_date)
        yield current, chunk_end
        current = chunk_end + timedelta(days=1)


def _single_days(start_date: date, end_date: date) -> Iterator[date]:
    """Yields each individual date in [start_date, end_date]."""
    current = start_date
    while current <= end_date:
        yield current
        current += timedelta(days=1)


def _to_iso_z(d: date) -> str:
    """Formats a date as midnight UTC in the API's expected ISO format."""
    return datetime(d.year, d.month, d.day, tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")


def _fetch_flat_range(
    endpoint: str,
    api_key: str,
    start_date: date,
    end_date: date,
    value_field: str,
) -> list[dict]:
    """Fetches a date range from an endpoint with a flat response shape.

    Covers carbon-intensity, renewable-energy, carbon-free-energy,
    total-load, and price-day-ahead -- all share the same response
    shape, differing only in which key holds the numeric value.

    Args:
        endpoint: API path segment, e.g. "carbon-intensity".
        api_key: Electricity Maps auth token.
        start_date: First date to include.
        end_date: Last date to include.
        value_field: Response key holding the numeric value
            (e.g. "carbonIntensity" or "value").

    Returns:
        Flat row dicts with keys: datetime, value, is_estimated.
    """
    response = requests.get(
        f"{BASE_URL}/{endpoint}/past-range",
        headers={"auth-token": api_key},
        params={
            "zone": ZONE,
            "start": _to_iso_z(start_date),
            "end": _to_iso_z(end_date + timedelta(days=1)),  # end is exclusive
            "temporalGranularity": "hourly",
        },
        timeout=30,
    )
    response.raise_for_status()
    rows = response.json().get("data", [])
    return [
        {
            "datetime": row["datetime"],
            "value": row.get(value_field),
            "is_estimated": row.get("isEstimated"),
            "created_at": row.get("createdAt"),
            "updated_at": row.get("updatedAt"),
        }
        for row in rows
    ]


def _fetch_electricity_mix_range(api_key: str, start_date: date, end_date: date) -> list[dict]:
    """Fetches the full (unfiltered) electricity mix for a date range.

    Uses the unfiltered endpoint deliberately -- the source-type-filtered
    variant (electricity-mix/gas/past-range) returns 401 for this token.
    The unfiltered response includes every generation source nested
    under `mix`, including gas, at no extra cost.

    Returns:
        Row dicts with keys: datetime, mix (nested dict of all sources).
    """
    response = requests.get(
        f"{BASE_URL}/electricity-mix/past-range",
        headers={"auth-token": api_key},
        params={
            "zone": ZONE,
            "start": _to_iso_z(start_date),
            "end": _to_iso_z(end_date + timedelta(days=1)),
            "temporalGranularity": "hourly",
        },
        timeout=30,
    )
    response.raise_for_status()
    rows = response.json().get("data", [])
    return [
        {
            "datetime": row["datetime"],
            "mix": row.get("mix"),
            "estimation_method": row.get("estimationMethod"),
        }
        for row in rows
    ]


def _backfill_with_fallback(fetch_chunk_fn, start_date: date, end_date: date) -> tuple[list[dict], list[date]]:
    """Chunked backfill with day-by-day fallback on chunk failure.

    Precautionary pattern carried over from the EnergyZero source, where
    it was necessary due to confirmed isolated data gaps. No equivalent
    gap has been confirmed for Electricity Maps yet -- this exists in
    case one is ever encountered, not because one is known to exist.

    Auth errors (401/403) are re-raised instead of skipped: they mean a
    missing or invalid API key, not missing data, and would otherwise turn
    every day into a "skipped" day while the load still reports success.

    Returns:
        Tuple of (loaded_rows, skipped_days).
    """
    all_rows: list[dict] = []
    skipped_days: list[date] = []

    chunks = list(_date_chunks(start_date, end_date, CHUNK_DAYS))
    for i, (chunk_start, chunk_end) in enumerate(chunks, start=1):
        try:
            rows = fetch_chunk_fn(chunk_start, chunk_end)
            all_rows.extend(rows)
            print(f"    chunk {i}/{len(chunks)} ({chunk_start} to {chunk_end}): {len(rows)} rows")
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code in (401, 403):
                raise
            print(f"    chunk {i}/{len(chunks)} ({chunk_start} to {chunk_end}): FAILED ({e}), retrying day-by-day...")
            for single_day in _single_days(chunk_start, chunk_end):
                try:
                    all_rows.extend(fetch_chunk_fn(single_day, single_day))
                except requests.HTTPError as day_error:
                    if day_error.response is not None and day_error.response.status_code in (401, 403):
                        raise
                    skipped_days.append(single_day)
                time.sleep(DAY_RETRY_DELAY_SECONDS)

        time.sleep(CHUNK_RETRY_DELAY_SECONDS)

    return all_rows, skipped_days


def _make_flat_resource(name: str, endpoint: str, value_field: str, true_min_start: date):
    """Factory for the 5 flat-shape resources.

    true_min_start is the signal's documented earliest availability
    (differs per signal -- see module docstring). Whatever start_date
    is requested by the caller is clipped to never go earlier than
    true_min_start, and the resource is skipped entirely (no API calls)
    if the requested range doesn't overlap true_min_start at all. This
    matters specifically for day_ahead_price when backfilling year by
    year: without clipping, years before 2017-04-30 would generate
    hundreds of wasted 404 requests instead of being skipped outright.
    """

    @dlt.resource(name=name, write_disposition="merge", primary_key="datetime")
    def resource(
        api_key: str = dlt.secrets.value,
        start_date: date = true_min_start,
        end_date: date = dlt.config.value,
    ):
        if end_date < true_min_start:
            print(f"[{name}] skipped entirely: range ends before {true_min_start}")
            yield []
            return

        effective_start = max(start_date, true_min_start)
        print(f"[{name}] fetching {effective_start} to {end_date}...")

        rows, skipped_days = _backfill_with_fallback(
            lambda s, e: _fetch_flat_range(endpoint, api_key, s, e, value_field),
            effective_start,
            end_date,
        )
        if skipped_days:
            print(f"[{name}] no data for {len(skipped_days)} day(s): {skipped_days}")
        yield rows

    resource.__name__ = name
    return resource


# Historical start dates per signal, per Electricity Maps' coverage page.
carbon_intensity = _make_flat_resource("carbon_intensity", "carbon-intensity", "carbonIntensity", date(2015, 1, 1))
renewable_percentage = _make_flat_resource("renewable_percentage", "renewable-energy", "value", date(2015, 1, 1))
carbon_free_percentage = _make_flat_resource("carbon_free_percentage", "carbon-free-energy", "value", date(2015, 1, 1))
total_load = _make_flat_resource("total_load", "total-load", "value", date(2015, 1, 1))
day_ahead_price = _make_flat_resource("day_ahead_price", "price-day-ahead", "value", date(2017, 4, 30))


@dlt.resource(name="electricity_mix", write_disposition="merge", primary_key="datetime")
def electricity_mix(
    api_key: str = dlt.secrets.value,
    start_date: date = date(2015, 1, 1),
    end_date: date = dlt.config.value,
):
    """dlt resource yielding full electricity generation mix for NL."""
    true_min_start = date(2015, 1, 1)
    if end_date < true_min_start:
        print(f"[electricity_mix] skipped entirely: range ends before {true_min_start}")
        yield []
        return

    effective_start = max(start_date, true_min_start)
    print(f"[electricity_mix] fetching {effective_start} to {end_date}...")

    rows, skipped_days = _backfill_with_fallback(
        lambda s, e: _fetch_electricity_mix_range(api_key, s, e),
        effective_start,
        end_date,
    )
    if skipped_days:
        print(f"[electricity_mix] no data for {len(skipped_days)} day(s): {skipped_days}")
    yield rows


@dlt.source(name="electricity_maps")
def electricity_maps_source(start_date: date = dlt.config.value, end_date: date = dlt.config.value):
    """Groups all 6 signals into a single dlt source.

    Both start_date and end_date are passed through to every resource.
    Each resource independently clips start_date to never go earlier
    than its own true documented availability -- see _make_flat_resource
    and electricity_mix docstrings for why this matters.
    """
    return (
        carbon_intensity(start_date=start_date, end_date=end_date),
        renewable_percentage(start_date=start_date, end_date=end_date),
        carbon_free_percentage(start_date=start_date, end_date=end_date),
        electricity_mix(start_date=start_date, end_date=end_date),
        total_load(start_date=start_date, end_date=end_date),
        day_ahead_price(start_date=start_date, end_date=end_date),
    )