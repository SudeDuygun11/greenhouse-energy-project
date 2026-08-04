"""EnergyZero electricity and gas price source for dlt.

Loads dynamic day-ahead electricity prices and daily gas prices for the
Netherlands via the EnergyZero GraphQL API. Requires no authentication.

Known API constraints (see tests/test_energyzero.py):
    - GraphQL backend required for multi-day ranges; REST is limited to
      single-date requests.
    - Date ranges above ~30 days are unreliable and may return
      internal_server_error. CHUNK_DAYS is capped accordingly.
    - A chunk request fails entirely if any single day within it has no
      data (no partial results). Historical gaps are handled via
      day-level fallback.
    - Gas price windows reset at 06:00 local time, not midnight, so a
      single calendar-day query can return two overlapping rows.
"""

import asyncio
import logging
from datetime import date, timedelta
from typing import Awaitable, Callable, Iterator

import dlt
from energyzero import APIBackend, EnergyZero, PriceType

logger = logging.getLogger(__name__)

CHUNK_DAYS = 30
CHUNK_RETRY_DELAY_SECONDS = 1
DAY_RETRY_DELAY_SECONDS = 0.5

FetchFn = Callable[[EnergyZero, date, date], Awaitable[list[dict]]]


def _date_chunks(start_date: date, end_date: date, chunk_days: int) -> Iterator[tuple[date, date]]:
    """Splits a date range into fixed-size, non-overlapping chunks.

    Args:
        start_date: First date to include.
        end_date: Last date to include.
        chunk_days: Maximum number of days per chunk.

    Yields:
        (chunk_start, chunk_end) tuples covering the full range.
    """
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


async def _fetch_electricity_range(client: EnergyZero, start_date: date, end_date: date) -> list[dict]:
    """Fetches hourly electricity prices for a date range.

    Args:
        client: Authenticated EnergyZero GraphQL client.
        start_date: First date to include.
        end_date: Last date to include.

    Returns:
        Flat row dicts with keys: timestamp_utc, price_eur_per_kwh.
    """
    result = await client.get_electricity_prices(
        start_date=start_date,
        end_date=end_date,
        price_type=PriceType.ALL_IN,
    )
    return [
        {
            "timestamp_utc": entry["timerange"].start_including,
            "price_eur_per_kwh": entry["price"],
        }
        for entry in result.timestamp_prices
    ]


async def _fetch_gas_range(client: EnergyZero, start_date: date, end_date: date) -> list[dict]:
    """Fetches gas prices for a date range.

    Args:
        client: Authenticated EnergyZero GraphQL client.
        start_date: First date to include.
        end_date: Last date to include.

    Returns:
        Flat row dicts with keys: timestamp_utc, price_eur_per_m3.
    """
    result = await client.get_gas_prices(
        start_date=start_date,
        end_date=end_date,
        price_type=PriceType.ALL_IN,
    )
    return [
        {
            "timestamp_utc": entry["timerange"].start_including,
            "price_eur_per_m3": entry["price"],
        }
        for entry in result.timestamp_prices
    ]


async def _backfill_with_fallback(
    fetch_fn: FetchFn,
    start_date: date,
    end_date: date,
) -> tuple[list[dict], list[date]]:
    """Backfills a date range in chunks, retrying failed chunks day-by-day.

    A chunk request fails entirely if any single day inside it lacks data.
    On failure, this retries the chunk one day at a time so a single bad
    day does not discard the rest of the chunk.

    Args:
        fetch_fn: Either _fetch_electricity_range or _fetch_gas_range.
        start_date: First date to include.
        end_date: Last date to include.

    Returns:
        Tuple of (loaded_rows, skipped_days). skipped_days lists any
        individual dates with no available data, for observability.
    """
    all_rows: list[dict] = []
    skipped_days: list[date] = []

    async with EnergyZero(backend=APIBackend.GRAPHQL) as client:
        for chunk_start, chunk_end in _date_chunks(start_date, end_date, CHUNK_DAYS):
            try:
                all_rows.extend(await fetch_fn(client, chunk_start, chunk_end))
            except Exception:
                for single_day in _single_days(chunk_start, chunk_end):
                    try:
                        all_rows.extend(await fetch_fn(client, single_day, single_day))
                    except Exception:
                        skipped_days.append(single_day)
                    await asyncio.sleep(DAY_RETRY_DELAY_SECONDS)

            await asyncio.sleep(CHUNK_RETRY_DELAY_SECONDS)

    return all_rows, skipped_days


@dlt.resource(
    name="electricity_prices",
    write_disposition="merge",
    primary_key="timestamp_utc",
)
def electricity_prices(
    start_date: date = dlt.config.value,
    end_date: date = dlt.config.value,
):
    """dlt resource yielding hourly NL electricity prices (EUR/kWh, all-in)."""
    rows, skipped_days = asyncio.run(_backfill_with_fallback(_fetch_electricity_range, start_date, end_date))
    if skipped_days:
        logger.warning("electricity_prices: no data for %d day(s): %s", len(skipped_days), skipped_days)
    yield rows


@dlt.resource(
    name="gas_prices",
    write_disposition="merge",
    primary_key="timestamp_utc",
)
def gas_prices(
    start_date: date = dlt.config.value,
    end_date: date = dlt.config.value,
):
    """dlt resource yielding NL gas prices (EUR/m3, all-in, 06:00-aligned)."""
    rows, skipped_days = asyncio.run(_backfill_with_fallback(_fetch_gas_range, start_date, end_date))
    if skipped_days:
        logger.warning("gas_prices: no data for %d day(s): %s", len(skipped_days), skipped_days)
    yield rows


@dlt.source(name="energyzero")

def energyzero_source(start_date: date = dlt.config.value, end_date: date = dlt.config.value):
    """Groups electricity and gas resources into a single dlt source."""
    return electricity_prices(start_date, end_date), gas_prices(start_date, end_date)