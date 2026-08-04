"""Tests for sources/energyzero.py.

Two categories, kept separate:

Unit tests (test_date_chunks_*): pure logic, no network calls, fast.
Run these on every change.

Integration tests (test_electricity_*, test_gas_*): call the live
EnergyZero API. 
"""

from datetime import date

import pytest
from energyzero import APIBackend, EnergyZero

from sources.energyzero import _date_chunks, _fetch_electricity_range, _fetch_gas_range

# Date confirmed to have no electricity data available from EnergyZero.
KNOWN_ELECTRICITY_GAP_DATE = date(2019, 10, 27)


# --- Unit tests ---

def test_date_chunks_covers_full_range_no_gaps():
    chunks = list(_date_chunks(date(2024, 1, 1), date(2024, 3, 1), chunk_days=30))

    for (_, chunk_end), (next_start, _) in zip(chunks, chunks[1:]):
        assert (next_start - chunk_end).days == 1

    assert chunks[0][0] == date(2024, 1, 1)
    assert chunks[-1][1] == date(2024, 3, 1)


def test_date_chunks_single_day_range():
    chunks = list(_date_chunks(date(2024, 1, 1), date(2024, 1, 1), chunk_days=30))
    assert chunks == [(date(2024, 1, 1), date(2024, 1, 1))]


# --- Integration tests ---

@pytest.mark.asyncio
async def test_electricity_returns_expected_row_shape():
    """Regression check on the API's response schema."""
    async with EnergyZero(backend=APIBackend.GRAPHQL) as client:
        rows = await _fetch_electricity_range(client, date(2026, 1, 1), date(2026, 1, 1))

    assert len(rows) == 24
    assert {"timestamp_utc", "price_eur_per_kwh"} <= rows[0].keys()
    assert rows[0]["timestamp_utc"].tzinfo is not None


@pytest.mark.asyncio
async def test_electricity_30_day_chunk_size_is_safe():
    """Regression check on CHUNK_DAYS; ranges above ~30 days are unreliable."""
    async with EnergyZero(backend=APIBackend.GRAPHQL) as client:
        rows = await _fetch_electricity_range(client, date(2026, 1, 1), date(2026, 1, 30))

    assert len(rows) == 720


@pytest.mark.asyncio
async def test_known_electricity_gap_date_still_fails():
    """Regression check on an upstream data gap.

    If this starts passing, EnergyZero has backfilled the gap and
    KNOWN_ELECTRICITY_GAP_DATE-related handling can be revisited.
    """
    async with EnergyZero(backend=APIBackend.GRAPHQL) as client:
        with pytest.raises(Exception):
            await _fetch_electricity_range(client, KNOWN_ELECTRICITY_GAP_DATE, KNOWN_ELECTRICITY_GAP_DATE)


@pytest.mark.asyncio
async def test_gas_single_day_returns_two_overlapping_rows():
    """Gas rate windows reset at 06:00 local, not midnight."""
    async with EnergyZero(backend=APIBackend.GRAPHQL) as client:
        rows = await _fetch_gas_range(client, date(2026, 1, 1), date(2026, 1, 1))

    assert len(rows) == 2


@pytest.mark.asyncio
async def test_gas_unaffected_by_known_electricity_gap_date():
    """Confirms the electricity gap is not a shared upstream outage."""
    async with EnergyZero(backend=APIBackend.GRAPHQL) as client:
        rows = await _fetch_gas_range(client, KNOWN_ELECTRICITY_GAP_DATE, KNOWN_ELECTRICITY_GAP_DATE)

    assert len(rows) > 0