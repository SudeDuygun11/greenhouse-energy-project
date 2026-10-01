"""Tests for sources/electricity_maps.py.

Unit tests (test_date_chunks_*): pure logic, no network calls.
Integration tests: call the live Electricity Maps API.

test_deep_historical_access_2015 resolves a real open question flagged
in data_quality_notes.md: whether this Academic-license token actually
honors the documented 2015-01-01 availability, or hits some other limit
first. Not assumed -- tested directly here.
"""

import tomllib
from datetime import date

import pytest
import requests

from sources.electricity_maps import _date_chunks, _fetch_flat_range

with open(".dlt/secrets.toml", "rb") as f:
    _secrets = tomllib.load(f)

API_KEY = _secrets["sources"]["electricity_maps"]["api_key"]


# --- Unit tests ---

def test_date_chunks_covers_full_range_no_gaps():
    chunks = list(_date_chunks(date(2024, 1, 1), date(2024, 2, 1), chunk_days=10))

    for (_, chunk_end), (next_start, _) in zip(chunks, chunks[1:]):
        assert (next_start - chunk_end).days == 1

    assert chunks[0][0] == date(2024, 1, 1)
    assert chunks[-1][1] == date(2024, 2, 1)


def test_date_chunks_single_day_range():
    chunks = list(_date_chunks(date(2024, 1, 1), date(2024, 1, 1), chunk_days=10))
    assert chunks == [(date(2024, 1, 1), date(2024, 1, 1))]


# --- Integration tests ---

def test_carbon_intensity_returns_expected_row_shape():
    """Regression check on the API's response schema."""
    rows = _fetch_flat_range("carbon-intensity", API_KEY, date(2026, 8, 1), date(2026, 8, 1), "carbonIntensity")

    assert len(rows) == 24
    assert {"datetime", "value", "is_estimated", "created_at", "updated_at"} <= rows[0].keys()


def test_10_day_chunk_size_is_safe():
    """Regression check on the documented CHUNK_DAYS limit."""
    rows = _fetch_flat_range("carbon-intensity", API_KEY, date(2026, 7, 1), date(2026, 7, 10), "carbonIntensity")
    assert len(rows) == 240


def test_deep_historical_access_2015():
    """
    Resolves an open question from data_quality_notes.md: does this
    Academic-license token actually return data from 2015-01-01, as the
    coverage page claims, or is there a stricter real limit?

    If this starts failing, the historical start_date defaults in
    sources/electricity_maps.py need to move forward to match reality.
    """
    rows = _fetch_flat_range("carbon-intensity", API_KEY, date(2015, 1, 1), date(2015, 1, 1), "carbonIntensity")
    assert len(rows) > 0, "Expected data on 2015-01-01 per documented coverage; token may not honor full range"


def test_day_ahead_price_unavailable_before_2017():
    """
    Confirms Day-Ahead Price's earlier availability boundary (2017-04-30).

    Initially assumed this would return an empty 200 OK response --
    confirmed via running this test that the real behavior is a 404
    error instead. Fixed here to match observed reality, not the
    original (wrong) assumption.
    """
    with pytest.raises(requests.HTTPError) as exc_info:
        _fetch_flat_range("price-day-ahead", API_KEY, date(2015, 1, 1), date(2015, 1, 1), "value")

    assert "404" in str(exc_info.value)