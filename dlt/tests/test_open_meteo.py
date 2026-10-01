"""Tests for sources/open_meteo.py.

This is the FIRST real confirmation that Open-Meteo's documented
response shape actually matches reality for this project -- unlike
EnergyZero/Electricity Maps, no manual diagnostic script was run first.
If these fail, the source file's parsing logic needs fixing before
any backfill is attempted.
"""

from datetime import date

from sources.open_meteo import HOURLY_VARIABLES, _fetch_weather_range, _year_chunks


# --- Unit tests ---

def test_year_chunks_covers_full_range():
    chunks = list(_year_chunks(date(2019, 6, 15), date(2021, 3, 10)))
    assert chunks[0] == (date(2019, 6, 15), date(2019, 12, 31))
    assert chunks[1] == (date(2020, 1, 1), date(2020, 12, 31))
    assert chunks[2] == (date(2021, 1, 1), date(2021, 3, 10))


def test_year_chunks_single_year():
    chunks = list(_year_chunks(date(2024, 1, 1), date(2024, 12, 31)))
    assert chunks == [(date(2024, 1, 1), date(2024, 12, 31))]


# --- Integration tests ---

def test_weather_returns_expected_row_shape():
    """First real test against the live API in this project."""
    rows = _fetch_weather_range(date(2026, 1, 1), date(2026, 1, 1))

    assert len(rows) == 24, "Expected 24 hourly rows for one day"
    first_row = rows[0]
    assert "datetime" in first_row
    for variable in HOURLY_VARIABLES:
        assert variable in first_row


def test_multi_day_range_row_count():
    """Confirms row count scales correctly across a multi-day request."""
    rows = _fetch_weather_range(date(2026, 1, 1), date(2026, 1, 7))
    assert len(rows) == 7 * 24


def test_deep_historical_access_2019():
    """Confirms the project's chosen backfill start date actually
    returns data -- not assumed just because 2019 is well within
    Open-Meteo's documented 1940+ archive.
    """
    rows = _fetch_weather_range(date(2019, 1, 1), date(2019, 1, 1))
    assert len(rows) == 24