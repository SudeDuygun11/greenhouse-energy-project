"""Tests for sources/nager_date.py.

test_no_auth_required_confirms_or_refutes_key_claim directly resolves
a real contradiction found during research: the official docs show no
auth needed, but a third-party site claims an API key is required.
"""

from datetime import date

from sources.nager_date import _fetch_holidays_for_year, _years


# --- Unit tests ---

def test_years_covers_full_range():
    years = list(_years(2019, 2022))
    assert years == [2019, 2020, 2021, 2022]


def test_years_single_year():
    years = list(_years(2024, 2024))
    assert years == [2024]


# --- Integration tests ---

def test_no_auth_required_confirms_or_refutes_key_claim():
    """
    Official docs (date.nager.at) show no authentication required.
    A third-party site claims an API key is needed. This test calls
    the API with NO key or auth header at all -- if it succeeds, the
    official docs were correct; if it fails with a 401/403, the
    third-party claim was correct and the source needs an auth header
    added.
    """
    rows = _fetch_holidays_for_year(2026)
    assert len(rows) > 0, "Expected at least one NL public holiday in 2026"


def test_holiday_row_shape():
    rows = _fetch_holidays_for_year(2026)
    first_row = rows[0]
    assert {"date", "local_name", "name", "country_code", "is_global", "types"} <= first_row.keys()
    assert first_row["country_code"] == "NL"


def test_known_dutch_holiday_present():
    """
    Sanity check using a fixed, well-known fact: King's Day (Koningsdag)
    is a Dutch national holiday on April 27 each year (unless that falls
    on a Sunday, in which case it moves to April 26 -- 2026-04-27 is a
    Monday, so no shift expected this year).
    """
    rows = _fetch_holidays_for_year(2026)
    dates = {row["date"] for row in rows}
    assert "2026-04-27" in dates, "Expected King's Day (Koningsdag) on 2026-04-27"