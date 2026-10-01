"""Nager.Date public holidays source for dlt. Country: NL.

NOT yet verified against the live API in this project. Official docs
(date.nager.at) show no authentication required and explicitly state
no rate limits -- but a third-party site claims an API key is needed.
These directly contradict each other; tests/test_nager_date.py resolves
this empirically rather than trusting either source blindly.

One call per calendar year (holidays are inherently year-scoped, not
a date-range query) -- no chunking needed given the small total call
count for this project's range.
"""

from datetime import date
from typing import Iterator

import dlt
import requests

BASE_URL = "https://date.nager.at/api/v3"
COUNTRY_CODE = "NL"


def _years(start_year: int, end_year: int) -> Iterator[int]:
    """Yields each calendar year in [start_year, end_year], inclusive."""
    yield from range(start_year, end_year + 1)


def _fetch_holidays_for_year(year: int) -> list[dict]:
    """Fetches all public holidays for one year, country=NL.

    Returns:
        Row dicts matching the raw API response shape: date, local_name,
        name, country_code, is_global, counties, types.
    """
    response = requests.get(
        f"{BASE_URL}/PublicHolidays/{year}/{COUNTRY_CODE}",
        timeout=15,
    )
    response.raise_for_status()
    raw_holidays = response.json()

    return [
        {
            "date": holiday["date"],
            "local_name": holiday.get("localName"),
            "name": holiday.get("name"),
            "country_code": holiday.get("countryCode"),
            "is_global": holiday.get("global"),
            "counties": holiday.get("counties"),
            "types": holiday.get("types"),
        }
        for holiday in raw_holidays
    ]


@dlt.resource(name="public_holidays", write_disposition="merge", primary_key="date")
def public_holidays(
    start_year: int = 2019,
    end_year: int = dlt.config.value,
):
    """dlt resource yielding NL public holidays, one row per holiday."""
    all_rows: list[dict] = []
    for year in _years(start_year, end_year):
        rows = _fetch_holidays_for_year(year)
        all_rows.extend(rows)
        print(f"[public_holidays] {year}: {len(rows)} holidays")
    yield all_rows


@dlt.source(name="nager_date")
def nager_date_source(start_year: int = 2019, end_year: int = dlt.config.value):
    """Single-resource source, kept as @dlt.source for consistency
    with the other sources in this project.
    """
    return (public_holidays(start_year=start_year, end_year=end_year),)