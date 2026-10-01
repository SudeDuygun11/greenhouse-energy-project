"""Open-Meteo historical weather source for dlt. Location: Naaldwijk,
Westland (NL) -- centre of Dutch commercial greenhouse horticulture.

No API key required. Uses the Historical Weather (archive) API:
https://archive-api.open-meteo.com/v1/archive

NOT yet verified against the live API in this project (unlike
EnergyZero/Electricity Maps) -- built from Open-Meteo's documented,
stable response format. tests/test_open_meteo.py is the first real
confirmation step; run it before trusting this file.

Design decisions, flagged:
    - Backfill defaults to 2019-01-01, not Open-Meteo's full 1940
      archive -- weather data before other sources' usable ranges
      has no join partner and isn't useful for this project.
    - Chunked by year (like Electricity Maps' final design) for the
      same interruption-durability reason -- NOT because any request
      size limit has been confirmed or is even expected; Open-Meteo's
      own example code shows single calls spanning arbitrary ranges.
    - Variable selection (temperature, humidity, precipitation, wind,
      cloud cover, solar radiation) is a judgment call for greenhouse
      relevance, not an exhaustive list of what's available.
"""

import time
from datetime import date, datetime, timedelta, timezone
from typing import Iterator

import dlt
import requests

BASE_URL = "https://archive-api.open-meteo.com/v1/archive"
LATITUDE = 51.992373
LONGITUDE = 4.208034
# Must be UTC: with a named timezone the API returns local times without an
# offset, which were stored as UTC -- every weather row ended up 2 hours late.
TIMEZONE = "UTC"

# Judgment call: variables relevant to greenhouse heating/lighting decisions.
HOURLY_VARIABLES = [
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "wind_speed_10m",
    "cloud_cover",
    "shortwave_radiation",
]

CHUNK_RETRY_DELAY_SECONDS = 1


def _year_chunks(start_date: date, end_date: date) -> Iterator[tuple[date, date]]:
    """Yields (chunk_start, chunk_end) tuples, one per calendar year."""
    current_year = start_date.year
    while date(current_year, 1, 1) <= end_date:
        chunk_start = max(start_date, date(current_year, 1, 1))
        chunk_end = min(end_date, date(current_year, 12, 31))
        yield chunk_start, chunk_end
        current_year += 1


def _fetch_weather_range(start_date: date, end_date: date) -> list[dict]:
    """Fetches one date range of hourly weather data.

    Open-Meteo's response shape: a top-level "hourly" object containing
    a "time" array plus one parallel array per requested variable, all
    aligned by index. This function transposes that into one flat row
    dict per timestamp.

    Returns:
        Flat row dicts with keys: datetime, plus one key per variable
        in HOURLY_VARIABLES.
    """
    response = requests.get(
        BASE_URL,
        params={
            "latitude": LATITUDE,
            "longitude": LONGITUDE,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "hourly": ",".join(HOURLY_VARIABLES),
            "timezone": TIMEZONE,
        },
        timeout=30,
    )
    response.raise_for_status()
    hourly = response.json()["hourly"]

    timestamps = hourly["time"]
    rows = []
    for i, timestamp in enumerate(timestamps):
        row = {"datetime": datetime.fromisoformat(timestamp).replace(tzinfo=timezone.utc)}
        for variable in HOURLY_VARIABLES:
            row[variable] = hourly[variable][i]
        # Hours the archive hasn't published yet come back with every value null.
        if all(row[variable] is None for variable in HOURLY_VARIABLES):
            continue
        rows.append(row)
    return rows


@dlt.resource(name="weather", write_disposition="merge", primary_key="datetime")
def weather(
    start_date: date = date(2019, 1, 1),
    end_date: date = dlt.config.value,
):
    """dlt resource yielding hourly weather for Naaldwijk, NL."""
    all_rows: list[dict] = []

    for chunk_start, chunk_end in _year_chunks(start_date, end_date):
        rows = _fetch_weather_range(chunk_start, chunk_end)
        all_rows.extend(rows)
        time.sleep(CHUNK_RETRY_DELAY_SECONDS)

    yield all_rows


@dlt.source(name="open_meteo")
def open_meteo_source(start_date: date = date(2019, 1, 1), end_date: date = dlt.config.value):
    """Single-resource source, kept as a @dlt.source for consistency
    with the other two sources in this project even though there's
    only one resource here.
    """
    return (weather(start_date=start_date, end_date=end_date),)