"""Runs a one-time historical backfill of all 6 Electricity Maps signals
into BigQuery.

Manual backfill entry point, matching the pattern used for
energyzero_pipeline.py -- with one deliberate difference: this runs
pipeline.run() once PER YEAR, rather than once for the entire ~11-year
range in a single call.

Airflow calls run(start_date, end_date) with a short trailing window for
incremental loads; that runs as a single pipeline.run() call.

Usage:
    python -m pipelines.electricity_maps_pipeline
"""

import logging
from datetime import date

import dlt

from sources.electricity_maps import electricity_maps_source

logging.basicConfig(level=logging.INFO)

# Earliest date across all 6 signals -- individual resources still use
# their own later start_date default (e.g. day_ahead_price at 2017-04-30)
# and will simply return no data for years before their own start.
EARLIEST_YEAR = 2015


def _year_boundaries(year: int) -> tuple[date, date]:
    """Returns (start, end) for a calendar year, capped at today if
    the year is the current year in progress."""
    start = date(year, 1, 1)
    year_end = date(year, 12, 31)
    today = date.today()
    end = min(year_end, today)
    return start, end


def run(start_date: date | None = None, end_date: date | None = None) -> str | None:
    pipeline = dlt.pipeline(
        pipeline_name="electricity_maps_pipeline",
        destination="bigquery",
        dataset_name="staging_electricity_maps",
    )

    if start_date is not None:
        load_info = pipeline.run(electricity_maps_source(start_date=start_date, end_date=end_date or date.today()))
        print(load_info)
        return str(load_info)

    current_year = date.today().year

    for year in range(EARLIEST_YEAR, current_year + 1):
        year_start, year_end = _year_boundaries(year)
        print(f"\n=== Backfilling {year} ({year_start} to {year_end}) ===")

        load_info = pipeline.run(
            electricity_maps_source(start_date=year_start, end_date=year_end)
        )
        print(load_info)


if __name__ == "__main__":
    run()