"""Runs a one-time historical backfill of Open-Meteo weather data into
BigQuery, for Naaldwijk (Westland, NL).

Year-chunked from the start (unlike the first version of the
Electricity Maps pipeline, which had to be fixed after an interruption
lost all progress) -- each year is a separate pipeline.run() call, so
completed years are durably saved even if a later year is interrupted.

Airflow calls run(start_date, end_date) with a short trailing window for
incremental loads; that runs as a single pipeline.run() call.

Usage:
    python -m pipelines.open_meteo_pipeline
"""

import logging
from datetime import date

import dlt

from sources.open_meteo import open_meteo_source

logging.basicConfig(level=logging.INFO)

EARLIEST_YEAR = 2019


def run(start_date: date | None = None, end_date: date | None = None) -> str | None:
    pipeline = dlt.pipeline(
        pipeline_name="open_meteo_pipeline",
        destination="bigquery",
        dataset_name="staging_open_meteo",
    )

    if start_date is not None:
        load_info = pipeline.run(open_meteo_source(start_date=start_date, end_date=end_date or date.today()))
        print(load_info)
        return str(load_info)

    current_year = date.today().year
    today = date.today()

    for year in range(EARLIEST_YEAR, current_year + 1):
        year_start = date(year, 1, 1)
        year_end = min(date(year, 12, 31), today)

        print(f"\n=== Backfilling {year} ({year_start} to {year_end}) ===")
        load_info = pipeline.run(open_meteo_source(start_date=year_start, end_date=year_end))
        print(load_info)


if __name__ == "__main__":
    run()