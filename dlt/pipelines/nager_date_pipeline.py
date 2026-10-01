"""Runs a one-time historical backfill of NL public holidays into BigQuery.

Given the small total call count (~8 years), this doesn't need the
year-by-year pipeline.run() splitting used for Electricity Maps/
Open-Meteo -- a single run completing quickly poses little
interruption risk. If that assumption turns out wrong in practice,
apply the same year-chunked pipeline.run() pattern used elsewhere.

Airflow calls run(start_year, end_year) for the current and next year.

Usage:
    python -m pipelines.nager_date_pipeline
"""

import logging
from datetime import date

import dlt

from sources.nager_date import nager_date_source

logging.basicConfig(level=logging.INFO)


def run(start_year: int = 2019, end_year: int | None = None) -> str:
    pipeline = dlt.pipeline(
        pipeline_name="nager_date_pipeline",
        destination="bigquery",
        dataset_name="staging_nager_date",
    )

    load_info = pipeline.run(nager_date_source(start_year=start_year, end_year=end_year or date.today().year))
    print(load_info)
    return str(load_info)


if __name__ == "__main__":
    run()