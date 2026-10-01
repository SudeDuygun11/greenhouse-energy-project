"""Loads the EnergyZero source into BigQuery.

Run directly for the one-time historical backfill; Airflow calls run()
with a short trailing window for incremental loads (merge on
timestamp_utc makes re-loading overlapping days safe).

Usage (from the dlt/ folder):
    python -m pipelines.energyzero_pipeline
"""

import logging
from datetime import date

import dlt

from sources.energyzero import energyzero_source

logging.basicConfig(level=logging.INFO)

# One-time backfill start. Electricity data has a confirmed gap around
# 2019-10-27 (see tests/test_energyzero.py); the source's day-level
# fallback handles this automatically, so no adjustment needed here.
BACKFILL_START_DATE = date(2019, 1, 1)


def run(start_date: date = BACKFILL_START_DATE, end_date: date | None = None) -> str:
    pipeline = dlt.pipeline(
        pipeline_name="energyzero_pipeline",
        destination="bigquery",
        dataset_name="staging_energyzero",
    )

    load_info = pipeline.run(
        energyzero_source(
            start_date=start_date,
            end_date=end_date or date.today(),
        )
    )

    print(load_info)
    return str(load_info)


if __name__ == "__main__":
    run()
