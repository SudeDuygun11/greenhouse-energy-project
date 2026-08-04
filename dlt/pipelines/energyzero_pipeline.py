"""Runs a one-time historical backfill of the EnergyZero source into BigQuery.

This is the manual backfill entry point, not the scheduled incremental
pipeline. 

Usage:
    python pipelines/energyzero_pipeline.py
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


def run() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="energyzero_pipeline",
        destination="bigquery",
        dataset_name="staging_energyzero",
    )

    load_info = pipeline.run(
        energyzero_source(
            start_date=BACKFILL_START_DATE,
            end_date=date.today(),
        )
    )

    print(load_info)


if __name__ == "__main__":
    run()