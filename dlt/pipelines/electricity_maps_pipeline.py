"""Runs a one-time historical backfill of all 6 Electricity Maps signals
into BigQuery.

Manual backfill entry point, matching the pattern used for
energyzero_pipeline.py. Each signal's start_date is fixed inside
sources/electricity_maps.py (they differ per signal); this script only
supplies end_date, computed fresh each run.

Usage:
    python -m pipelines.electricity_maps_pipeline
"""

import logging
from datetime import date

import dlt

from sources.electricity_maps import electricity_maps_source

logging.basicConfig(level=logging.INFO)


def run() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="electricity_maps_pipeline",
        destination="bigquery",
        dataset_name="staging_electricity_maps",
    )

    load_info = pipeline.run(electricity_maps_source(end_date=date.today()))

    print(load_info)


if __name__ == "__main__":
    run()