"""
## Greenhouse energy: daily ELT

1. **Extract & load (dlt)**: the four sources run in parallel, each re-loading a
   trailing window of `lookback_days` (merge keys make overlapping reloads safe).
   EnergyZero also loads tomorrow, as day-ahead prices are published ~13:00-15:00.
2. **Transform (dbt via Cosmos)**: every dbt seed, model and test becomes its own task,
   run with the `prod` target once all loads succeed.

dlt and dbt each run in their own virtualenv inside the image (see Dockerfile).
The one-time historical backfills are run manually: `python -m pipelines.<name>` in `dlt/`.
"""

from datetime import timedelta
from pathlib import Path

from airflow.sdk import Param, dag, task
from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import LoadMode
from pendulum import datetime

AIRFLOW_HOME = Path("/usr/local/airflow")
DLT_PROJECT_DIR = str(AIRFLOW_HOME / "project" / "dlt")
DBT_PROJECT_DIR = AIRFLOW_HOME / "project" / "dbt"
DLT_PYTHON = str(AIRFLOW_HOME / "dlt_venv" / "bin" / "python")
DBT_EXECUTABLE = str(AIRFLOW_HOME / "dbt_venv" / "bin" / "dbt")

# Templated per run: the run's date and the trailing window to reload.
# Airflow 3 manual runs have no logical date (so `ds` is undefined); run_after is
# set for every run: the scheduled time, or the trigger time for manual runs.
RUN_DATE = "{{ dag_run.run_after | ds }}"
LOOKBACK_DAYS = "{{ params.lookback_days }}"


@dag(
    start_date=datetime(2026, 10, 1, tz="Europe/Amsterdam"),
    schedule="30 15 * * *",  # after EnergyZero publishes tomorrow's day-ahead prices
    catchup=False,
    max_active_runs=1,
    doc_md=__doc__,
    default_args={"owner": "greenhouse-energy", "retries": 2, "retry_delay": timedelta(minutes=5)},
    params={
        "lookback_days": Param(7, type="integer", minimum=1, maximum=60, description="Days to re-load per source"),
    },
    tags=["greenhouse", "dlt", "dbt"],
)
def greenhouse_energy_daily():
    # dlt tasks run in the dlt venv. Functions passed to external_python are executed
    # in a separate interpreter, so they must import everything they use themselves.

    @task.external_python(python=DLT_PYTHON, expect_airflow=False)
    def load_energyzero(dlt_dir: str, run_date: str, lookback_days: str) -> str:
        import os
        import sys
        from datetime import date, timedelta

        os.chdir(dlt_dir)
        sys.path.insert(0, dlt_dir)
        from pipelines.energyzero_pipeline import run

        end = date.fromisoformat(run_date)
        return run(start_date=end - timedelta(days=int(lookback_days)), end_date=end + timedelta(days=1))

    @task.external_python(python=DLT_PYTHON, expect_airflow=False)
    def load_electricity_maps(dlt_dir: str, run_date: str, lookback_days: str) -> str:
        import os
        import sys
        from datetime import date, timedelta

        os.chdir(dlt_dir)
        sys.path.insert(0, dlt_dir)
        from pipelines.electricity_maps_pipeline import run

        end = date.fromisoformat(run_date)
        return run(start_date=end - timedelta(days=int(lookback_days)), end_date=end)

    @task.external_python(python=DLT_PYTHON, expect_airflow=False)
    def load_open_meteo(dlt_dir: str, run_date: str, lookback_days: str) -> str:
        import os
        import sys
        from datetime import date, timedelta

        os.chdir(dlt_dir)
        sys.path.insert(0, dlt_dir)
        from pipelines.open_meteo_pipeline import run

        end = date.fromisoformat(run_date)
        return run(start_date=end - timedelta(days=int(lookback_days)), end_date=end)

    @task.external_python(python=DLT_PYTHON, expect_airflow=False)
    def load_nager_date(dlt_dir: str, run_date: str) -> str:
        import os
        import sys
        from datetime import date

        os.chdir(dlt_dir)
        sys.path.insert(0, dlt_dir)
        from pipelines.nager_date_pipeline import run

        year = date.fromisoformat(run_date).year
        return run(start_year=year, end_year=year + 1)

    loads = [
        load_energyzero(DLT_PROJECT_DIR, RUN_DATE, LOOKBACK_DAYS),
        load_electricity_maps(DLT_PROJECT_DIR, RUN_DATE, LOOKBACK_DAYS),
        load_open_meteo(DLT_PROJECT_DIR, RUN_DATE, LOOKBACK_DAYS),
        load_nager_date(DLT_PROJECT_DIR, RUN_DATE),
    ]

    dbt_build = DbtTaskGroup(
        group_id="dbt_build",
        project_config=ProjectConfig(DBT_PROJECT_DIR, install_dbt_deps=True),
        profile_config=ProfileConfig(
            profile_name="greenhouse_energy",
            target_name="prod",
            profiles_yml_filepath=DBT_PROJECT_DIR / "profiles.yml",
        ),
        execution_config=ExecutionConfig(dbt_executable_path=DBT_EXECUTABLE),
        render_config=RenderConfig(load_method=LoadMode.DBT_LS),
    )

    loads >> dbt_build


greenhouse_energy_daily()
