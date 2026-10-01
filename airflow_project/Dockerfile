FROM astrocrpublic.azurecr.io/runtime:3.3-2

# dlt and dbt each get their own virtualenv so their dependencies can't
# conflict with Airflow's. The project code itself is mounted at runtime
# (see docker-compose.override.yml), not baked into the image.
COPY requirements-dlt.txt requirements-dbt.txt /tmp/

RUN python -m venv /usr/local/airflow/dlt_venv \
    && /usr/local/airflow/dlt_venv/bin/pip install --no-cache-dir -r /tmp/requirements-dlt.txt

RUN python -m venv /usr/local/airflow/dbt_venv \
    && /usr/local/airflow/dbt_venv/bin/pip install --no-cache-dir -r /tmp/requirements-dbt.txt
