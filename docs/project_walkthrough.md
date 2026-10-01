# Project walkthrough: Greenhouse Energy Cost Optimization

This document explains how the project is built, why each tool and setting was chosen, which
problems came up and how they were solved, and how to read the results. The README is the
short version; this is the long one.

**Contents**
1. [The question](#1-the-question)
2. [Architecture: which tool does what](#2-architecture-which-tool-does-what)
3. [The data sources](#3-the-data-sources)
4. [dlt: extract and load](#4-dlt-extract-and-load)
5. [BigQuery: the warehouse](#5-bigquery-the-warehouse)
6. [dbt: transformation](#6-dbt-transformation)
7. [Airflow: orchestration](#7-airflow-orchestration)
8. [Challenges and how they were solved](#8-challenges-and-how-they-were-solved)
9. [Results](#9-results)
10. [Is the result desirable?](#10-is-the-result-desirable)
11. [Limitations and next steps](#11-limitations-and-next-steps)

---

## 1. The question

Dutch greenhouses that grow under artificial light (for example tomatoes in winter) use a lot of
electricity. Since electricity on a dynamic contract is priced per hour, and those prices are
published a day ahead, a grower can choose *when* to run the lamps. The project asks:

> If a 4-hectare lit greenhouse in Westland keeps the same number of lamp hours per day but
> runs them in the cheapest dark hours, how much does it save, and what happens to its CO2?

There is no real greenhouse to measure, so the greenhouse is simulated from a heat balance and
a monthly operating profile, driven by real prices, real weather and real grid data.

## 2. Architecture: which tool does what

```
 APIs                     dlt                BigQuery (US)                dbt                       Power BI
 ───────────────          ─────────          ───────────────────          ─────────────────         ─────────
 EnergyZero      ───────► energyzero  ─────► staging_energyzero    ─┐
 Electricity Maps ──────► electricity_maps ► staging_electricity_maps├─► dbt_staging (views)
 Open-Meteo      ───────► open_meteo  ─────► staging_open_meteo    ─┤   dbt_intermediate (tables)
 Nager.Date      ───────► nager_date  ─────► staging_nager_date    ─┘   dbt_marts (tables) ───────► dashboard
                                                                       dbt_seeds (greenhouse profile)
                      └──────────────── Airflow (Astro, Docker) runs all of it daily at 15:30 ────────────────┘
```

| Tool | Role | Why this tool |
|---|---|---|
| **dlt** | Calls the APIs, turns JSON into tables, creates and evolves the BigQuery schema, upserts rows. | Writing loaders by hand means writing schema handling, type inference and merge logic yourself. dlt does that from plain Python generators, so the code is only about *what* to fetch. |
| **BigQuery** | Stores raw and modelled data. | Serverless, free tier is plenty for ~100k rows per table, and Power BI connects to it natively. |
| **dbt Core** | All transformation in SQL: cleaning, joining, the greenhouse model, tests and documentation. | Transformations become version-controlled, tested, dependency-ordered SQL instead of ad-hoc queries. |
| **Airflow (Astro CLI) + Docker** | Runs the loads and the dbt build every day, retries failures, shows history. | Industry-standard orchestrator; Astro CLI gives a ready Airflow in Docker with one command. |
| **Cosmos** | Turns each dbt model and its tests into separate Airflow tasks. | One opaque "dbt build" task hides which model failed. With Cosmos, a failed test shows up as one red task. |
| **Power BI** | Dashboard on the marts. | Not built yet. |

## 3. The data sources

| Source | Tables | Rows (Oct 2026) | Notes |
|---|---|---|---|
| EnergyZero | `electricity_prices`, `gas_prices` | ~67.9k / ~2.95k | All-in consumer prices. Hourly electricity, one gas price per *gas day* (06:00–06:00). No API key. |
| Electricity Maps | `carbon_intensity`, `renewable_percentage`, `carbon_free_percentage`, `total_load`, `day_ahead_price`, `electricity_mix` | ~103k each since 2015 | Academic API key. Hourly. |
| Open-Meteo | `weather` | ~68k since 2019 | Archive API, no key. Naaldwijk (51.99 N, 4.21 E), the centre of Dutch greenhouse horticulture. |
| Nager.Date | `public_holidays` (+ `public_holidays__types`) | ~100 | No key. Includes school/observance days, not only public holidays. |
| Synthetic | dbt seed `greenhouse_monthly_profile` | 12 | Lighting window and heating setpoints per month. |

## 4. dlt: extract and load

### Structure
```
dlt/
  .dlt/config.toml        non-secret config (BigQuery project id)
  .dlt/secrets.toml       Electricity Maps API key (gitignored)
  sources/<api>.py        how to fetch: one dlt source with one or more resources
  pipelines/<api>_pipeline.py   where to load: destination, dataset, date window -> run()
  tests/                  API behaviour tests
```
Sources and pipelines are separated on purpose: the *source* knows the API, the *pipeline*
knows the destination and the date window. The same source is used both for the one-time
historical backfill (`python -m pipelines.<name>`) and for the daily incremental load (Airflow
calls `run(start_date, end_date)`).

Backfills of the multi-year sources (Electricity Maps, Open-Meteo) call `pipeline.run()` once
**per year** instead of once for the whole range. An early single-call backfill of Electricity
Maps was interrupted and lost all progress; per-year runs mean every finished year is safely in
BigQuery even if a later year fails. Incremental runs from Airflow are small, so they use a
single call.

### Patterns used in every source

- **`write_disposition="merge"` with a `primary_key`** (`timestamp_utc`, `datetime`, `date`).
  A merge is an upsert: rows with an existing key are replaced, new keys are inserted. This is
  what makes it safe for Airflow to re-load the last 7 days every day: no duplicates, and
  revised values (provisional weather, re-estimated grid data) overwrite the old ones.
  It is also why dlt creates the `*_staging` datasets you see in BigQuery: it loads into those
  first and then merges into the real tables.
- **Chunking**: APIs limit how much you can ask for at once, so date ranges are split.
  EnergyZero uses 30-day chunks (larger ranges returned server errors in testing), Electricity
  Maps 10-day chunks (its documented limit for hourly data), Open-Meteo one calendar year.
- **Day-level fallback**: when a chunk fails, it is retried one day at a time so one bad day
  doesn't lose the other 29. Days that still fail are logged as "no data" and skipped.
  This was necessary for EnergyZero, which has a real gap on 2019-10-27.
- **Fail fast on auth errors**: Electricity Maps re-raises HTTP 401/403 instead of skipping
  the day. See [challenge 8.9](#89-a-bad-api-key-looked-like-a-successful-load) for why.
- **Authentication**: BigQuery credentials are *not* in any file. dlt falls back to Google's
  default credentials (`gcloud auth application-default login` locally, the same file mounted
  into Airflow). Only the Electricity Maps key is a secret, in `secrets.toml`.

### Per source

**EnergyZero** (`sources/energyzero.py`): uses the `energyzero` Python client with the GraphQL
backend, because the REST backend only supports single dates. Two resources,
`electricity_prices` and `gas_prices`, with `PriceType.ALL_IN` (including taxes). Async client,
wrapped in `asyncio.run` inside the resource.

**Electricity Maps** (`sources/electricity_maps.py`): six signals. Five have the same response
shape, so one factory function creates all five resources. The sixth, the generation mix, uses
the *unfiltered* `electricity-mix` endpoint because the gas-only endpoint returned 401 for the
academic key; the unfiltered one returns every source, including gas, nested under `mix`.
Each signal has its own historical start (most 2015, day-ahead price 2017-04-30). Every
resource clips the requested `start_date` to its own start and skips the call entirely when the
range ends before it, which avoids hundreds of wasted requests when backfilling 2015–2016 for
day-ahead prices. The flat signals keep `created_at` / `updated_at` and the mix keeps
`estimation_method`, so it stays visible when an estimated hour is later replaced by a measured one.

**Open-Meteo** (`sources/open_meteo.py`): six hourly variables (temperature, humidity,
precipitation, wind speed, cloud cover, shortwave radiation). Requested with `timezone=UTC` and
stored as timezone-aware UTC timestamps, see [challenge 8.6](#86-the-weather-was-two-hours-late).
Hours the archive hasn't published yet (all values null) are dropped.

**Nager.Date** (`sources/nager_date.py`): one request per year. Loads the current and next year
daily, so next year's holidays are available in advance. The nested `types` list becomes a
child table automatically (dlt's normalizer turns lists into child tables linked by `_dlt_parent_id`).

## 5. BigQuery: the warehouse

- **Project** `greenhouse-energy-project`, **location US** for every dataset. Moving to EU was
  considered; it isn't possible in place (a dataset's location is fixed at creation) and would
  mean copying or reloading everything. With no personal data involved, the decision was to stay
  in US. What matters is that *all* datasets share one location, because BigQuery cannot join
  across regions.
- **Raw datasets** keep the `staging_<source>` names dlt created. Renaming them to `raw_*` would
  be cleaner, but would force a full reload (Electricity Maps back to 2015), so dbt simply
  declares them as sources.
- **dbt datasets** are separate per environment: `dbt_dev_*` (your local runs) and `dbt_*`
  (Airflow / prod). Development never touches what the dashboard reads.

## 6. dbt: transformation

### Configuration

**`profiles.yml`** lives inside the project (it contains no secrets, so it can be committed):

| Setting | dev | prod | Why |
|---|---|---|---|
| `method` | oauth | oauth | Uses Google default credentials. Works with a personal gcloud login *or* a service-account key via `GOOGLE_APPLICATION_CREDENTIALS`, so no key file path is hard-coded. |
| `dataset` | `dbt_dev` | `dbt` | dbt appends the folder's schema: `dbt_dev_staging`, `dbt_marts`, … |
| `location` | `env_var('BQ_LOCATION', 'US')` | same | One switch if the datasets ever move. |
| `job_execution_timeout_seconds` | 300 | 300 | A stuck query fails instead of hanging the DAG. |
| `job_retries` | 1 | 1 | Retries transient BigQuery errors once. |

**`dbt_project.yml`**: staging is materialized as **views** (cheap, always current, only renames
and casts), intermediate and marts as **tables** (they contain window functions and joins over
~136k rows; materializing them makes the dashboard fast and the tests cheap).

**Vars**: every assumption about the greenhouse is a named var, so the model can be tuned
without touching SQL:

| Var | Value | Meaning / why this value |
|---|---|---|
| `local_timezone` | Europe/Amsterdam | All sources are UTC; schedules, gas days and holidays are local. |
| `spine_start_date` | 2019-01-01 | Start of the price and weather history. |
| `day_start_hour` / `day_end_hour` | 6 / 20 | Day vs. night heating setpoint. |
| `greenhouse_area_m2` | 40 000 | 4 ha, a mid-sized Dutch glasshouse. |
| `lighting_power_w_per_m2` | 100 | Typical installed LED grow-light power. |
| `lighting_radiation_cutoff_w_per_m2` | 200 | Above this outdoor sunlight, lamps add little; they stay off. |
| `base_load_w_per_m2` | 5 | Pumps, fans, screens, climate computer. |
| `heat_loss_w_per_m2_k` | 6 | Heat lost through the glass per degree of indoor–outdoor difference. |
| `solar_heat_gain_fraction` | 0.5 | Share of sunlight that warms the greenhouse. |
| `lighting_heat_fraction` | 0.7 | Share of lamp power that ends up as useful heat. |
| `boiler_efficiency` | 0.9 | Gas boiler. |
| `gas_kwh_per_m3` | 9.77 | Energy content of Dutch (Groningen-quality) gas. |
| `gas_kg_co2_per_m3` | 1.78 | CO2 from burning one m³ of natural gas. |

### Layers

**Staging** (`models/staging/<source>/`), one view per source table. Only "make it clean":
- Consistent names with units in them: `value` becomes `carbon_intensity_gco2eq_per_kwh`,
  `renewable_pct`, `total_load_mw`, …; every hour column is `hour_utc`.
- **Variant columns**: when the API sent the same field sometimes as an integer and sometimes
  as a decimal, dlt kept both, as `mix__wind` (INTEGER) and `mix__wind__v_double` (FLOAT).
  Staging merges them with `coalesce(cast(mix__wind as float64), mix__wind__v_double)`.
- **Gas deduplication**: keep one row per gas day (the latest), and add `valid_from_utc` /
  `valid_to_utc` (06:00 local today → 06:00 local tomorrow, converted to UTC) so hourly data can
  join with a simple range condition. See [challenge 8.5](#85-two-gas-prices-on-some-days).
- **Holidays**: cast the `STRING` date to `DATE`, and fold the child `types` table into an
  `is_public_holiday` flag.

**Seed** `greenhouse_monthly_profile.csv`: the synthetic schedule. Lamps 02:00–18:00
(16 h photoperiod) from November to February, shorter in spring and autumn, off May–August;
heating setpoints 20 °C day / 17 °C night in winter, lower in summer. A seed (a CSV in git) is
right for this: it is small, hand-made reference data that should be reviewable in a diff.

**Intermediate** (`models/intermediate/`), reusable building blocks:
- `int_calendar__hours`: one row per UTC hour from 2019 through tomorrow, with local time.
  Generating the timeline in UTC and *converting* to local is what makes DST correct: the
  spring-forward day has 23 hours and the fall-back day 25, automatically.
- `int_greenhouse__hourly_schedule`: joins every hour to its month's profile, giving
  `is_lighting_scheduled` and `heating_setpoint_c`.
- `int_energy__hourly_prices`: electricity price per hour, and the gas price spread over every
  hour inside its 06:00–06:00 window.
- `int_grid__hourly_signals`: the six Electricity Maps signals on one row per hour.
- `int_greenhouse__hourly_energy_demand`: the core model, explained below.

**The demand model.** For each hour and each scenario:

```
lamps on (baseline)  = lighting scheduled this hour AND outdoor sunlight < 200 W/m²
lamps on (optimized) = same number of lamp hours per day as baseline, placed in the
                       cheapest hours of that day where sunlight < 200 W/m²
                       (days with a missing price keep the baseline)

heat demand (W/m²)   = max(0, heat_loss × (setpoint − outdoor temp)
                              − 0.5 × sunlight
                              − 0.7 × lamp power if lamps on)
electricity (kWh)    = (lamp power + base load) × area / 1000
gas (m³)             = heat demand × area / 1000 / (0.9 × 9.77)
```

The optimized scenario uses a window function: rank each day's dark hours by price
(`row_number() over (partition by date_local ... order by price)`) and switch the lamps on for
the cheapest N, where N is the baseline's lamp hours that day. Keeping N equal is what makes
the comparison fair: the crop gets the same amount of light, only the timing changes. This is
also realistic in one important way: day-ahead prices are published the day before, so a
grower really can plan on them.

The scenarios are stored **long** (one row per hour per scenario) rather than wide. That makes
the SQL for both scenarios identical, and in Power BI the scenario becomes a slicer.

**Marts** (`models/marts/`), what the dashboard reads:

| Model | Grain | Contents |
|---|---|---|
| `dim_date` | local date | year, month, ISO week, weekday, season, weekend, NL public holiday |
| `fct_hourly_energy_market` | hour | prices, CO2 intensity, renewable %, generation mix, weather |
| `fct_greenhouse_hourly_energy` | hour × scenario | kWh, m³, cost (EUR), CO2 (kg) |
| `fct_greenhouse_daily_energy` | local date | baseline and optimized side by side, `cost_savings_eur`, `cost_savings_pct`, `co2_savings_kg`, `is_complete` |

Cost and CO2 are left **null** (not zero) when a price or grid value is missing, so a gap shows
up as a gap instead of as a suspiciously cheap day; `is_complete` flags those days.

### Tests

67 data tests run on every build, 87 nodes in total with the seed and 19 models:
- `unique` + `not_null` on every grain key (`hour_utc`, `gas_day`, `date_day`, and
  `hour_utc + scenario` via `dbt_utils.unique_combination_of_columns`).
- `accepted_range` on percentages (0–100) and gas use (≥ 0).
- `accepted_values` on `scenario`.
- `relationships` from every fact to `dim_date`.
- `expression_is_true`: baseline and optimized have the same lamp hours every day, which is
  the fairness rule of the comparison written down as a test.
- **Source freshness** (warn after 2 days, error after 7) on every timed source.

## 7. Airflow: orchestration

### Project setup (`airflow_project/`)

| File | What it does | Why |
|---|---|---|
| `Dockerfile` | Astro Runtime 3.3 (Airflow 3.3, Python 3.14) plus two virtualenvs: `dlt_venv` and `dbt_venv`. | dlt, dbt and Airflow each pin many libraries. Separate venvs mean they can never break each other. |
| `requirements.txt` | `astronomer-cosmos` only. | The only package Airflow itself needs. |
| `requirements-dlt.txt` / `requirements-dbt.txt` | Top-level packages for each venv. | Small, readable pins, kept in sync with `dlt/` and `dbt/`. |
| `docker-compose.override.yml` | Mounts `../dlt`, `../dbt` and your gcloud credential file into the scheduler, dag-processor and triggerer. Hides the Windows `.venv` folders. | Keeps `dlt/` and `dbt/` as top-level project folders, editable live. Local-dev only. |
| `.env` (from `.env.example`) | `GOOGLE_APPLICATION_CREDENTIALS`, `BQ_LOCATION`, parse timeouts. | Configuration without code changes. |
| `tests/dags/test_dag_integrity.py` | DAGs import without errors, have tags, have retries ≥ 2. | Catches broken DAGs before they reach the scheduler. |

### The DAG: `greenhouse_energy_daily`

```
load_energyzero ────────┐
load_electricity_maps ──┤
load_open_meteo ────────┼──► dbt_build (Cosmos task group: 40 tasks, one run + one test per model)
load_nager_date ────────┘
```

| Parameter | Value | Why |
|---|---|---|
| `schedule` | `30 15 * * *` | Day-ahead prices for tomorrow appear around 13:00–15:00. Running at 15:30 loads them the same day. |
| `start_date` | 2026-10-01, `tz="Europe/Amsterdam"` | The timezone makes 15:30 mean Amsterdam time all year, across DST. |
| `catchup` | False | History is loaded by the backfill scripts; Airflow should not create one run per missed day. |
| `max_active_runs` | 1 | Two runs merging into the same tables at once would compete. |
| `retries` / `retry_delay` | 2 / 5 min | Public APIs fail transiently; a retry 5 minutes later usually works. |
| `params.lookback_days` | 7 (1–60) | Each run re-loads the last 7 days, which overwrites provisional data and heals a few missed days automatically. Can be raised in the UI's "Trigger with config" to repair a longer outage. |
| `tags` | greenhouse, dlt, dbt | Filtering in the UI; also required by the integrity test. |
| `RUN_DATE` | `{{ dag_run.run_after \| ds }}` | The run's date. Not `{{ ds }}`, see [challenge 8.11](#811-the-first-real-dag-run-failed-in-3-seconds). |

**Load windows per source**:

| Task | Window | Why |
|---|---|---|
| `load_energyzero` | run date − 7 days … run date + 1 | +1 loads tomorrow's day-ahead prices. |
| `load_electricity_maps` | run date − 7 … run date | Within the API's 10-day limit, so one request per signal. |
| `load_open_meteo` | run date − 7 … run date | The archive revises recent days. |
| `load_nager_date` | current year … next year | Holidays are per year; next year's are known in advance. |

**dlt tasks** use `@task.external_python(python=dlt_venv, expect_airflow=False)`. The function
runs in the dlt venv's interpreter, so it imports everything inside itself, changes into the
mounted `dlt/` folder (so dlt finds `.dlt/config.toml` and `secrets.toml`) and calls the
pipeline's `run()`. `expect_airflow=False` tells Airflow the venv deliberately has no Airflow.

**dbt tasks** come from a Cosmos `DbtTaskGroup`:

| Cosmos setting | Value | Why |
|---|---|---|
| `ProjectConfig(..., install_dbt_deps=True)` | mounted `dbt/` | Installs `dbt_utils` automatically, so a fresh clone works. |
| `ProfileConfig` | profile `greenhouse_energy`, target `prod`, the project's `profiles.yml` | Reuses the same profile as local development, so there is one source of truth. |
| `ExecutionConfig(dbt_executable_path=...)` | `dbt_venv/bin/dbt` | Runs dbt from its own venv. |
| `RenderConfig(load_method=LoadMode.DBT_LS)` | `dbt ls` at parse time | Builds the task graph from the real project, including seeds and tests, with caching. |

The task group runs only if all four loads succeed: transforming half-loaded data would produce
a dashboard that looks right but isn't.

## 8. Challenges and how they were solved

Each item follows the same order: what was seen, how it was investigated, the cause, the fix,
and why that fix.

### 8.1 The warehouse couldn't be inspected from the shell
**Seen:** `bq ls` failed with "python3.13: command not found".
**Investigation:** read the `bq` launcher script; it is a Unix shell script that looks for an
executable called `python3.13`, which Windows doesn't have.
**Fix:** use the Windows launcher `bq.cmd` from PowerShell. A second trap: PowerShell 5.1 cuts
multi-line arguments to programs at the first newline, which silently truncated a query and
made it look like some datasets were empty. Queries are now collapsed to one line.
**Why it mattered:** the first review of the project was based on code alone and drew several
wrong conclusions (sources "missing", values "89% null"). Looking at the real data corrected all
of them. Lesson: check the data, not only the code.

### 8.2 Code for two sources was missing
**Seen:** BigQuery contained Nager.Date and Open-Meteo data, but the project folder had no code
for them.
**Investigation:** dlt stores its schema in BigQuery (`_dlt_version`), including table names,
columns, types, primary keys and write dispositions. That was enough to rebuild both sources so
they produce exactly the same tables. The weather location wasn't stored anywhere, so stored
temperatures were compared hour by hour against Open-Meteo for eight candidate locations;
Naaldwijk (Westland) matched exactly.
**Outcome:** the original project folder was found later and confirmed the reconstruction
(same tables, same location).

### 8.3 dlt "variant" columns
**Seen:** `electricity_mix` had both `mix__wind` and `mix__wind__v_double`.
**Cause:** the API returned some values as integers and others as decimals. dlt doesn't silently
change a column's type, so it put the decimals in a second column.
**Fix:** `coalesce` the pair in staging. **Why there:** staging is exactly the place to hide
loader quirks, so no downstream model ever needs to know.

### 8.4 A misdiagnosis: "day-ahead price is 89% empty"
**Seen:** 56k of 64k `day_ahead_price.value` rows were null.
**Investigation:** the table also had `value__v_double`; counting rows where *both* columns are
null gave zero. Same cause as 8.3.
**Lesson:** after the first variant column is found, check every table for them before drawing
conclusions. The real limitation is ~17.5k missing *hours*, so this signal is used only as a
cross-check against EnergyZero.

### 8.5 Two gas prices on some days
**Seen:** 125 gas days had two rows, and on 103 the prices differed slightly.
**Investigation:** listing the duplicates showed they fall exactly every 30 days, the
backfill's chunk size. The extra row is always stamped 00:00 UTC; the normal one 04:00/05:00 UTC
(06:00 local).
**Cause:** a chunk boundary in the backfill picked up an extra midnight row.
**Fix:** keep the latest row per local gas day (the 06:00 one), and test that `gas_day` is unique.

### 8.6 The weather was two hours late
**Seen:** while identifying the weather location, the best-matching location's temperatures were
the stored ones shifted by exactly two hours. The shift was the same in January, March and July.
**Cause (confirmed later in the original code):** the original source requested
`timezone=Europe/Amsterdam`. Open-Meteo then returns local times *without* an offset, and they
were stored as if they were UTC.
**Fix:** request `timezone=UTC`, store explicit UTC timestamps, re-run the full backfill. Because
the table merges on `datetime`, every wrong row was overwritten (all ~68k came from the new load),
and a recheck showed zero difference from the API.
**Why it mattered:** the demand model switches lamps off when the sun is strong. With sunlight two
hours late, lamps were planned against the wrong sunshine. After the fix, the 2019 CO2 result
flipped from −32 t to +56 t, and yearly savings rose by 0.4–1.1 percentage points.

### 8.7 Time zones and daylight saving time
**Problem:** prices and grid data are hourly UTC, gas days start at 06:00 local, schedules and
holidays are local, and local days have 23 or 25 hours twice a year.
**Decision:** store and join everything in UTC (`hour_utc` is the key everywhere); convert to
local only to *look up* local rules (which month, which hour of the day, which gas day).
Verified: 2025-03-30 has 23 hours and 2025-10-26 has 25 in the schedule.

### 8.8 The project didn't install in Docker
- `dlt/requirements.txt` was saved as UTF-16 (a Windows `pip freeze >` quirk), so pip on Linux
  couldn't read it, and it pinned `pywin32`, a Windows-only package. Fixed by re-saving as UTF-8
  and adding `; sys_platform == "win32"` to `pywin32`.
- `astro dev start` said "not an Astro project" because the root `.gitignore` excluded the whole
  `.astro/` folder, including `config.yaml`, which must be committed. Recreated it and removed
  that ignore rule.

### 8.9 A bad API key looked like a successful load
**Seen:** the Electricity Maps task succeeded, but no new rows arrived.
**Investigation:** the log showed every day listed as "no data". A direct request with the key
(read the same way the pipeline does, never printed) returned data fine.
**Cause:** `.env` contained `SOURCES__ELECTRICITY_MAPS__API_KEY=` with an empty value.
Environment variables take precedence over `secrets.toml` in dlt, so the key was empty, every
request got 401, and the day-level fallback treated each 401 as a missing day.
**Fix:** remove the empty line (the key now comes only from `secrets.toml`), and change the source
to re-raise 401/403. **Why the second fix:** "the data doesn't exist" and "we are not allowed to
ask" are different failures. Only the first should be skipped; the second must turn the task red.

### 8.10 The DAG timed out while parsing
**Seen:** `DagBag import timeout ... after 30.0s`.
**Investigation:** the traceback ended in `os.walk` inside Cosmos's cache code. Cosmos hashes the
dbt folder to know when to re-run `dbt ls`, and the mounted folder included the Windows
`dbt/.venv` with thousands of files, read slowly through the Docker mount.
**Fix:** hide both `.venv` folders inside the containers with empty anonymous volumes, and raise
the parse timeout to 120 s for the first, uncached `dbt ls`. Parsing now takes about 20 s once, then
comes from cache.

### 8.11 The first real DAG run failed in 3 seconds
**Seen:** all four loads failed with `'ds' is undefined`, although `airflow tasks test` had
passed.
**Cause:** in Airflow 3, manually triggered runs have no logical date, so `{{ ds }}` doesn't exist.
`tasks test` supplies a date explicitly, which hid the problem.
**Fix:** use `{{ dag_run.run_after | ds }}`; `run_after` exists for every run. **Lesson:** a
test that passes inputs the real system doesn't pass can hide bugs, so the only proof is a real
triggered run. The next run succeeded with all 44 tasks in 2.9 minutes.

### 8.12 A gap the daily run couldn't heal
**Seen:** after the key was fixed, Electricity Maps data still stopped on 2026-08-04, then jumped
to the last 7 days.
**Cause:** the daily window is 7 days; the data had been stale for 8 weeks.
**Fix:** one manual catch-up from 2026-08-01, then a completeness check (rows = expected hours:
103 000 = 103 000). For a future outage, raise `lookback_days` when triggering the DAG.

### 8.13 Decisions that were considered and rejected
- **Rename raw datasets to `raw_*`**: cleaner, but needs a full reload. Rejected.
- **Move to EU**: needs copying or reloading every dataset. Rejected; all datasets stay in US.
- **Move `dlt/` and `dbt/` into `airflow_project/include/`**: the usual Astro layout, and needed
  for a hosted deployment, but it would bury two of the three main layers. For local development,
  mounting was chosen; for deployment, the code would be copied into the image.
- **Optimize on CO2 instead of price**: possible, but the project's question is about cost, and
  the CO2 effect is reported alongside it.

## 9. Results

### The pipeline
- A daily run takes **~3 minutes**: loads 21–40 s each in parallel, then dbt.
- **87/87** dbt nodes pass (1 seed, 19 models, 67 tests); the DAG integrity tests pass.
- Data is current to the day: prices, weather and grid data to today, holidays through 2027.

### The greenhouse (4 ha, 2019 – Sep 2026)

| Year | Gas m³/m² | Electricity kWh/m² | Baseline cost | Optimized saves | CO2 saved | Baseline CO2 |
|---|---|---|---|---|---|---|
| 2019 | 14.7 | 312 | €2.53M | €95k (3.8%) | 56 t | 6 676 t |
| 2020 | 13.5 | 316 | €2.41M | €94k (3.9%) | 111 t | 5 869 t |
| 2021 | 16.3 | 310 | €4.00M | €280k (7.0%) | 74 t | 6 194 t |
| 2022 | 13.9 | 304 | €4.65M | €390k (8.4%) | 52 t | 5 438 t |
| 2023 | 13.7 | 315 | €4.40M | €203k (4.6%) | 67 t | 4 814 t |
| 2024 | 13.0 | 315 | €4.09M | €189k (4.6%) | 85 t | 4 539 t |
| 2025 | 14.6 | 305 | €4.10M | €198k (4.8%) | 99 t | 4 695 t |

Over all complete days:
- Electricity is **84%** of the energy bill, which is why the lamps are the lever worth optimizing.
- The average price paid per lamp-hour drops from **€0.256 to €0.236/kWh** (−8%), and the average
  carbon intensity of lamp-hours from **359 to 351 g/kWh**.
- Of 2 825 complete days, the optimized schedule is cheaper on **1 705**, equal on 997 (982 of
  them summer days without any lighting) and **more expensive on 123**.
- It emits less CO2 on 1 128 days and more on 700.

## 10. Is the result desirable?

**Mostly yes, with clear caveats.**

What is good:
- **The savings are real and implementable.** 4–8% of a multi-million energy bill per year is
  €95k–€390k, from changing *when* the lamps run, not *how much*. Day-ahead prices are known a day
  in advance, so a grower can actually plan this; it is not hindsight.
- **It pays most when it matters most.** Savings peaked in 2021–22 (7–8.4%), when prices were
  highest and most volatile. Volatility is what creates savings.
- **Cheaper is slightly cleaner, over a year.** Cheap hours are often windy or sunny hours, so the
  optimized schedule cut CO2 every year (52–111 t, about 1–2%). This was hidden until the weather
  timestamp bug was fixed.

What should not be over-read:
- **Absolute costs are too high.** All-in consumer tariffs include energy tax and VAT a business
  doesn't pay at that rate. The savings are less distorted (the per-kWh tax cancels out between
  scenarios, VAT doesn't), but the €M baseline is not a real grower's bill.
- **The greenhouse is a model, not a measurement.** Gas use (13–16 m³/m²) is below typical Dutch
  values because CO2 dosing, humidity control and CHPs are not modelled. Electricity use
  (~310 kWh/m²) is plausible for a fully LED-lit crop.
- **Not every day improves.** On 123 days the "optimized" schedule cost more, which is a flaw
  rather than a feature: the optimizer ranks hours by *electricity* price only, but moving the
  lamps also moves their heat, which changes how much gas the boiler burns. On those days the
  extra gas outweighs the cheaper electricity. On 700 days it emitted more CO2, because price and
  carbon don't always line up.
- **The dark period isn't enforced.** Lamp hours don't have to be consecutive, which slightly
  flatters the savings.

So: the direction and order of magnitude are credible and useful for the portfolio story, and
the data platform underneath is sound. The numbers are an *indication* of the opportunity, not
a forecast of a specific grower's savings.

## 11. Limitations and next steps

Limitations that can't be fixed with the available data are listed in the README (consumer
tariffs, source gaps, provisional recent data, one weather location, simulated greenhouse).

Fixable next steps, in order of value:
1. **Optimize on total cost**: rank hours by electricity cost minus the value of the lamp heat
   (gas it saves). This should remove the 123 loss days.
2. **Enforce a contiguous dark period** (for example at least 6–8 hours without light).
3. **Calibrate the model** against sector figures (gas per m², electricity per m²) using the
   physical vars, without code changes.
4. **Power BI dashboard** on `dbt_marts`: savings over time, price vs. lamp schedule for a chosen
   day, CO2 trade-off.
5. **Hosted deployment**: copy `dlt/` and `dbt/` into the image and switch to a service-account key.
