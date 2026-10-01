# Greenhouse Energy Cost Optimization

Portfolio project simulating energy-cost optimization for a Dutch commercial greenhouse:
how much could a lit greenhouse in Westland save by moving its grow-light hours to the
cheapest hours of the dynamic (day-ahead) electricity market, and what does that do to its
CO2 footprint?

A full walkthrough of the design, the problems hit along the way and how they were solved is
in [docs/project_walkthrough.md](docs/project_walkthrough.md).

## Stack
- dlt — extraction & loading
- BigQuery — data warehouse
- dbt Core — transformation
- Airflow (via Astro CLI) + Docker — orchestration
- PowerBI — dashboard

## Data Sources
| Source | What | Grain | Since |
|---|---|---|---|
| EnergyZero | all-in electricity + gas prices, NL | hourly / per gas day | 2019 |
| Electricity Maps | carbon intensity, renewable %, generation mix, load, day-ahead price (NL zone) | hourly | 2015 |
| Open-Meteo | weather at Naaldwijk (Westland) | hourly | 2019 |
| Nager.Date | NL public holidays | daily | 2019 |
| Synthetic greenhouse schedule | monthly lighting/heating profile (dbt seed) | monthly → hourly | — |

## Project layout
```
dlt/              extract & load: one source + pipeline per API -> BigQuery datasets staging_*
dbt/              staging -> intermediate -> marts, plus the synthetic greenhouse seed
airflow_project/  Astro project: daily DAG running the dlt loads, then dbt via Cosmos
docs/             project walkthrough
```

### dbt models
| Layer | Models |
|---|---|
| staging | one view per source table (`stg_<source>__<table>`): renames, types, dedup |
| intermediate | hourly spine, hourly prices (gas mapped onto its 06:00–06:00 day), grid signals, greenhouse schedule and energy demand (baseline vs. price-optimized lighting) |
| marts | `dim_date`, `fct_hourly_energy_market`, `fct_greenhouse_hourly_energy` (hour × scenario), `fct_greenhouse_daily_energy` (baseline vs. optimized, savings) |

The greenhouse's physical assumptions (area, lamp power, heat loss, boiler efficiency, …)
are dbt vars in `dbt/dbt_project.yml`.

## Results (4 ha lit greenhouse, 2019 – Sep 2026)
| Year | Baseline cost | Optimized saves | CO2 saved |
|---|---|---|---|
| 2019 | €2.53M | €95k (3.8%) | 56 t |
| 2020 | €2.41M | €94k (3.9%) | 111 t |
| 2021 | €4.00M | €280k (7.0%) | 74 t |
| 2022 | €4.65M | €390k (8.4%) | 52 t |
| 2023 | €4.40M | €203k (4.6%) | 67 t |
| 2024 | €4.09M | €189k (4.6%) | 85 t |
| 2025 | €4.10M | €198k (4.8%) | 99 t |

Shifting the same number of lamp hours to the cheapest dark hours saves 4–8% of the energy
bill per year, most in the 2021–22 energy crisis when prices swung hardest, and slightly
lowers CO2 because cheap hours tend to be windy/sunny hours. See the walkthrough for how
to read these numbers and why they are an indication rather than a forecast.

## Running it

### dlt (backfill)
```powershell
cd dlt
python -m venv .venv; .venv\Scripts\pip install -r requirements.txt
.venv\Scripts\python -m pipelines.energyzero_pipeline        # also: electricity_maps / open_meteo / nager_date
```
BigQuery auth uses `gcloud auth application-default login`. The Electricity Maps key goes in
`dlt/.dlt/secrets.toml` (gitignored) under `[sources.electricity_maps] api_key`.

### dbt
```powershell
cd dbt
python -m venv .venv; .venv\Scripts\pip install -r requirements.txt
.venv\Scripts\dbt deps; .venv\Scripts\dbt build              # dev target -> dbt_dev_* datasets
```

### Airflow (local)
```powershell
cd airflow_project
copy .env.example .env
astro dev start             # UI at http://localhost:8080
```
DAG `greenhouse_energy_daily` runs at 15:30 Europe/Amsterdam (after day-ahead prices are
published): the four dlt loads re-load the last `lookback_days` (default 7) in parallel,
then every dbt model and test runs as its own task (Cosmos, `prod` target → `dbt_*` datasets).
A full run takes about 3 minutes.

`dlt/` and `dbt/` (including `dlt/.dlt/secrets.toml`) and your gcloud login are mounted into
the containers via `docker-compose.override.yml`, so this setup is for local development;
a hosted deployment would copy the code into the image and use a service-account key.

## Known limitations
These follow from the available data or from simulating the greenhouse, not from bugs.

**Data**
- **Consumer tariffs.** EnergyZero only publishes all-in consumer prices (incl. energy tax and VAT).
  A commercial grower pays much lower tax rates and no VAT, so absolute costs are overstated.
  Savings are less affected: both scenarios use the same kWh, so the per-kWh energy tax cancels
  out, but VAT still inflates them by roughly a fifth.
- **Source gaps.** EnergyZero has no electricity prices for 2019-10-27 and is missing 2 gas days;
  Electricity Maps' day-ahead price misses ~17.5k hours (it is only used as a cross-check).
  Days with missing prices are flagged `is_complete = false` in `fct_greenhouse_daily_energy`.
- **Recent data is provisional.** Open-Meteo and Electricity Maps revise the latest days; the
  daily DAG re-loads a trailing window so these values get overwritten.
- **One weather location.** Weather is taken at Naaldwijk (Westland, 51.99 N 4.21 E).

**Simulated greenhouse**
- There is no metered greenhouse: energy use comes from a steady-state heat balance and a
  fixed monthly lighting profile, not from measurements.
- Gas is only used for space heating. Real growers also burn gas for CO2 dosing and humidity
  control, and many run a CHP that produces their own electricity; none of this is modelled,
  so gas use (13–16 m³/m²/yr) is below typical Dutch values.
- The optimized scenario moves lamp hours to the cheapest dark hours of the day without
  requiring them to be contiguous, so a crop's minimum uninterrupted dark period is not enforced.
- Price and CO2 don't always align: on ~38% of lit days the cheapest schedule emits more CO2
  than the baseline, even though it is lower over each full year.

## Next steps
- Optimize on total cost (electricity + the gas the lamps' heat saves), not electricity price
  alone: on 123 days the current "optimized" schedule costs more than the baseline.
- Enforce a contiguous dark period in the optimizer.
- Power BI dashboard on the `dbt_marts` tables.
