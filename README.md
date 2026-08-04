# Greenhouse Energy Cost Optimization

Portfolio project simulating energy-cost optimization for a Dutch commercial greenhouse.

## Stack
- dlt — extraction & loading
- BigQuery — data warehouse
- dbt Core — transformation
- Airflow (via Astro CLI) + Docker — orchestration
- PowerBI — dashboard

## Data Sources
- EnergyZero (electricity + gas prices, NL)
- Electricity Maps (carbon intensity + renewable %, NL zone)
- Open-Meteo (weather)
- Nager.Date (NL public holidays)
- Synthetic greenhouse usage schedule