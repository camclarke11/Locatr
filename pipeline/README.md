# London Santander Data Pipeline

This pipeline builds a **DuckDB-friendly Parquet lake** for historical Santander cycle trips.

## What it does

1. Discovers monthly-relevant CSV/ZIP links from the TfL usage stats index.
2. Downloads and reads raw journey CSVs.
3. Cleans and standardizes:
   - station names
   - station coordinates (median coordinate per station name)
4. Requests bike routes from OSRM (`/route/v1/bicycle`) and stores route geometry as **Polyline6**.
5. Writes one Parquet file per day:
   - `trip_id`
   - `start_time`
   - `end_time`
   - `route_geometry`

## Install

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r pipeline/requirements.txt
```

## Run for one month

```bash
python pipeline/london_bike_pipeline.py \
  --month 2024-05 \
  --output-dir pipeline/output/parquet \
  --route-cache pipeline/output/route_cache.parquet
```

### Useful flags

- `--max-trips 10000` for quick smoke tests.
- `--max-new-routes 5000` to cap OSRM calls in one run.
- `--osrm-qps 5` to be gentler with shared/public OSRM instances.
- `--osrm-url http://localhost:5000` to use your own OSRM backend.

## Output layout

```text
pipeline/output/
  route_cache.parquet
  parquet/
    2024-05-01.parquet
    2024-05-02.parquet
    ...
    manifest.json
```

`manifest.json` includes the trip count and date bounds for the run.
