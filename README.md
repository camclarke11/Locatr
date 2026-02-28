# London Bike Trip Visualizer (Santander Cycles)

This repository contains two parts:

- `pipeline/` — Python data pipeline that downloads TfL journey CSVs, routes trips with OSRM, and exports daily Polyline6 Parquet files.
- `client/` — React + Deck.gl + DuckDB WASM browser app that queries Parquet directly from static storage/CDN.

## Quick start

### 1) Build Parquet data

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r pipeline/requirements.txt
python pipeline/london_bike_pipeline.py --month 2024-05 --max-trips 10000 --max-new-routes 5000
```

Fast-demo notes:
- `--max-trips` now defaults to a **uniform month sample** (`--max-trips-strategy uniform`) so the sample is not biased to only earliest days.
- Output `manifest.json` now lists **all parquet day files** in `output-dir`, which keeps wildcard client loading consistent across repeated runs.

### 2) Run the client

```bash
cd client
npm install
npm run dev
```

Set your remote parquet URL:

```bash
VITE_PARQUET_GLOB_URL=https://your-cdn.example.com/trips/*.parquet
```

## Recommended build modes

### Fast demo (minutes)

```bash
python pipeline/london_bike_pipeline.py --month 2024-05 --max-trips 10000 --max-new-routes 5000
```

### Fuller quality per month (longer)

```bash
python pipeline/london_bike_pipeline.py --month 2024-05 --max-new-routes 20000
```

### Month-range backfill (resume-friendly)

```bash
python pipeline/backfill_range.py --start-month 2024-05 --end-month latest --resume
```

Add `--max-trips` and `--max-new-routes` to the range runner for faster demo backfills, or remove caps for maximum route realism.