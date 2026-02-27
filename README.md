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
python pipeline/london_bike_pipeline.py --month 2024-05
```

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