# London Bike Trip Visualizer (Client)

React + Vite client that mirrors the `bikemap.nyc` style architecture:

- **No server-side database**
- **DuckDB WASM** queries Parquet directly over HTTP range requests
- **Deck.gl TripsLayer** animates routes
- **chrono-node** handles natural-language time jumps
- **Polyline decoding runs in a Web Worker**

## Run

```bash
cd client
npm install
npm run dev
```

## Configure Parquet source

The app expects route parquet files with columns:

- `trip_id`
- `start_time`
- `end_time`
- `route_geometry` (Polyline6)
- `route_source` (`osrm`, fallback variants, etc.)
- `route_distance_m`
- `route_duration_s`

Set the glob URL in `.env`:

```bash
VITE_PARQUET_GLOB_URL=https://your-cdn.example.com/trips/*.parquet
```

Without an env var, the app defaults to:

```text
{current-origin}/data/parquet/*.parquet
```

## Implemented features

- DuckDB WASM worker-backed querying.
- 30-minute rolling window prefetch (plus neighbor windows).
- TripsLayer playback with timeline slider + play/pause + 1x/10x/60x speeds.
- Route-source filters: `All`, `OSRM only`, `Fallback only`.
- Trip diagnostics inspector (hover/click) with id, time range, source, point count, distance, duration.
- Keyboard controls:
  - `Space`: play/pause
  - `Left/Right`: ±5 minutes
  - `Shift + Left/Right`: ±30 minutes
- Natural-language jumps such as: `"Last Friday at 5pm"`.
- Polyline6 decoding + timestamp interpolation in `src/workers/polylineDecoder.worker.ts`.
- Live tab (`Live Stations`) using TfL BikePoint API with ~45s refresh for near-real-time station availability.

## Playback troubleshooting quick checks

- Open `http://localhost:5173/data/parquet/manifest.json` and verify `parquet_files` is non-empty.
- If playback fails with a parquet error, ensure files under `public/data/parquet/` are actual `.parquet` binaries.
- Straight lines are usually non-OSRM fallback routes; use `OSRM only` filter to isolate routed trips.
