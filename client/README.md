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
- Keyboard controls:
  - `Space`: play/pause
  - `Left/Right`: ±5 minutes
  - `Shift + Left/Right`: ±30 minutes
- Natural-language jumps such as: `"Last Friday at 5pm"`.
- Polyline6 decoding + timestamp interpolation in `src/workers/polylineDecoder.worker.ts`.
