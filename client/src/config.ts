const DEFAULT_PARQUET_GLOB = `${window.location.origin}/data/parquet/*.parquet`;

export const PARQUET_GLOB_URL =
  import.meta.env.VITE_PARQUET_GLOB_URL ?? DEFAULT_PARQUET_GLOB;

export const MAP_STYLE_URL =
  "https://basemaps.cartocdn.com/gl/positron-gl-style/style.json";

export const LONDON_VIEW_STATE = {
  longitude: -0.1276,
  latitude: 51.5074,
  zoom: 11,
  pitch: 45,
  bearing: 0,
};

export const WINDOW_SIZE_MS = 30 * 60 * 1000;
