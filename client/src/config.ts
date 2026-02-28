const DEFAULT_PARQUET_GLOB = `${window.location.origin}/data/parquet/*.parquet`;

export const PARQUET_GLOB_URL =
  import.meta.env.VITE_PARQUET_GLOB_URL ?? DEFAULT_PARQUET_GLOB;

export const DARK_MAP_STYLE_URL =
  "https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json";
export const LIGHT_MAP_STYLE_URL =
  "https://basemaps.cartocdn.com/gl/positron-gl-style/style.json";

export const LONDON_VIEW_STATE = {
  longitude: -0.1276,
  latitude: 51.5074,
  zoom: 11.4,
  pitch: 34,
  bearing: -16,
};

export const WINDOW_SIZE_MS = 30 * 60 * 1000;
export const LIVE_STATION_REFRESH_MS = 45 * 1000;
export const TUBE_REFRESH_MS = 30 * 1000;
export const TUBE_REQUEST_TIMEOUT_MS = 15 * 1000;
export const TUBE_HISTORY_DEFAULT_SPEED = 30;
export const TUBE_HISTORY_DEFAULT_DATE = new Date().toISOString().slice(0, 10);

export const TFL_APP_ID = import.meta.env.VITE_TFL_APP_ID ?? "";
export const TFL_APP_KEY = import.meta.env.VITE_TFL_APP_KEY ?? "";
