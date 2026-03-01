import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import DeckGL from "@deck.gl/react";
import { TripsLayer } from "@deck.gl/geo-layers";
import { IconLayer, PathLayer, ScatterplotLayer, SolidPolygonLayer } from "@deck.gl/layers";
import MapView, { type MapRef } from "react-map-gl/maplibre";
import type { Layer, MapViewState, PickingInfo } from "@deck.gl/core";
import type { Map as MapLibreMap, StyleSpecification } from "maplibre-gl";
import { GL } from "@luma.gl/constants";

import {
  DARK_MAP_STYLE_URL,
  LIGHT_MAP_STYLE_URL,
  LIVE_STATION_REFRESH_MS,
  LONDON_VIEW_STATE,
  TUBE_HISTORY_DEFAULT_DATE,
  TUBE_HISTORY_DEFAULT_SPEED,
  TUBE_REFRESH_MS,
} from "./config";
import { PlaybackControls } from "./components/PlaybackControls";
import { useTripPlayback } from "./hooks/useTripPlayback";
import { fetchBikePointStations, type BikePointStation } from "./lib/bikePointClient";
import {
  fetchTubeArrivals,
  fetchTubeLiveActions,
  fetchTubeLineStatuses,
  fetchTubeTimetable,
  fetchTubeTopology,
  type TubeTopologyProgressEvent,
  type TubeTimetableResponse,
} from "./lib/tubeClient";
import {
  buildTubeHistoricalFrame,
  buildTubeLiveTrainMarkersFromActions,
  buildTubeLiveTrainMarkers,
  mergeLineSegmentDurations,
} from "./lib/tubeSimulation";
import type {
  DecodedTrip,
  TubeArrival,
  TubeDirection,
  TubeHistoricalFrame,
  TubeLiveActionsByLine,
  TubeLineMeta,
  TubeLineStatus,
  TubeLiveTrainMarker,
  TubeMode,
  TubeStation,
} from "./types";

import "./App.css";

type RouteFilterMode = "all" | "osrm" | "fallback";
type AppMode = "playback" | "live" | "tube";
type ThemeMode = "dark" | "light";
type CinematicTourId = "thames-loop" | "ride-rush" | "tube-radar";
type ColorTrip = Pick<DecodedTrip, "tripId" | "routeSource">;
type ArrowCategory = "ebike" | "classic" | "unlocked" | "docked";
type ArrowMarker = {
  id: string;
  tripId: string;
  position: [number, number];
  angleDeg: number;
  color: [number, number, number, number];
  category: ArrowCategory;
};
type PulseMarker = {
  id: string;
  position: [number, number];
  color: [number, number, number, number];
  radius: number;
};
type TubeLineBranchOption = {
  key: string;
  stationIds: string[];
  label: string;
};
type TubeDepartureBoardEntry = {
  id: string;
  lineId: string;
  lineName: string;
  lineColor: [number, number, number];
  destination: string;
  etaSeconds: number;
  etaLabel: string;
  expectedTimeLabel: string;
};
type CameraPose = {
  longitude: number;
  latitude: number;
  zoom: number;
  pitch: number;
  bearing: number;
};
type CinematicTourKeyframe = CameraPose & {
  durationMs: number;
};
type CinematicTourDefinition = {
  id: CinematicTourId;
  label: string;
  subtitle: string;
  keyframes: CinematicTourKeyframe[];
};
const THEME_STORAGE_KEY = "locatr.theme";
const THREE_D_BUILDINGS_LAYER_ID = "locatr-3d-buildings";
const THREE_D_MIN_ZOOM = 13;
const THREE_D_TARGET_PITCH = 58;
const THREE_D_TARGET_BEARING = -16;
const THREE_D_MIN_ZOOM_TARGET = 12.8;
const EVENT_PULSE_WINDOW_MS = 14_000;
const ARROW_LEAD_SEGMENTS = 0.08;
const TRIP_TRAIL_LENGTH_SECONDS = 180;
const TRIP_TRAIL_OPACITY = 0.24;
const TRIP_TRAIL_MIN_WIDTH_PX = 3;
const FOLLOW_BLEND_FACTOR = 0.2;
const FOLLOW_LOOKAHEAD_SEGMENTS = 0.28;
const CINEMATIC_MIN_SEGMENT_MS = 750;
const TUBE_LIVE_CLOCK_FRAME_MS = 33;
const TUBE_HISTORY_EASE_MULTIPLIER = 2;
const TUBE_HISTORY_EASE_IN_MS = 420 * TUBE_HISTORY_EASE_MULTIPLIER;
const TUBE_HISTORY_EASE_OUT_MS = 560 * TUBE_HISTORY_EASE_MULTIPLIER;
const FOCUS_DIM_OVERLAY_DARK: [number, number, number, number] = [0, 0, 0, 150];
const FOCUS_DIM_OVERLAY_LIGHT: [number, number, number, number] = [255, 255, 255, 118];
const DIM_OVERLAY_POLYGON = [
  [
    [-180, -90],
    [-180, 90],
    [180, 90],
    [180, -90],
    [-180, -90],
  ],
];

const TRIP_PALETTE: Array<[number, number, number]> = [
  [126, 201, 255],
  [101, 232, 205],
  [181, 151, 255],
  [121, 163, 255],
  [88, 214, 255],
];
const ARROW_ICON_ATLAS = `data:image/svg+xml;charset=utf-8,${encodeURIComponent(
  `<svg xmlns="http://www.w3.org/2000/svg" width="128" height="128" viewBox="0 0 128 128">
    <polygon points="24,28 112,64 24,100" fill="white"/>
  </svg>`,
)}`;
const ARROW_ICON_MAPPING = {
  arrow: {
    x: 0,
    y: 0,
    width: 128,
    height: 128,
    anchorX: 24,
    anchorY: 64,
    mask: true,
  },
} as const;
const TRAIN_BODY_ICON_ATLAS = `data:image/svg+xml;charset=utf-8,${encodeURIComponent(
  `<svg xmlns="http://www.w3.org/2000/svg" width="192" height="96" viewBox="0 0 192 96">
    <path d="M24 48C24 31 38 16 56 16H126C146 16 163 30 169 48C163 66 146 80 126 80H56C38 80 24 65 24 48Z" fill="white"/>
    <rect x="150" y="30" width="16" height="36" rx="8" fill="white"/>
  </svg>`,
)}`;
const TRAIN_BODY_ICON_MAPPING = {
  train: {
    x: 0,
    y: 0,
    width: 192,
    height: 96,
    anchorX: 96,
    anchorY: 48,
    mask: true,
  },
} as const;
const TRAIN_NOSE_ICON_ATLAS = `data:image/svg+xml;charset=utf-8,${encodeURIComponent(
  `<svg xmlns="http://www.w3.org/2000/svg" width="96" height="96" viewBox="0 0 96 96">
    <polygon points="34,30 66,48 34,66" fill="white"/>
  </svg>`,
)}`;
const TRAIN_NOSE_ICON_MAPPING = {
  nose: {
    x: 0,
    y: 0,
    width: 96,
    height: 96,
    anchorX: 48,
    anchorY: 48,
    mask: true,
  },
} as const;
const ARROW_COLORS: Record<ArrowCategory, [number, number, number, number]> = {
  ebike: [110, 196, 255, 230],
  classic: [183, 146, 255, 230],
  unlocked: [86, 224, 168, 230],
  docked: [255, 126, 173, 230],
};
const TUBE_TRAIN_COLOR: [number, number, number, number] = [255, 236, 92, 250];
const TUBE_TRAIN_GLOW_COLOR: [number, number, number, number] = [255, 132, 48, 172];
const TUBE_LIVE_TRAIN_SHADOW_COLOR: [number, number, number, number] = [5, 9, 18, 178];
const TUBE_LIVE_TRAIN_OUTLINE_COLOR: [number, number, number, number] = [230, 238, 250, 210];
const TUBE_LIVE_TRAIN_NOSE_COLOR: [number, number, number, number] = [12, 18, 32, 228];
const TUBE_STATION_BASE_COLOR: [number, number, number, number] = [78, 107, 147, 180];
const TUBE_STATUS_COLORS: Record<number, [number, number, number, number]> = {
  10: [58, 176, 103, 215],
  9: [255, 182, 32, 225],
  8: [255, 150, 32, 225],
  7: [255, 120, 30, 225],
  6: [230, 68, 54, 225],
  5: [202, 43, 43, 225],
  4: [176, 35, 35, 225],
  3: [156, 30, 30, 225],
  2: [138, 25, 25, 225],
  1: [120, 18, 18, 225],
  0: [100, 14, 14, 225],
};
const DEFAULT_TUBE_TOPOLOGY_PROGRESS: TubeTopologyProgressEvent = {
  phase: "starting",
  percent: 0,
  completedLines: 0,
  totalLines: 0,
};

const DEFAULT_TOUR_LOOKOUTS = {
  westminster: [-0.1246, 51.5008] as [number, number],
  soho: [-0.1337, 51.5136] as [number, number],
  city: [-0.0928, 51.5149] as [number, number],
  canaryWharf: [-0.0186, 51.5045] as [number, number],
  kingsCross: [-0.1235, 51.5316] as [number, number],
};

function tubeTopologyProgressMessage(progress: TubeTopologyProgressEvent): string {
  switch (progress.phase) {
    case "starting":
      return "Starting Tube topology request...";
    case "lines":
      return `Loaded line catalog (${progress.totalLines.toLocaleString()} lines).`;
    case "routes":
      return progress.totalLines > 0
        ? `Loading route geometry ${progress.completedLines.toLocaleString()}/${progress.totalLines.toLocaleString()}...`
        : "Loading route geometry...";
    case "stations":
      return "Loading Tube station metadata...";
    case "finalizing":
      return "Finalizing topology cache...";
    case "done":
      return "Tube topology loaded.";
    default:
      return "Loading Tube network topology...";
  }
}

function hashString(value: string): number {
  let hash = 0;
  for (let index = 0; index < value.length; index += 1) {
    hash = (hash << 5) - hash + value.charCodeAt(index);
    hash |= 0;
  }
  return Math.abs(hash);
}

function tripTrailColor(trip: ColorTrip): [number, number, number] {
  if (trip.routeSource !== "osrm") {
    return [170, 137, 255];
  }
  const paletteIndex = hashString(trip.tripId) % TRIP_PALETTE.length;
  return TRIP_PALETTE[paletteIndex] ?? TRIP_PALETTE[0];
}

function interpolatePoint(
  a: [number, number],
  b: [number, number],
  t: number,
): [number, number] {
  return [a[0] + (b[0] - a[0]) * t, a[1] + (b[1] - a[1]) * t];
}

function clamp01(value: number): number {
  return Math.max(0, Math.min(1, value));
}

function easeInOutCubic(value: number): number {
  if (value < 0.5) {
    return 4 * value * value * value;
  }
  return 1 - ((-2 * value + 2) ** 3) / 2;
}

function normalizeBearing(value: number): number {
  let result = value;
  while (result <= -180) {
    result += 360;
  }
  while (result > 180) {
    result -= 360;
  }
  return result;
}

function interpolateBearing(from: number, to: number, t: number): number {
  const normalizedFrom = normalizeBearing(from);
  const normalizedTo = normalizeBearing(to);
  let delta = normalizedTo - normalizedFrom;
  if (delta > 180) {
    delta -= 360;
  } else if (delta < -180) {
    delta += 360;
  }
  return normalizeBearing(normalizedFrom + delta * t);
}

function headingDegrees(
  from: [number, number],
  to: [number, number],
): number {
  const fromLon = (from[0] * Math.PI) / 180;
  const toLon = (to[0] * Math.PI) / 180;
  const fromLat = (from[1] * Math.PI) / 180;
  const toLat = (to[1] * Math.PI) / 180;
  const fromMercatorY = Math.log(Math.tan(Math.PI / 4 + fromLat / 2));
  const toMercatorY = Math.log(Math.tan(Math.PI / 4 + toLat / 2));
  const dx = toLon - fromLon;
  const dy = toMercatorY - fromMercatorY;
  if (Math.abs(dx) < 1e-12 && Math.abs(dy) < 1e-12) {
    return 0;
  }
  return (Math.atan2(dy, dx) * 180) / Math.PI;
}

function tripPositionAtTime(
  trip: DecodedTrip,
  currentTimeMs: number,
  leadSegments = 0,
): [number, number] | null {
  if (currentTimeMs < trip.startTimeMs || currentTimeMs > trip.endTimeMs) {
    return null;
  }
  const path = trip.path;
  if (path.length < 2) {
    return path[0] ?? null;
  }
  const durationMs = Math.max(1, trip.endTimeMs - trip.startTimeMs);
  const progress = Math.min(
    1,
    Math.max(0, (currentTimeMs - trip.startTimeMs) / durationMs),
  );
  const positionAlongPath = Math.min(
    path.length - 1,
    progress * (path.length - 1) + leadSegments,
  );
  const leftIndex = Math.max(0, Math.min(path.length - 2, Math.floor(positionAlongPath)));
  const rightIndex = leftIndex + 1;
  const segmentProgress = positionAlongPath - leftIndex;
  const from = path[leftIndex];
  const to = path[rightIndex] ?? from;
  return interpolatePoint(from, to, segmentProgress);
}

function classifyArrowCategory(tripId: string, progress: number): ArrowCategory {
  if (progress <= 0.08) {
    return "unlocked";
  }
  if (progress >= 0.92) {
    return "docked";
  }
  return hashString(tripId) % 2 === 0 ? "ebike" : "classic";
}

function buildPulseColor(
  base: [number, number, number],
  intensity: number,
): [number, number, number, number] {
  const alpha = Math.round(70 + 170 * Math.max(0, Math.min(1, intensity)));
  return [base[0], base[1], base[2], alpha];
}

function isFallbackSource(routeSource: string): boolean {
  return routeSource !== "osrm";
}

function findFirstSymbolLayerId(style: StyleSpecification): string | undefined {
  for (const layer of style.layers ?? []) {
    if (layer.type === "symbol") {
      return layer.id;
    }
  }
  return undefined;
}

function findBuildingVectorLayer(style: StyleSpecification): {
  source: string;
  sourceLayer: string;
} | null {
  for (const layer of style.layers ?? []) {
    if (!("source" in layer) || !("source-layer" in layer)) {
      continue;
    }
    const source = layer.source;
    const sourceLayer = layer["source-layer"];
    if (typeof source !== "string" || typeof sourceLayer !== "string") {
      continue;
    }
    const isBuildingLike =
      layer.id.toLowerCase().includes("building")
      || sourceLayer.toLowerCase().includes("building");
    if (isBuildingLike) {
      return { source, sourceLayer };
    }
  }
  return null;
}

function syncThreeDBuildingsLayer(map: MapLibreMap, enabled: boolean): void {
  const existing = map.getLayer(THREE_D_BUILDINGS_LAYER_ID);
  if (existing) {
    map.setLayoutProperty(
      THREE_D_BUILDINGS_LAYER_ID,
      "visibility",
      enabled ? "visible" : "none",
    );
    return;
  }
  if (!enabled || !map.isStyleLoaded()) {
    return;
  }
  const style = map.getStyle();
  const buildingLayer = findBuildingVectorLayer(style);
  if (!buildingLayer) {
    return;
  }
  map.addLayer(
    {
      id: THREE_D_BUILDINGS_LAYER_ID,
      type: "fill-extrusion",
      source: buildingLayer.source,
      "source-layer": buildingLayer.sourceLayer,
      minzoom: THREE_D_MIN_ZOOM,
      paint: {
        "fill-extrusion-color": "#9db8dd",
        "fill-extrusion-opacity": 0.76,
        "fill-extrusion-height": [
          "coalesce",
          ["get", "height"],
          ["get", "render_height"],
          ["get", "building:height"],
          14,
        ],
        "fill-extrusion-base": [
          "coalesce",
          ["get", "min_height"],
          ["get", "render_min_height"],
          ["get", "building:min_height"],
          0,
        ],
      },
    },
    findFirstSymbolLayerId(style),
  );
}

function shortName(pathOrUrl: string): string {
  try {
    const parsed = new URL(pathOrUrl);
    const parts = parsed.pathname.split("/");
    return parts[parts.length - 1] || pathOrUrl;
  } catch {
    const parts = pathOrUrl.split(/[\\/]/);
    return parts[parts.length - 1] || pathOrUrl;
  }
}

function stationFillColor(station: BikePointStation): [number, number, number, number] {
  const ratio = station.availableBikes / Math.max(1, station.totalDocks);
  const red = Math.round(255 * (1 - ratio));
  const green = Math.round(180 * ratio + 40);
  return [red, green, 80, 210];
}

function tubeStatusColor(severity: number | null): [number, number, number, number] {
  if (severity === null || !Number.isFinite(severity)) {
    return TUBE_STATION_BASE_COLOR;
  }
  return TUBE_STATUS_COLORS[Math.round(severity)] ?? [220, 78, 56, 225];
}

function tubeLiveTrainBodyColor(
  marker: TubeLiveTrainMarker,
  lineMap: Map<string, TubeLineMeta>,
): [number, number, number, number] {
  const lineColor = lineMap.get(marker.lineId)?.color ?? [165, 191, 230];
  return [lineColor[0], lineColor[1], lineColor[2], marker.isInterpolated ? 236 : 206];
}

function tubeLiveTrainNoseColor(marker: TubeLiveTrainMarker): [number, number, number, number] {
  if (!marker.isInterpolated) {
    return [TUBE_LIVE_TRAIN_NOSE_COLOR[0], TUBE_LIVE_TRAIN_NOSE_COLOR[1], TUBE_LIVE_TRAIN_NOSE_COLOR[2], 0];
  }
  return TUBE_LIVE_TRAIN_NOSE_COLOR;
}

function tubeRunPositionAtTime(
  path: [number, number][],
  timestamps: number[],
  currentMs: number,
): [number, number] | null {
  if (path.length < 2 || timestamps.length < 2) {
    return path[0] ?? null;
  }
  const first = timestamps[0] ?? 0;
  const last = timestamps[timestamps.length - 1] ?? first;
  if (currentMs < first || currentMs > last) {
    return null;
  }
  for (let index = 1; index < timestamps.length; index += 1) {
    const prevTime = timestamps[index - 1] ?? first;
    const nextTime = timestamps[index] ?? prevTime;
    if (currentMs > nextTime) {
      continue;
    }
    const from = path[Math.max(0, index - 1)] ?? path[0];
    const to = path[Math.min(path.length - 1, index)] ?? from;
    const span = Math.max(1, nextTime - prevTime);
    const t = Math.max(0, Math.min(1, (currentMs - prevTime) / span));
    return interpolatePoint(from, to, t);
  }
  return path[path.length - 1] ?? null;
}

function dedupeStationIds(stationIds: string[]): string[] {
  const output: string[] = [];
  const seen = new Set<string>();
  for (const stationId of stationIds) {
    if (!stationId || seen.has(stationId)) {
      continue;
    }
    seen.add(stationId);
    output.push(stationId);
  }
  return output;
}

function routeTerminusLabel(
  stationIds: string[],
  stationById: Map<string, TubeStation>,
): string {
  const first = stationIds[0];
  const last = stationIds[stationIds.length - 1];
  if (!first || !last) {
    return "Branch";
  }
  const firstName = stationById.get(first)?.name ?? first;
  const lastName = stationById.get(last)?.name ?? last;
  return `${firstName} to ${lastName}`;
}

function etaLabel(seconds: number): string {
  if (!Number.isFinite(seconds) || seconds <= 30) {
    return "Due";
  }
  return `${Math.max(1, Math.round(seconds / 60))} min`;
}

function expectedTimeLabel(timestampIso: string): string {
  const parsed = Date.parse(timestampIso);
  if (!Number.isFinite(parsed)) {
    return "--:--";
  }
  return new Date(parsed).toLocaleTimeString("en-GB", {
    hour: "2-digit",
    minute: "2-digit",
  });
}

function App() {
  const mapRef = useRef<MapRef | null>(null);
  const [mode, setMode] = useState<AppMode>("playback");
  const {
    status,
    error,
    loadingMessage,
    runtimeMessage,
    initProgress,
    troubleshootingHints,
    jumpError,
    bounds,
    currentTimeMs,
    isPlaying,
    speed,
    decodedTrips,
    setPlaybackTime,
    togglePlay,
    setSpeed,
    jumpToNaturalLanguage,
    diagnostics,
  } = useTripPlayback({ keyboardShortcutsEnabled: mode === "playback" });
  const [theme, setTheme] = useState<ThemeMode>(() => {
    if (typeof window === "undefined") {
      return "dark";
    }
    const stored = window.localStorage.getItem(THEME_STORAGE_KEY);
    if (stored === "dark" || stored === "light") {
      return stored;
    }
    return window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light";
  });
  const [is3DMode, setIs3DMode] = useState(false);
  const [routeFilter, setRouteFilter] = useState<RouteFilterMode>("osrm");
  const [hoveredTrip, setHoveredTrip] = useState<DecodedTrip | null>(null);
  const [selectedTrip, setSelectedTrip] = useState<DecodedTrip | null>(null);
  const [autoRandomFollowEnabled, setAutoRandomFollowEnabled] = useState(false);
  const [viewState, setViewState] = useState<MapViewState>(LONDON_VIEW_STATE);
  const [cinematicTourId, setCinematicTourId] = useState<CinematicTourId>("thames-loop");
  const [cinematicTourPlaying, setCinematicTourPlaying] = useState(false);
  const [liveStations, setLiveStations] = useState<BikePointStation[]>([]);
  const [liveStatus, setLiveStatus] = useState<"idle" | "loading" | "ready" | "error">(
    "idle",
  );
  const [liveError, setLiveError] = useState<string | null>(null);
  const [liveLastRefreshMs, setLiveLastRefreshMs] = useState<number | null>(null);
  const [hoveredStation, setHoveredStation] = useState<BikePointStation | null>(null);
  const [selectedStation, setSelectedStation] = useState<BikePointStation | null>(null);
  const [tubeMode, setTubeMode] = useState<TubeMode>("live");
  const [tubeTopologyStatus, setTubeTopologyStatus] = useState<
    "idle" | "loading" | "ready" | "error"
  >("idle");
  const [tubeTopologyProgress, setTubeTopologyProgress] = useState<TubeTopologyProgressEvent>(
    DEFAULT_TUBE_TOPOLOGY_PROGRESS,
  );
  const [tubeHistoricalStatus, setTubeHistoricalStatus] = useState<
    "idle" | "loading" | "ready" | "error"
  >("idle");
  const [tubeError, setTubeError] = useState<string | null>(null);
  const [tubeLines, setTubeLines] = useState<TubeLineMeta[]>([]);
  const [tubeStations, setTubeStations] = useState<TubeStation[]>([]);
  const [tubeStatuses, setTubeStatuses] = useState<TubeLineStatus[]>([]);
  const [tubeArrivals, setTubeArrivals] = useState<TubeArrival[]>([]);
  const [tubeLiveActions, setTubeLiveActions] = useState<TubeLiveActionsByLine>({});
  const [tubeLiveClockMs, setTubeLiveClockMs] = useState<number>(Date.now());
  const [tubeLastRefreshMs, setTubeLastRefreshMs] = useState<number | null>(null);
  const [tubeLineFilters, setTubeLineFilters] = useState<string[]>([]);
  const [tubeInspectorLineId, setTubeInspectorLineId] = useState<string>("");
  const [tubeInspectorDirection, setTubeInspectorDirection] = useState<TubeDirection>("outbound");
  const [tubeInspectorBranchKey, setTubeInspectorBranchKey] = useState<string>("");
  const [tubeInspectorStationId, setTubeInspectorStationId] = useState<string | null>(null);
  const [tubeHoveredStation, setTubeHoveredStation] = useState<TubeStation | null>(null);
  const [tubeSelectedStation, setTubeSelectedStation] = useState<TubeStation | null>(null);
  const [tubeHoveredTrain, setTubeHoveredTrain] = useState<TubeLiveTrainMarker | null>(null);
  const [tubeSelectedTrain, setTubeSelectedTrain] = useState<TubeLiveTrainMarker | null>(null);
  const [tubeHistoricalDate, setTubeHistoricalDate] = useState(TUBE_HISTORY_DEFAULT_DATE);
  const [tubeTimetablesByLineId, setTubeTimetablesByLineId] = useState<
    Record<string, TubeTimetableResponse[]>
  >({});
  const [tubeHistoricalFrame, setTubeHistoricalFrame] = useState<TubeHistoricalFrame | null>(null);
  const [tubeHistoricalCurrentMs, setTubeHistoricalCurrentMs] = useState<number>(0);
  const [tubeHistoricalPlaying, setTubeHistoricalPlaying] = useState(false);
  const [tubeHistoricalSpeed, setTubeHistoricalSpeed] = useState<number>(
    TUBE_HISTORY_DEFAULT_SPEED,
  );
  const tubeHistoricalCacheRef = useRef<Map<string, TubeHistoricalFrame>>(new Map());
  const tubeHistoricalPlayingRef = useRef(tubeHistoricalPlaying);
  const tubeHistoricalBlendRef = useRef(0);
  const autoAdvancedTripIdRef = useRef<string | null>(null);
  const latestViewStateRef = useRef<MapViewState>(LONDON_VIEW_STATE);
  const activeCinematicKeyframesRef = useRef<CinematicTourKeyframe[] | null>(null);

  const osrmCount = useMemo(
    () => decodedTrips.filter((trip) => trip.routeSource === "osrm").length,
    [decodedTrips],
  );
  const fallbackCount = useMemo(
    () => decodedTrips.filter((trip) => isFallbackSource(trip.routeSource)).length,
    [decodedTrips],
  );
  const visibleTrips = useMemo(() => {
    if (routeFilter === "all") {
      return decodedTrips;
    }
    if (routeFilter === "osrm") {
      return decodedTrips.filter((trip) => trip.routeSource === "osrm");
    }
    return decodedTrips.filter((trip) => isFallbackSource(trip.routeSource));
  }, [decodedTrips, routeFilter]);
  const renderedTrips = useMemo(() => {
    if (!selectedTrip) {
      return visibleTrips;
    }
    if (visibleTrips.some((trip) => trip.tripId === selectedTrip.tripId)) {
      return visibleTrips;
    }
    return [...visibleTrips, selectedTrip];
  }, [selectedTrip, visibleTrips]);
  const activeTrips = useMemo(
    () =>
      visibleTrips.filter(
        (trip) => currentTimeMs >= trip.startTimeMs && currentTimeMs <= trip.endTimeMs,
      ),
    [currentTimeMs, visibleTrips],
  );
  const activeTripSamplePositions = useMemo(() => {
    if (activeTrips.length === 0) {
      return [] as Array<[number, number]>;
    }
    const positions: Array<[number, number]> = [];
    const stride = Math.max(1, Math.floor(activeTrips.length / 4));
    for (let index = 0; index < activeTrips.length && positions.length < 4; index += stride) {
      const trip = activeTrips[index];
      if (!trip) {
        continue;
      }
      const position = tripPositionAtTime(trip, currentTimeMs, ARROW_LEAD_SEGMENTS);
      if (position) {
        positions.push(position);
      }
    }
    return positions;
  }, [activeTrips, currentTimeMs]);
  const selectedTripPosition = useMemo(() => {
    if (!selectedTrip) {
      return null;
    }
    return tripPositionAtTime(selectedTrip, currentTimeMs, FOLLOW_LOOKAHEAD_SEGMENTS);
  }, [currentTimeMs, selectedTrip]);

  const focusedTrip = selectedTrip ?? hoveredTrip ?? null;
  const tripForInspector = focusedTrip;
  const focusedTripId = focusedTrip?.tripId ?? null;
  const stationForInspector = selectedStation ?? hoveredStation;
  const tubeStationForInspector = tubeSelectedStation ?? tubeHoveredStation;
  const tubeTrainForInspector = tubeSelectedTrain ?? tubeHoveredTrain;
  const tubeLinesWithDurations = useMemo(
    () => mergeLineSegmentDurations(tubeLines, tubeTimetablesByLineId),
    [tubeLines, tubeTimetablesByLineId],
  );
  const tubeLinesById = useMemo(
    () => new Map(tubeLinesWithDurations.map((line) => [line.lineId, line])),
    [tubeLinesWithDurations],
  );
  const tubeStationsById = useMemo(
    () => new Map(tubeStations.map((station) => [station.id, station])),
    [tubeStations],
  );
  const tubeLineStatusesById = useMemo(
    () => new Map(tubeStatuses.map((status) => [status.lineId, status])),
    [tubeStatuses],
  );
  const allTubeLineIds = useMemo(
    () => tubeLinesWithDurations.map((line) => line.lineId),
    [tubeLinesWithDurations],
  );
  const visibleTubeLineIds = useMemo(() => {
    if (tubeLineFilters.length === 0) {
      return new Set(allTubeLineIds);
    }
    const validLineIdSet = new Set(allTubeLineIds);
    const selected = tubeLineFilters.filter((lineId) => validLineIdSet.has(lineId));
    if (selected.length === 0) {
      return new Set(allTubeLineIds);
    }
    return new Set(selected);
  }, [allTubeLineIds, tubeLineFilters]);
  const filteredTubeLines = useMemo(
    () => tubeLinesWithDurations.filter((line) => visibleTubeLineIds.has(line.lineId)),
    [tubeLinesWithDurations, visibleTubeLineIds],
  );
  const tubeStationSeverityById = useMemo(() => {
    const out = new Map<string, number>();
    for (const station of tubeStations) {
      const candidateLines = station.lines.filter((lineId) => visibleTubeLineIds.has(lineId));
      if (candidateLines.length === 0) {
        continue;
      }
      const severities = candidateLines
        .map((lineId) => tubeLineStatusesById.get(lineId)?.severity ?? 10);
      out.set(station.id, Math.min(...severities));
    }
    return out;
  }, [tubeLineStatusesById, tubeStations, visibleTubeLineIds]);
  const filteredTubeStations = useMemo(
    () =>
      tubeStations.filter(
        (station) =>
          station.lines.some((lineId) => visibleTubeLineIds.has(lineId)),
      ),
    [tubeStations, visibleTubeLineIds],
  );
  const filteredTubeArrivals = useMemo(
    () => tubeArrivals.filter((arrival) => visibleTubeLineIds.has(arrival.lineId)),
    [tubeArrivals, visibleTubeLineIds],
  );
  const tubeArrivalCountByStation = useMemo(() => {
    const counts = new Map<string, number>();
    for (const arrival of filteredTubeArrivals) {
      counts.set(arrival.naptanId, (counts.get(arrival.naptanId) ?? 0) + 1);
    }
    return counts;
  }, [filteredTubeArrivals]);
  const busiestLiveStation = useMemo(() => {
    if (liveStations.length === 0) {
      return null;
    }
    return liveStations.reduce<BikePointStation | null>((best, station) => {
      if (!best) {
        return station;
      }
      const stationLoad = station.availableBikes + station.availableDocks;
      const bestLoad = best.availableBikes + best.availableDocks;
      return stationLoad > bestLoad ? station : best;
    }, null);
  }, [liveStations]);
  const busiestTubeStation = useMemo(() => {
    let topStationId: string | null = null;
    let topCount = -1;
    for (const [stationId, count] of tubeArrivalCountByStation) {
      if (count <= topCount) {
        continue;
      }
      topStationId = stationId;
      topCount = count;
    }
    if (!topStationId) {
      return null;
    }
    return tubeStationsById.get(topStationId) ?? null;
  }, [tubeArrivalCountByStation, tubeStationsById]);
  const cinematicTourDefinitions = useMemo<Record<CinematicTourId, CinematicTourDefinition>>(() => {
    const frame = (
      longitude: number,
      latitude: number,
      zoom: number,
      pitch: number,
      bearing: number,
      durationMs: number,
    ): CinematicTourKeyframe => ({
      longitude,
      latitude,
      zoom,
      pitch,
      bearing,
      durationMs,
    });
    const rideLead = selectedTripPosition
      ?? activeTripSamplePositions[0]
      ?? DEFAULT_TOUR_LOOKOUTS.soho;
    const rideMid = activeTripSamplePositions[1] ?? DEFAULT_TOUR_LOOKOUTS.city;
    const rideTail = activeTripSamplePositions[2] ?? DEFAULT_TOUR_LOOKOUTS.westminster;
    const stationAnchor = busiestLiveStation
      ? ([busiestLiveStation.lon, busiestLiveStation.lat] as [number, number])
      : DEFAULT_TOUR_LOOKOUTS.soho;
    const tubeAnchor = busiestTubeStation
      ? ([busiestTubeStation.lon, busiestTubeStation.lat] as [number, number])
      : DEFAULT_TOUR_LOOKOUTS.kingsCross;

    return {
      "thames-loop": {
        id: "thames-loop",
        label: "Thames Loop",
        subtitle: "Landmarks, river arc, skyline",
        keyframes: [
          frame(-0.136, 51.508, 12.2, 58, -24, 1_200),
          frame(DEFAULT_TOUR_LOOKOUTS.westminster[0], DEFAULT_TOUR_LOOKOUTS.westminster[1], 13.2, 62, -34, 1_700),
          frame(DEFAULT_TOUR_LOOKOUTS.city[0], DEFAULT_TOUR_LOOKOUTS.city[1], 13.1, 57, -2, 1_850),
          frame(DEFAULT_TOUR_LOOKOUTS.canaryWharf[0], DEFAULT_TOUR_LOOKOUTS.canaryWharf[1], 13.5, 66, 34, 2_050),
          frame(-0.109, 51.507, 12.0, 48, 6, 1_600),
        ],
      },
      "ride-rush": {
        id: "ride-rush",
        label: "Ride Rush",
        subtitle: "Follow active bike flow",
        keyframes: [
          frame(rideLead[0], rideLead[1], 14.0, 63, -28, 1_100),
          frame(rideMid[0], rideMid[1], 13.3, 56, -4, 1_450),
          frame(stationAnchor[0], stationAnchor[1], 13.9, 64, 18, 1_700),
          frame(rideTail[0], rideTail[1], 13.5, 52, -44, 1_450),
          frame(DEFAULT_TOUR_LOOKOUTS.soho[0], DEFAULT_TOUR_LOOKOUTS.soho[1], 12.4, 42, -12, 1_550),
        ],
      },
      "tube-radar": {
        id: "tube-radar",
        label: "Tube Radar",
        subtitle: "Scan busy interchanges",
        keyframes: [
          frame(DEFAULT_TOUR_LOOKOUTS.kingsCross[0], DEFAULT_TOUR_LOOKOUTS.kingsCross[1], 13.7, 60, -12, 1_100),
          frame(tubeAnchor[0], tubeAnchor[1], 14.2, 66, 20, 1_650),
          frame(DEFAULT_TOUR_LOOKOUTS.city[0], DEFAULT_TOUR_LOOKOUTS.city[1], 13.1, 56, -8, 1_600),
          frame(DEFAULT_TOUR_LOOKOUTS.westminster[0], DEFAULT_TOUR_LOOKOUTS.westminster[1], 12.6, 44, -28, 1_650),
          frame(-0.122, 51.511, 11.8, 34, -16, 1_700),
        ],
      },
    };
  }, [
    activeTripSamplePositions,
    busiestLiveStation,
    busiestTubeStation,
    selectedTripPosition,
  ]);
  const cinematicTourOptions = useMemo(
    () =>
      (Object.values(cinematicTourDefinitions) as CinematicTourDefinition[]).map((tour) => ({
        id: tour.id,
        label: tour.label,
      })),
    [cinematicTourDefinitions],
  );
  const selectedCinematicTour = cinematicTourDefinitions[cinematicTourId];
  const tubeInspectorLineOptions = useMemo(
    () => (filteredTubeLines.length > 0 ? filteredTubeLines : tubeLinesWithDurations),
    [filteredTubeLines, tubeLinesWithDurations],
  );
  const tubeSuggestedInspectorLineId = useMemo(() => {
    if (tubeSelectedTrain && visibleTubeLineIds.has(tubeSelectedTrain.lineId)) {
      return tubeSelectedTrain.lineId;
    }
    if (tubeSelectedStation) {
      const candidate = tubeSelectedStation.lines.find((lineId) => visibleTubeLineIds.has(lineId));
      if (candidate) {
        return candidate;
      }
    }
    if (tubeLineFilters.length === 1 && visibleTubeLineIds.has(tubeLineFilters[0])) {
      return tubeLineFilters[0] as string;
    }
    return tubeInspectorLineOptions[0]?.lineId ?? "";
  }, [
    tubeInspectorLineOptions,
    tubeLineFilters,
    tubeSelectedStation,
    tubeSelectedTrain,
    visibleTubeLineIds,
  ]);
  const tubeInspectorLine = useMemo(
    () => (tubeInspectorLineId ? tubeLinesById.get(tubeInspectorLineId) ?? null : null),
    [tubeInspectorLineId, tubeLinesById],
  );
  const tubeInspectorBranches = useMemo<TubeLineBranchOption[]>(() => {
    if (!tubeInspectorLine) {
      return [];
    }
    const sourceSequences = tubeInspectorLine.branchSequences[tubeInspectorDirection];
    const fallbackIds = tubeInspectorLine.orderedStations[tubeInspectorDirection];
    const sequences = sourceSequences.length > 0
      ? sourceSequences
      : [{ stationIds: fallbackIds, path: tubeInspectorLine.stationPaths[tubeInspectorDirection] }];
    const out: TubeLineBranchOption[] = [];
    const seenKeys = new Set<string>();
    for (let index = 0; index < sequences.length; index += 1) {
      const sequence = sequences[index];
      const stationIds = dedupeStationIds(sequence?.stationIds ?? []);
      if (stationIds.length === 0) {
        continue;
      }
      const fallbackKey = `${tubeInspectorLine.lineId}:${tubeInspectorDirection}:${index}`;
      const key = stationIds.join(">") || fallbackKey;
      if (seenKeys.has(key)) {
        continue;
      }
      seenKeys.add(key);
      out.push({
        key,
        stationIds,
        label: routeTerminusLabel(stationIds, tubeStationsById),
      });
    }
    return out;
  }, [tubeInspectorDirection, tubeInspectorLine, tubeStationsById]);
  const tubeInspectorBranch = useMemo(
    () =>
      tubeInspectorBranches.find((branch) => branch.key === tubeInspectorBranchKey)
      ?? tubeInspectorBranches[0]
      ?? null,
    [tubeInspectorBranchKey, tubeInspectorBranches],
  );
  const tubeInspectorStations = useMemo(() => {
    if (!tubeInspectorBranch) {
      return [];
    }
    return tubeInspectorBranch.stationIds.map((stationId, index) => {
      const station = tubeStationsById.get(stationId);
      return {
        stationId,
        index: index + 1,
        name: station?.name ?? stationId,
        zone: station?.zone ?? null,
        hasInterchange: station?.isInterchange ?? false,
        departures: tubeArrivalCountByStation.get(stationId) ?? 0,
      };
    });
  }, [tubeArrivalCountByStation, tubeInspectorBranch, tubeStationsById]);
  const tubeInspectorStation = useMemo(
    () => (tubeInspectorStationId ? tubeStationsById.get(tubeInspectorStationId) ?? null : null),
    [tubeInspectorStationId, tubeStationsById],
  );
  const tubeInspectorDepartures = useMemo<TubeDepartureBoardEntry[]>(() => {
    if (!tubeInspectorStationId) {
      return [];
    }
    const nearestByTrain = new Map<string, TubeArrival>();
    for (const arrival of filteredTubeArrivals) {
      if (arrival.naptanId !== tubeInspectorStationId) {
        continue;
      }
      const vehicle = arrival.vehicleId.trim();
      const key = `${arrival.lineId}:${vehicle.length > 0 ? vehicle : arrival.timestamp}`;
      const existing = nearestByTrain.get(key);
      if (!existing || arrival.timeToStation < existing.timeToStation) {
        nearestByTrain.set(key, arrival);
      }
    }
    return Array.from(nearestByTrain.values())
      .sort((a, b) => a.timeToStation - b.timeToStation)
      .slice(0, 14)
      .map((arrival, index) => {
        const line = tubeLinesById.get(arrival.lineId);
        return {
          id: `${arrival.lineId}:${arrival.vehicleId}:${arrival.expectedArrival}:${index}`,
          lineId: arrival.lineId,
          lineName: line?.lineName ?? arrival.lineId,
          lineColor: line?.color ?? [165, 191, 230],
          destination: arrival.towards ?? "Destination unavailable",
          etaSeconds: Math.max(0, Math.round(arrival.timeToStation)),
          etaLabel: etaLabel(arrival.timeToStation),
          expectedTimeLabel: expectedTimeLabel(arrival.expectedArrival),
        };
      });
  }, [filteredTubeArrivals, tubeInspectorStationId, tubeLinesById]);
  const tubeActionMarkers = useMemo(
    () =>
      buildTubeLiveTrainMarkersFromActions(
        tubeLiveActions,
        filteredTubeLines,
        filteredTubeStations,
        tubeLiveClockMs,
      ),
    [filteredTubeLines, filteredTubeStations, tubeLiveActions, tubeLiveClockMs],
  );
  const tubeArrivalMarkers = useMemo(
    () =>
      buildTubeLiveTrainMarkers(
        filteredTubeArrivals,
        filteredTubeLines,
        filteredTubeStations,
        tubeLiveClockMs,
      ),
    [filteredTubeArrivals, filteredTubeLines, filteredTubeStations, tubeLiveClockMs],
  );
  const tubeLiveTrainMarkers = useMemo(
    () => (tubeActionMarkers.length > 0 ? tubeActionMarkers : tubeArrivalMarkers),
    [tubeActionMarkers, tubeArrivalMarkers],
  );
  const filteredTubeHistoricalRuns = useMemo(
    () =>
      (tubeHistoricalFrame?.runs ?? []).filter((run) => visibleTubeLineIds.has(run.lineId)),
    [tubeHistoricalFrame, visibleTubeLineIds],
  );
  const tubeHistoricalRunsWithColor = useMemo(
    () =>
      filteredTubeHistoricalRuns.map((run) => ({
        ...run,
        color: tubeLinesById.get(run.lineId)?.color ?? [165, 191, 230] as [number, number, number],
      })),
    [filteredTubeHistoricalRuns, tubeLinesById],
  );
  const tubeHistoricalActiveMarkers = useMemo(() => {
    const markers: TubeLiveTrainMarker[] = [];
    for (const run of filteredTubeHistoricalRuns) {
      const position = tubeRunPositionAtTime(
        run.path,
        run.timestamps,
        tubeHistoricalCurrentMs,
      );
      if (!position) {
        continue;
      }
      const nextIndex = run.timestamps.findIndex((value) => value >= tubeHistoricalCurrentMs);
      const safeIndex = Math.max(1, nextIndex);
      const from = run.path[Math.max(0, safeIndex - 1)] ?? position;
      const to = run.path[Math.min(run.path.length - 1, safeIndex)] ?? from;
      markers.push({
        trainKey: run.runId,
        lineId: run.lineId,
        position,
        headingDeg: headingDegrees(from, to),
        nextStopId: "",
        progress01: 0,
        isInterpolated: true,
        destinationName: run.direction,
        timeToStation: 0,
      });
    }
    return markers;
  }, [filteredTubeHistoricalRuns, tubeHistoricalCurrentMs]);
  const tubeLiveSummary = useMemo(() => {
    const disruptionLines = tubeStatuses.filter((status) => status.severity < 10).length;
    return {
      lines: filteredTubeLines.length,
      stations: filteredTubeStations.length,
      trains: tubeLiveTrainMarkers.length,
      disruptions: disruptionLines,
    };
  }, [filteredTubeLines.length, filteredTubeStations.length, tubeLiveTrainMarkers.length, tubeStatuses]);
  const tubeHistoricalSummary = useMemo(() => {
    const active = tubeHistoricalActiveMarkers.length;
    return {
      runs: filteredTubeHistoricalRuns.length,
      active,
      firstDeparture:
        filteredTubeHistoricalRuns[0]?.departureTimeMs ?? tubeHistoricalFrame?.minMs ?? 0,
      lastArrival:
        filteredTubeHistoricalRuns[filteredTubeHistoricalRuns.length - 1]?.timestamps.at(-1)
        ?? tubeHistoricalFrame?.maxMs
        ?? 0,
    };
  }, [filteredTubeHistoricalRuns, tubeHistoricalActiveMarkers.length, tubeHistoricalFrame]);
  const initPercent =
    initProgress && initProgress.totalFiles > 0
      ? Math.min(100, Math.round((initProgress.checkedFiles / initProgress.totalFiles) * 100))
      : 0;
  const tubeTopologyPercent = Math.max(
    2,
    Math.min(100, Math.round(tubeTopologyProgress.percent * 100)),
  );
  const tubeTopologyProgressLabel = tubeTopologyProgressMessage(tubeTopologyProgress);
  const isAllTubeLineFilterActive = tubeLineFilters.length === 0;

  const formatDistance = (meters: number): string => `${(meters / 1000).toFixed(2)} km`;
  const formatDuration = (seconds: number): string => `${Math.round(seconds / 60)} min`;
  const hudTimestampMs =
    mode === "playback"
      ? currentTimeMs
      : mode === "live"
        ? liveLastRefreshMs ?? currentTimeMs
        : tubeMode === "historical"
          ? tubeHistoricalCurrentMs || Date.now()
          : tubeLastRefreshMs ?? Date.now();
  const hudDateLabel = new Date(hudTimestampMs).toLocaleDateString("en-GB", {
    weekday: "short",
    month: "short",
    day: "numeric",
    year: "numeric",
  });
  const hudTimeLabel = new Date(hudTimestampMs).toLocaleTimeString("en-GB", {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
  const mapStyleUrl = theme === "dark" ? DARK_MAP_STYLE_URL : LIGHT_MAP_STYLE_URL;
  const updateMapThreeDLayer = useCallback(
    (enabled: boolean) => {
      const map = mapRef.current?.getMap();
      if (!map) {
        return;
      }
      syncThreeDBuildingsLayer(map, enabled);
    },
    [],
  );

  const clearTripFocus = useCallback(() => {
    setSelectedTrip(null);
    setHoveredTrip(null);
    setAutoRandomFollowEnabled(false);
    autoAdvancedTripIdRef.current = null;
  }, []);

  const toggleCinematicTourPlayback = useCallback(() => {
    setCinematicTourPlaying((playing) => {
      const next = !playing;
      if (!next) {
        activeCinematicKeyframesRef.current = null;
        return next;
      }
      const frozenKeyframes = (selectedCinematicTour?.keyframes ?? []).map((keyframe) => ({
        ...keyframe,
      }));
      if (frozenKeyframes.length === 0) {
        activeCinematicKeyframesRef.current = null;
        return false;
      }
      activeCinematicKeyframesRef.current = frozenKeyframes;
      if (!isPlaying) {
        togglePlay();
      }
      return next;
    });
  }, [isPlaying, selectedCinematicTour, togglePlay]);

  useEffect(() => {
    latestViewStateRef.current = viewState;
  }, [viewState]);

  useEffect(() => {
    if (typeof window !== "undefined") {
      window.localStorage.setItem(THEME_STORAGE_KEY, theme);
    }
  }, [theme]);

  useEffect(() => {
    if (mode === "playback") {
      return;
    }
    if (!cinematicTourPlaying) {
      return;
    }
    activeCinematicKeyframesRef.current = null;
    setCinematicTourPlaying(false);
  }, [cinematicTourPlaying, mode]);

  useEffect(() => {
    if (!cinematicTourPlaying || mode !== "playback") {
      return;
    }
    const keyframes = activeCinematicKeyframesRef.current ?? [];
    if (keyframes.length === 0) {
      activeCinematicKeyframesRef.current = null;
      setCinematicTourPlaying(false);
      return;
    }

    let segmentIndex = 0;
    let segmentStartTs = performance.now();
    let frameId = 0;
    const startPose: CameraPose = {
      longitude: latestViewStateRef.current.longitude,
      latitude: latestViewStateRef.current.latitude,
      zoom: latestViewStateRef.current.zoom,
      pitch: latestViewStateRef.current.pitch ?? 0,
      bearing: latestViewStateRef.current.bearing ?? 0,
    };

    const step = (timestamp: number): void => {
      const target = keyframes[segmentIndex];
      if (!target) {
        activeCinematicKeyframesRef.current = null;
        setCinematicTourPlaying(false);
        return;
      }
      const previousTarget = keyframes[segmentIndex - 1];
      const from = previousTarget ?? startPose;
      const durationMs = Math.max(CINEMATIC_MIN_SEGMENT_MS, target.durationMs);
      const t = clamp01((timestamp - segmentStartTs) / durationMs);
      const eased = easeInOutCubic(t);

      setViewState((current) => ({
        ...current,
        longitude: from.longitude + (target.longitude - from.longitude) * eased,
        latitude: from.latitude + (target.latitude - from.latitude) * eased,
        zoom: from.zoom + (target.zoom - from.zoom) * eased,
        pitch: from.pitch + (target.pitch - from.pitch) * eased,
        bearing: interpolateBearing(from.bearing, target.bearing, eased),
      }));

      if (t >= 0.999) {
        segmentIndex += 1;
        segmentStartTs = timestamp;
      }
      if (segmentIndex >= keyframes.length) {
        activeCinematicKeyframesRef.current = null;
        setCinematicTourPlaying(false);
        return;
      }
      frameId = window.requestAnimationFrame(step);
    };

    frameId = window.requestAnimationFrame(step);
    return () => {
      window.cancelAnimationFrame(frameId);
    };
  }, [cinematicTourPlaying, mode]);

  useEffect(() => {
    setViewState((current) => ({
      ...current,
      pitch: is3DMode ? THREE_D_TARGET_PITCH : 0,
      bearing: is3DMode ? THREE_D_TARGET_BEARING : 0,
      zoom: is3DMode ? Math.max(current.zoom, THREE_D_MIN_ZOOM_TARGET) : current.zoom,
    }));
    updateMapThreeDLayer(is3DMode);
  }, [is3DMode, updateMapThreeDLayer]);

  useEffect(() => {
    clearTripFocus();
    setHoveredStation(null);
    setSelectedStation(null);
    setTubeHoveredStation(null);
    setTubeSelectedStation(null);
    setTubeHoveredTrain(null);
    setTubeSelectedTrain(null);
    if (mode !== "tube") {
      setTubeHistoricalPlaying(false);
    }
  }, [clearTripFocus, mode]);

  useEffect(() => {
    if (tubeInspectorLineOptions.length === 0) {
      if (tubeInspectorLineId !== "") {
        setTubeInspectorLineId("");
      }
      return;
    }
    const availableLineIds = new Set(tubeInspectorLineOptions.map((line) => line.lineId));
    if (tubeInspectorLineId && availableLineIds.has(tubeInspectorLineId)) {
      return;
    }
    if (tubeSuggestedInspectorLineId && availableLineIds.has(tubeSuggestedInspectorLineId)) {
      setTubeInspectorLineId(tubeSuggestedInspectorLineId);
      return;
    }
    setTubeInspectorLineId(tubeInspectorLineOptions[0]?.lineId ?? "");
  }, [tubeInspectorLineId, tubeInspectorLineOptions, tubeSuggestedInspectorLineId]);

  useEffect(() => {
    if (!tubeSelectedStation || !tubeInspectorLineId) {
      return;
    }
    if (tubeSelectedStation.lines.includes(tubeInspectorLineId)) {
      return;
    }
    const nextLine = tubeSelectedStation.lines.find((lineId) =>
      tubeInspectorLineOptions.some((line) => line.lineId === lineId),
    );
    if (nextLine) {
      setTubeInspectorLineId(nextLine);
    }
  }, [tubeInspectorLineId, tubeInspectorLineOptions, tubeSelectedStation]);

  useEffect(() => {
    if (tubeInspectorBranches.length === 0) {
      if (tubeInspectorBranchKey !== "") {
        setTubeInspectorBranchKey("");
      }
      return;
    }
    if (tubeInspectorBranches.some((branch) => branch.key === tubeInspectorBranchKey)) {
      return;
    }
    const preferredBranch = tubeSelectedStation
      ? tubeInspectorBranches.find((branch) => branch.stationIds.includes(tubeSelectedStation.id))
      : null;
    setTubeInspectorBranchKey((preferredBranch ?? tubeInspectorBranches[0]).key);
  }, [tubeInspectorBranchKey, tubeInspectorBranches, tubeSelectedStation]);

  useEffect(() => {
    const stationIds = tubeInspectorBranch?.stationIds ?? [];
    if (stationIds.length === 0) {
      if (tubeInspectorStationId !== null) {
        setTubeInspectorStationId(null);
      }
      return;
    }
    if (tubeSelectedStation && stationIds.includes(tubeSelectedStation.id)) {
      if (tubeInspectorStationId !== tubeSelectedStation.id) {
        setTubeInspectorStationId(tubeSelectedStation.id);
      }
      return;
    }
    if (tubeInspectorStationId && stationIds.includes(tubeInspectorStationId)) {
      return;
    }
    setTubeInspectorStationId(stationIds[0] ?? null);
  }, [tubeInspectorBranch, tubeInspectorStationId, tubeSelectedStation]);

  const jumpToRandomTime = useCallback(() => {
    if (!bounds) {
      return;
    }
    const span = Math.max(1, bounds.maxMs - bounds.minMs);
    const target = bounds.minMs + Math.floor(Math.random() * span);
    setPlaybackTime(target);
  }, [bounds, setPlaybackTime]);

  const pickRandomTrip = useCallback(
    (excludeTripId?: string): DecodedTrip | null => {
      const activeCandidates = activeTrips.filter((trip) => trip.tripId !== excludeTripId);
      if (activeCandidates.length > 0) {
        return activeCandidates[Math.floor(Math.random() * activeCandidates.length)] ?? null;
      }

      const futureCandidates = visibleTrips
        .filter((trip) => trip.tripId !== excludeTripId && trip.startTimeMs > currentTimeMs)
        .sort((a, b) => a.startTimeMs - b.startTimeMs);
      if (futureCandidates.length > 0) {
        const nearFuture = futureCandidates.slice(0, Math.min(80, futureCandidates.length));
        return nearFuture[Math.floor(Math.random() * nearFuture.length)] ?? null;
      }

      const fallbackCandidates = visibleTrips.filter((trip) => trip.tripId !== excludeTripId);
      if (fallbackCandidates.length === 0) {
        return null;
      }
      return fallbackCandidates[Math.floor(Math.random() * fallbackCandidates.length)] ?? null;
    },
    [activeTrips, currentTimeMs, visibleTrips],
  );

  const startRandomRideFollow = useCallback(
    (excludeTripId?: string): boolean => {
      const trip = pickRandomTrip(excludeTripId);
      if (!trip) {
        return false;
      }

      setCinematicTourPlaying(false);
      setSelectedTrip(trip);
      setHoveredTrip(null);
      setAutoRandomFollowEnabled(true);
      autoAdvancedTripIdRef.current = null;

      if (!isPlaying) {
        togglePlay();
      }

      if (bounds && (currentTimeMs < trip.startTimeMs || currentTimeMs > trip.endTimeMs)) {
        const target = Math.min(bounds.maxMs, Math.max(bounds.minMs, trip.startTimeMs));
        setPlaybackTime(target);
      }
      return true;
    },
    [bounds, currentTimeMs, isPlaying, pickRandomTrip, setPlaybackTime, togglePlay],
  );

  useEffect(() => {
    if (mode !== "live") {
      return;
    }
    let cancelled = false;
    const refresh = async (): Promise<void> => {
      if (!cancelled) {
        setLiveStatus((prev) => (prev === "ready" ? "ready" : "loading"));
      }
      try {
        const stations = await fetchBikePointStations();
        if (cancelled) {
          return;
        }
        setLiveStations(stations);
        setLiveLastRefreshMs(Date.now());
        setLiveError(null);
        setLiveStatus("ready");
      } catch (refreshError) {
        if (cancelled) {
          return;
        }
        setLiveError(
          refreshError instanceof Error
            ? refreshError.message
            : "Failed to refresh BikePoint station data.",
        );
        setLiveStatus("error");
      }
    };

    void refresh();
    const timer = window.setInterval(() => {
      void refresh();
    }, LIVE_STATION_REFRESH_MS);
    return () => {
      cancelled = true;
      window.clearInterval(timer);
    };
  }, [mode]);

  const liveSummary = useMemo(() => {
    const stations = liveStations.length;
    const bikes = liveStations.reduce((sum, station) => sum + station.availableBikes, 0);
    const ebikes = liveStations.reduce((sum, station) => sum + station.availableEBikes, 0);
    const docks = liveStations.reduce((sum, station) => sum + station.availableDocks, 0);
    return { stations, bikes, ebikes, docks };
  }, [liveStations]);

  useEffect(() => {
    if (mode !== "tube") {
      return;
    }
    if (tubeTopologyStatus !== "idle") {
      return;
    }
    let timedOut = false;
    let watchdogId = 0;
    const loadTopology = async (): Promise<void> => {
      setTubeTopologyStatus("loading");
      setTubeTopologyProgress({ ...DEFAULT_TUBE_TOPOLOGY_PROGRESS });
      setTubeError(null);
      watchdogId = window.setTimeout(() => {
        timedOut = true;
        setTubeError(
          "Tube topology is taking too long. Check network/TfL API access and try again.",
        );
        setTubeTopologyStatus("error");
      }, 45_000);
      try {
        const topology = await fetchTubeTopology((progress) => {
          setTubeTopologyProgress(progress);
        });
        if (timedOut) {
          return;
        }
        tubeHistoricalCacheRef.current.clear();
        setTubeTimetablesByLineId({});
        setTubeHistoricalFrame(null);
        setTubeLines(topology.lines);
        setTubeStations(topology.stations);
        setTubeTopologyProgress({
          phase: "done",
          percent: 1,
          completedLines: topology.lines.length,
          totalLines: topology.lines.length,
        });
        setTubeTopologyStatus("ready");
      } catch (topologyError) {
        if (timedOut) {
          return;
        }
        setTubeError(
          topologyError instanceof Error
            ? topologyError.message
            : "Failed to load Tube topology.",
        );
        setTubeTopologyStatus("error");
      } finally {
        if (watchdogId) {
          window.clearTimeout(watchdogId);
        }
      }
    };
    void loadTopology();
  }, [mode, tubeTopologyStatus]);

  useEffect(() => {
    if (mode !== "tube" || tubeTopologyStatus !== "ready") {
      return;
    }
    let cancelled = false;
    const refreshTubeLive = async (): Promise<void> => {
      try {
        const [statuses, liveActions, arrivals] = await Promise.all([
          fetchTubeLineStatuses(),
          fetchTubeLiveActions().catch(() => ({})),
          fetchTubeArrivals().catch(() => []),
        ]);
        if (cancelled) {
          return;
        }
        setTubeStatuses(statuses);
        setTubeLiveActions(liveActions);
        setTubeArrivals(arrivals);
        setTubeLastRefreshMs(Date.now());
        setTubeLiveClockMs(Date.now());
        setTubeError(null);
      } catch (refreshError) {
        if (cancelled) {
          return;
        }
        setTubeError(
          refreshError instanceof Error
            ? refreshError.message
            : "Failed to refresh Tube live data.",
        );
      }
    };

    void refreshTubeLive();
    const timer = window.setInterval(() => {
      void refreshTubeLive();
    }, TUBE_REFRESH_MS);

    return () => {
      cancelled = true;
      window.clearInterval(timer);
    };
  }, [mode, tubeTopologyStatus]);

  useEffect(() => {
    if (mode !== "tube" || tubeMode !== "live") {
      return;
    }
    setTubeLiveClockMs(Date.now());
    let animationFrameId = 0;
    let lastCommitTimestamp = 0;

    const tick = (timestamp: number): void => {
      if (
        lastCommitTimestamp === 0
        || timestamp - lastCommitTimestamp >= TUBE_LIVE_CLOCK_FRAME_MS
      ) {
        lastCommitTimestamp = timestamp;
        setTubeLiveClockMs(Date.now());
      }
      animationFrameId = window.requestAnimationFrame(tick);
    };

    animationFrameId = window.requestAnimationFrame(tick);
    return () => window.cancelAnimationFrame(animationFrameId);
  }, [mode, tubeMode]);

  useEffect(() => {
    tubeHistoricalPlayingRef.current = tubeHistoricalPlaying;
  }, [tubeHistoricalPlaying]);

  useEffect(() => {
    if (mode !== "tube" || tubeMode !== "historical" || tubeTopologyStatus !== "ready") {
      return;
    }
    const cachedFrame = tubeHistoricalCacheRef.current.get(tubeHistoricalDate);
    if (cachedFrame) {
      setTubeHistoricalFrame(cachedFrame);
      setTubeHistoricalCurrentMs(cachedFrame.minMs);
      setTubeHistoricalStatus("ready");
      return;
    }

    let cancelled = false;
    const loadTubeHistorical = async (): Promise<void> => {
      setTubeHistoricalStatus("loading");
      setTubeError(null);
      try {
        const timetablesByLineId: Record<string, TubeTimetableResponse[]> = {};
        for (const line of tubeLines) {
          const inboundBranchStops = line.branchSequences.inbound.map(
            (sequence) => sequence.stationIds,
          );
          const outboundBranchStops = line.branchSequences.outbound.map(
            (sequence) => sequence.stationIds,
          );
          const fallbackInbound = line.orderedStations.inbound;
          const fallbackOutbound = line.orderedStations.outbound;
          const origins = Array.from(
            new Set([
              ...inboundBranchStops.flatMap((stations) => [stations[0], stations[stations.length - 1]]),
              ...outboundBranchStops.flatMap((stations) => [stations[0], stations[stations.length - 1]]),
              fallbackInbound[0],
              fallbackInbound[fallbackInbound.length - 1],
              fallbackOutbound[0],
              fallbackOutbound[fallbackOutbound.length - 1],
            ].filter((value): value is string => typeof value === "string" && value.length > 0)),
          );
          const entries: TubeTimetableResponse[] = [];
          for (const originId of origins) {
            try {
              const timetable = await fetchTubeTimetable(line.lineId, originId);
              if (timetable) {
                entries.push(timetable);
              }
            } catch {
              // Skip unavailable line timetables and continue simulation from available data.
            }
          }
          timetablesByLineId[line.lineId] = entries;
        }

        if (cancelled) {
          return;
        }
        setTubeTimetablesByLineId(timetablesByLineId);
        const mergedLines = mergeLineSegmentDurations(tubeLines, timetablesByLineId);
        const frame = buildTubeHistoricalFrame(
          tubeHistoricalDate,
          mergedLines,
          timetablesByLineId,
        );
        tubeHistoricalCacheRef.current.set(tubeHistoricalDate, frame);
        setTubeHistoricalFrame(frame);
        setTubeHistoricalCurrentMs(frame.minMs);
        setTubeHistoricalStatus("ready");
      } catch (historicalError) {
        if (cancelled) {
          return;
        }
        setTubeError(
          historicalError instanceof Error
            ? historicalError.message
            : "Failed to build Tube historical simulation.",
        );
        setTubeHistoricalStatus("error");
      }
    };

    void loadTubeHistorical();
    return () => {
      cancelled = true;
    };
  }, [mode, tubeMode, tubeHistoricalDate, tubeLines, tubeTopologyStatus]);

  useEffect(() => {
    const isInteractiveTarget = (target: EventTarget | null): boolean => {
      if (!(target instanceof HTMLElement)) {
        return false;
      }
      if (target.isContentEditable) {
        return true;
      }
      return Boolean(target.closest("input, textarea, select, button, [role='button']"));
    };

    const onKeyDown = (event: KeyboardEvent): void => {
      if (mode !== "playback") {
        return;
      }
      if (isInteractiveTarget(event.target)) {
        return;
      }
      if (event.code !== "KeyC" || event.repeat) {
        return;
      }
      event.preventDefault();
      event.stopPropagation();
      toggleCinematicTourPlayback();
    };

    window.addEventListener("keydown", onKeyDown, true);
    return () => window.removeEventListener("keydown", onKeyDown, true);
  }, [mode, toggleCinematicTourPlayback]);

  useEffect(() => {
    const isInteractiveTarget = (target: EventTarget | null): boolean => {
      if (!(target instanceof HTMLElement)) {
        return false;
      }
      if (target.isContentEditable) {
        return true;
      }
      return Boolean(target.closest("input, textarea, select, button, [role='button']"));
    };

    const onKeyDown = (event: KeyboardEvent): void => {
      if (mode !== "tube" || tubeMode !== "historical" || !tubeHistoricalFrame) {
        return;
      }
      if (isInteractiveTarget(event.target)) {
        return;
      }
      if (event.code !== "Space" || event.repeat) {
        return;
      }
      event.preventDefault();
      event.stopPropagation();
      setTubeHistoricalPlaying((playing) => !playing);
    };

    window.addEventListener("keydown", onKeyDown, true);
    return () => window.removeEventListener("keydown", onKeyDown, true);
  }, [mode, tubeHistoricalFrame, tubeMode]);

  useEffect(() => {
    if (mode !== "tube" || tubeMode !== "historical" || !tubeHistoricalFrame) {
      tubeHistoricalBlendRef.current = 0;
      return;
    }
    if (!tubeHistoricalPlaying && tubeHistoricalBlendRef.current <= 0.0001) {
      return;
    }

    let animationFrameId = 0;
    let previousTimestamp = performance.now();

    const step = (timestamp: number): void => {
      const deltaMs = Math.max(0, timestamp - previousTimestamp);
      previousTimestamp = timestamp;

      const targetBlend = tubeHistoricalPlayingRef.current ? 1 : 0;
      const currentBlend = tubeHistoricalBlendRef.current;
      const easeDurationMs =
        targetBlend > currentBlend ? TUBE_HISTORY_EASE_IN_MS : TUBE_HISTORY_EASE_OUT_MS;
      const blendDelta = easeDurationMs <= 0 ? 1 : deltaMs / easeDurationMs;
      const nextBlend =
        targetBlend > currentBlend
          ? Math.min(targetBlend, currentBlend + blendDelta)
          : Math.max(targetBlend, currentBlend - blendDelta);
      tubeHistoricalBlendRef.current = nextBlend;

      if (nextBlend > 0) {
        setTubeHistoricalCurrentMs((previous) => {
          const next = previous + deltaMs * tubeHistoricalSpeed * nextBlend;
          if (next >= tubeHistoricalFrame.maxMs) {
            tubeHistoricalBlendRef.current = 0;
            setTubeHistoricalPlaying(false);
            return tubeHistoricalFrame.maxMs;
          }
          return next;
        });
      }

      if (tubeHistoricalPlayingRef.current || tubeHistoricalBlendRef.current > 0.0001) {
        animationFrameId = window.requestAnimationFrame(step);
      }
    };

    animationFrameId = window.requestAnimationFrame(step);
    return () => window.cancelAnimationFrame(animationFrameId);
  }, [mode, tubeHistoricalFrame, tubeHistoricalPlaying, tubeHistoricalSpeed, tubeMode]);

  useEffect(() => {
    if (mode === "tube" && tubeMode === "historical") {
      return;
    }
    tubeHistoricalBlendRef.current = 0;
  }, [mode, tubeMode]);

  const followedTripPosition = useMemo(() => {
    if (
      mode !== "playback"
      || cinematicTourPlaying
      || !autoRandomFollowEnabled
      || !selectedTrip
    ) {
      return null;
    }
    return tripPositionAtTime(selectedTrip, currentTimeMs, FOLLOW_LOOKAHEAD_SEGMENTS);
  }, [autoRandomFollowEnabled, cinematicTourPlaying, currentTimeMs, mode, selectedTrip]);

  useEffect(() => {
    if (
      mode !== "playback"
      || cinematicTourPlaying
      || !autoRandomFollowEnabled
      || !followedTripPosition
    ) {
      return;
    }
    const [targetLon, targetLat] = followedTripPosition;
    setViewState((previous) => {
      const nextLon = previous.longitude + (targetLon - previous.longitude) * FOLLOW_BLEND_FACTOR;
      const nextLat = previous.latitude + (targetLat - previous.latitude) * FOLLOW_BLEND_FACTOR;
      if (
        Math.abs(nextLon - previous.longitude) < 1e-8 &&
        Math.abs(nextLat - previous.latitude) < 1e-8
      ) {
        return previous;
      }
      return {
        ...previous,
        longitude: nextLon,
        latitude: nextLat,
      };
    });
  }, [autoRandomFollowEnabled, cinematicTourPlaying, followedTripPosition, mode]);

  useEffect(() => {
    if (mode !== "playback" || !autoRandomFollowEnabled || !selectedTrip) {
      return;
    }
    if (currentTimeMs <= selectedTrip.endTimeMs) {
      autoAdvancedTripIdRef.current = null;
      return;
    }
    if (autoAdvancedTripIdRef.current === selectedTrip.tripId) {
      return;
    }
    const changed = startRandomRideFollow(selectedTrip.tripId);
    autoAdvancedTripIdRef.current = changed ? selectedTrip.tripId : null;
  }, [autoRandomFollowEnabled, currentTimeMs, mode, selectedTrip, startRandomRideFollow]);

  const arrowMarkers = useMemo<ArrowMarker[]>(() => {
    if (mode !== "playback") {
      return [];
    }

    const markers: ArrowMarker[] = [];
    for (const trip of renderedTrips) {
      if (currentTimeMs < trip.startTimeMs || currentTimeMs > trip.endTimeMs) {
        continue;
      }
      const path = trip.path;
      if (path.length < 2) {
        continue;
      }

      const durationMs = Math.max(1, trip.endTimeMs - trip.startTimeMs);
      const progress = Math.min(
        1,
        Math.max(0, (currentTimeMs - trip.startTimeMs) / durationMs),
      );
      const positionAlongPath = Math.min(
        path.length - 1,
        progress * (path.length - 1) + ARROW_LEAD_SEGMENTS,
      );
      const leftIndex = Math.max(0, Math.min(path.length - 2, Math.floor(positionAlongPath)));
      const rightIndex = leftIndex + 1;
      const segmentProgress = positionAlongPath - leftIndex;
      const from = path[leftIndex];
      const to = path[rightIndex] ?? from;
      const category = classifyArrowCategory(trip.tripId, progress);

      markers.push({
        id: `${trip.tripId}:${leftIndex}`,
        tripId: trip.tripId,
        position: interpolatePoint(from, to, segmentProgress),
        angleDeg: headingDegrees(from, to),
        color: ARROW_COLORS[category],
        category,
      });
    }

    return markers;
  }, [currentTimeMs, mode, renderedTrips]);

  const spawnPulseMarkers = useMemo<PulseMarker[]>(() => {
    if (mode !== "playback") {
      return [];
    }
    const markers: PulseMarker[] = [];
    for (const trip of renderedTrips) {
      const delta = currentTimeMs - trip.startTimeMs;
      if (delta < 0 || delta > EVENT_PULSE_WINDOW_MS) {
        continue;
      }
      if (trip.path.length === 0) {
        continue;
      }
      const intensity = 1 - delta / EVENT_PULSE_WINDOW_MS;
      const startPoint = trip.path[0];
      if (!startPoint) {
        continue;
      }
      markers.push({
        id: `spawn:${trip.tripId}:${trip.startTimeMs}`,
        position: startPoint,
        color: buildPulseColor([86, 224, 168], intensity),
        radius: 10 + intensity * 22,
      });
    }
    return markers;
  }, [currentTimeMs, mode, renderedTrips]);

  const finishPulseMarkers = useMemo<PulseMarker[]>(() => {
    if (mode !== "playback") {
      return [];
    }
    const markers: PulseMarker[] = [];
    for (const trip of renderedTrips) {
      const delta = currentTimeMs - trip.endTimeMs;
      if (delta < 0 || delta > EVENT_PULSE_WINDOW_MS) {
        continue;
      }
      if (trip.path.length === 0) {
        continue;
      }
      const intensity = 1 - delta / EVENT_PULSE_WINDOW_MS;
      const endPoint = trip.path[trip.path.length - 1];
      if (!endPoint) {
        continue;
      }
      markers.push({
        id: `finish:${trip.tripId}:${trip.endTimeMs}`,
        position: endPoint,
        color: buildPulseColor([255, 126, 173], intensity),
        radius: 10 + intensity * 22,
      });
    }
    return markers;
  }, [currentTimeMs, mode, renderedTrips]);

  const layers = useMemo(() => {
    const timelineTimeS = bounds ? (currentTimeMs - bounds.minMs) / 1000 : 0;
    const output: Layer[] = [
      new TripsLayer<DecodedTrip>({
        id: "santander-trips",
        data: renderedTrips,
        getPath: (trip) => trip.path,
        getTimestamps: (trip) => trip.timestamps,
        getColor: tripTrailColor,
        opacity: TRIP_TRAIL_OPACITY,
        widthMinPixels: TRIP_TRAIL_MIN_WIDTH_PX,
        trailLength: TRIP_TRAIL_LENGTH_SECONDS,
        currentTime: timelineTimeS,
        fadeTrail: true,
        capRounded: true,
        jointRounded: true,
        parameters: {
          [GL.DEPTH_TEST]: false,
        } as any,
        getPolygonOffset: () => [0, -120],
        pickable: true,
        onHover: ({ object }: PickingInfo<DecodedTrip>) => {
          setHoveredTrip(object ?? null);
        },
        onClick: ({ object }: PickingInfo<DecodedTrip>) => {
          setSelectedTrip(object ?? null);
          setAutoRandomFollowEnabled(false);
          autoAdvancedTripIdRef.current = null;
        },
      }),
      new ScatterplotLayer<PulseMarker>({
        id: "trip-spawn-pulses",
        data: spawnPulseMarkers,
        pickable: false,
        filled: true,
        stroked: true,
        radiusUnits: "meters",
        lineWidthUnits: "pixels",
        getPosition: (marker) => marker.position,
        getRadius: (marker) => marker.radius,
        getFillColor: (marker) => marker.color,
        getLineColor: [136, 255, 205, 220],
        lineWidthMinPixels: 1.2,
      }),
      new ScatterplotLayer<PulseMarker>({
        id: "trip-finish-pulses",
        data: finishPulseMarkers,
        pickable: false,
        filled: true,
        stroked: true,
        radiusUnits: "meters",
        lineWidthUnits: "pixels",
        getPosition: (marker) => marker.position,
        getRadius: (marker) => marker.radius,
        getFillColor: (marker) => marker.color,
        getLineColor: [255, 172, 205, 220],
        lineWidthMinPixels: 1.2,
      }),
      new IconLayer<ArrowMarker>({
        id: "trip-arrows",
        data: arrowMarkers,
        pickable: false,
        iconAtlas: ARROW_ICON_ATLAS,
        iconMapping: ARROW_ICON_MAPPING,
        getIcon: () => "arrow",
        getPosition: (marker) => marker.position,
        getColor: (marker) => {
          if (!focusedTripId || marker.tripId === focusedTripId) {
            return marker.color;
          }
          return [marker.color[0], marker.color[1], marker.color[2], 95];
        },
        getAngle: (marker) => marker.angleDeg,
        getSize: () => 12,
        sizeScale: 1,
        sizeUnits: "pixels",
        sizeMinPixels: 9,
        sizeMaxPixels: 18,
        billboard: true,
      }),
    ];

    if (focusedTrip) {
      const focusColor = tripTrailColor(focusedTrip);
      output.push(
        new SolidPolygonLayer({
          id: "trip-focus-dim-overlay",
          data: DIM_OVERLAY_POLYGON,
          getPolygon: (polygon) => polygon,
          getFillColor: theme === "dark" ? FOCUS_DIM_OVERLAY_DARK : FOCUS_DIM_OVERLAY_LIGHT,
          pickable: false,
        }),
      );
      output.push(
        new PathLayer<DecodedTrip>({
          id: "trip-focus-route",
          data: [focusedTrip],
          getPath: (trip) => trip.path,
          getColor: [focusColor[0], focusColor[1], focusColor[2], 245],
          getWidth: 4,
          widthUnits: "pixels",
          widthMinPixels: 2,
          capRounded: true,
          jointRounded: true,
          parameters: {
            [GL.DEPTH_TEST]: false,
          } as any,
          opacity: 0.9,
          pickable: false,
        }),
      );
      output.push(
        new IconLayer<ArrowMarker>({
          id: "trip-focus-arrow",
          data: arrowMarkers.filter((marker) => marker.tripId === focusedTrip.tripId),
          pickable: false,
          iconAtlas: ARROW_ICON_ATLAS,
          iconMapping: ARROW_ICON_MAPPING,
          getIcon: () => "arrow",
          getPosition: (marker) => marker.position,
          getColor: (marker) => [marker.color[0], marker.color[1], marker.color[2], 255],
          getAngle: (marker) => marker.angleDeg,
          getSize: () => 14,
          sizeScale: 1,
          sizeUnits: "pixels",
          sizeMinPixels: 11,
          sizeMaxPixels: 22,
          billboard: true,
        }),
      );
    }

    return output;
  }, [
    arrowMarkers,
    bounds,
    currentTimeMs,
    finishPulseMarkers,
    focusedTripId,
    focusedTrip,
    renderedTrips,
    spawnPulseMarkers,
    theme,
  ]);

  const liveLayers = useMemo(
    () => [
      new ScatterplotLayer<BikePointStation>({
        id: "bikepoint-stations",
        data: liveStations,
        pickable: true,
        filled: true,
        stroked: true,
        radiusUnits: "meters",
        radiusMinPixels: 3,
        lineWidthMinPixels: 1,
        getPosition: (station) => [station.lon, station.lat],
        getRadius: (station) => Math.max(18, station.totalDocks * 1.8),
        getFillColor: stationFillColor,
        getLineColor: [28, 45, 65, 190],
        onHover: ({ object }: PickingInfo<BikePointStation>) => {
          setHoveredStation(object ?? null);
        },
        onClick: ({ object }: PickingInfo<BikePointStation>) => {
          setSelectedStation(object ?? null);
        },
      }),
    ],
    [liveStations],
  );

  const tubeLayers = useMemo(() => {
    const routeSegments = filteredTubeLines.flatMap((line) =>
      line.routePolylines.map((path, index) => ({
        id: `${line.lineId}:${index}`,
        lineId: line.lineId,
        path,
        color: [...line.color, 116] as [number, number, number, number],
      })),
    );
    const stationArrivalCounts = tubeArrivalCountByStation;

    const output: Layer[] = [
      new PathLayer<{
        id: string;
        lineId: string;
        path: [number, number][];
        color: [number, number, number, number];
      }>({
        id: "tube-line-routes",
        data: routeSegments,
        pickable: false,
        getPath: (segment) => segment.path,
        getColor: (segment) => segment.color,
        getWidth: 2,
        widthUnits: "pixels",
        widthMinPixels: 1.2,
        capRounded: true,
        jointRounded: true,
        opacity: 0.48,
      }),
      new ScatterplotLayer<TubeStation>({
        id: "tube-stations",
        data: filteredTubeStations,
        pickable: true,
        filled: true,
        stroked: true,
        radiusUnits: "meters",
        radiusMinPixels: 3,
        lineWidthMinPixels: 1,
        getPosition: (station) => [station.lon, station.lat],
        getRadius: (station) => {
          const arrivals = stationArrivalCounts.get(station.id) ?? 0;
          return 14 + station.lines.length * 5 + arrivals * 1.1;
        },
        getFillColor: (station) => {
          const [r, g, b, a] = tubeStatusColor(tubeStationSeverityById.get(station.id) ?? null);
          return [r, g, b, Math.min(148, a)] as [number, number, number, number];
        },
        getLineColor: [14, 19, 31, 230],
        onHover: ({ object }: PickingInfo<TubeStation>) => {
          setTubeHoveredStation(object ?? null);
        },
        onClick: ({ object }: PickingInfo<TubeStation>) => {
          setTubeSelectedStation(object ?? null);
        },
      }),
    ];

    if (tubeMode === "live") {
      output.push(
        new IconLayer<TubeLiveTrainMarker>({
          id: "tube-live-train-shadow",
          data: tubeLiveTrainMarkers,
          pickable: false,
          iconAtlas: TRAIN_BODY_ICON_ATLAS,
          iconMapping: TRAIN_BODY_ICON_MAPPING,
          getIcon: () => "train",
          getPosition: (marker) => marker.position,
          getColor: () => TUBE_LIVE_TRAIN_SHADOW_COLOR,
          getAngle: (marker) => marker.headingDeg,
          getSize: () => 18.5,
          sizeScale: 1,
          sizeUnits: "pixels",
          sizeMinPixels: 15,
          sizeMaxPixels: 25,
          billboard: true,
          opacity: 0.72,
        }),
      );
      output.push(
        new IconLayer<TubeLiveTrainMarker>({
          id: "tube-live-train-outline",
          data: tubeLiveTrainMarkers,
          pickable: false,
          iconAtlas: TRAIN_BODY_ICON_ATLAS,
          iconMapping: TRAIN_BODY_ICON_MAPPING,
          getIcon: () => "train",
          getPosition: (marker) => marker.position,
          getColor: () => TUBE_LIVE_TRAIN_OUTLINE_COLOR,
          getAngle: (marker) => marker.headingDeg,
          getSize: () => 15.6,
          sizeScale: 1,
          sizeUnits: "pixels",
          sizeMinPixels: 12.4,
          sizeMaxPixels: 21,
          billboard: true,
          opacity: 0.9,
        }),
      );
      output.push(
        new IconLayer<TubeLiveTrainMarker>({
          id: "tube-live-trains",
          data: tubeLiveTrainMarkers,
          pickable: true,
          iconAtlas: TRAIN_BODY_ICON_ATLAS,
          iconMapping: TRAIN_BODY_ICON_MAPPING,
          getIcon: () => "train",
          getPosition: (marker) => marker.position,
          getColor: (marker) => tubeLiveTrainBodyColor(marker, tubeLinesById),
          getAngle: (marker) => marker.headingDeg,
          getSize: () => 14.2,
          sizeScale: 1,
          sizeUnits: "pixels",
          sizeMinPixels: 11.4,
          sizeMaxPixels: 19,
          billboard: true,
          onHover: ({ object }: PickingInfo<TubeLiveTrainMarker>) => {
            setTubeHoveredTrain(object ?? null);
          },
          onClick: ({ object }: PickingInfo<TubeLiveTrainMarker>) => {
            setTubeSelectedTrain(object ?? null);
          },
        }),
      );
      output.push(
        new IconLayer<TubeLiveTrainMarker>({
          id: "tube-live-train-nose",
          data: tubeLiveTrainMarkers,
          pickable: false,
          iconAtlas: TRAIN_NOSE_ICON_ATLAS,
          iconMapping: TRAIN_NOSE_ICON_MAPPING,
          getIcon: () => "nose",
          getPosition: (marker) => marker.position,
          getColor: (marker) => tubeLiveTrainNoseColor(marker),
          getAngle: (marker) => marker.headingDeg,
          getSize: () => 6.2,
          sizeScale: 1,
          sizeUnits: "pixels",
          sizeMinPixels: 5,
          sizeMaxPixels: 8.6,
          billboard: true,
          opacity: 0.98,
        }),
      );
    } else {
      output.push(
        new TripsLayer<{
          runId: string;
          lineId: string;
          path: [number, number][];
          timestamps: number[];
          color: [number, number, number];
        }>({
          id: "tube-historical-runs",
          data: tubeHistoricalRunsWithColor,
          getPath: (run) => run.path,
          getTimestamps: (run) => run.timestamps.map((timestamp) => timestamp / 1000),
          getColor: (run) => run.color,
          opacity: 0.48,
          widthMinPixels: 2.2,
          trailLength: 220,
          currentTime: tubeHistoricalCurrentMs / 1000,
          fadeTrail: true,
          capRounded: true,
          jointRounded: true,
          getPolygonOffset: () => [0, -120],
          pickable: false,
        }),
      );
      output.push(
        new ScatterplotLayer<TubeLiveTrainMarker>({
          id: "tube-historical-train-glow",
          data: tubeHistoricalActiveMarkers,
          pickable: false,
          stroked: false,
          filled: true,
          radiusUnits: "meters",
          radiusMinPixels: 10,
          getPosition: (marker) => marker.position,
          getRadius: () => 48,
          getFillColor: () => TUBE_TRAIN_GLOW_COLOR,
          opacity: 0.86,
        }),
      );
      output.push(
        new IconLayer<TubeLiveTrainMarker>({
          id: "tube-historical-markers",
          data: tubeHistoricalActiveMarkers,
          pickable: true,
          iconAtlas: ARROW_ICON_ATLAS,
          iconMapping: ARROW_ICON_MAPPING,
          getIcon: () => "arrow",
          getPosition: (marker) => marker.position,
          getColor: () => TUBE_TRAIN_COLOR,
          getAngle: (marker) => marker.headingDeg,
          getSize: () => 13,
          sizeScale: 1,
          sizeUnits: "pixels",
          sizeMinPixels: 10,
          sizeMaxPixels: 20,
          billboard: true,
          onHover: ({ object }: PickingInfo<TubeLiveTrainMarker>) => {
            setTubeHoveredTrain(object ?? null);
          },
          onClick: ({ object }: PickingInfo<TubeLiveTrainMarker>) => {
            setTubeSelectedTrain(object ?? null);
          },
        }),
      );
    }

    return output;
  }, [
    tubeArrivalCountByStation,
    filteredTubeLines,
    filteredTubeStations,
    tubeHistoricalActiveMarkers,
    tubeHistoricalCurrentMs,
    tubeHistoricalRunsWithColor,
    tubeLiveTrainMarkers,
    tubeLinesById,
    tubeMode,
    tubeStationSeverityById,
  ]);

  if (mode === "playback" && (status === "loading" || !bounds)) {
    return (
      <main className="appStatus">
        <h1>London Bike Trips</h1>
        <p>{loadingMessage}</p>
        {initProgress && initProgress.totalFiles > 0 ? (
          <div className="appStatus__progress">
            <p>
              Files checked {initProgress.checkedFiles}/{initProgress.totalFiles} ({initPercent}%) |
              usable {initProgress.usableFiles} | skipped {initProgress.skippedFiles}
            </p>
            <div className="appStatus__progressBar" role="progressbar" aria-valuenow={initPercent}>
              <span className="appStatus__progressFill" style={{ width: `${initPercent}%` }} />
            </div>
            {initProgress.lastSkippedFile ? (
              <p className="appStatus__warning">
                Last skipped: {shortName(initProgress.lastSkippedFile)}
              </p>
            ) : null}
          </div>
        ) : null}
        <p className="appStatus__hint">Source: {diagnostics.parquetGlobUrl}</p>
        {troubleshootingHints.length > 0 ? (
          <ul className="appStatus__hints">
            {troubleshootingHints.map((hint) => (
              <li key={hint}>{hint}</li>
            ))}
          </ul>
        ) : null}
        <div className="appStatus__actions">
          <button
            type="button"
            className="appStatus__action"
            onClick={() => setMode("live")}
          >
            Switch to Live Stations
          </button>
        </div>
      </main>
    );
  }

  if (mode === "playback" && status === "error") {
    return (
      <main className="appStatus">
        <h1>London Bike Trips</h1>
        <p>{error ?? "Unexpected error while loading data."}</p>
        {troubleshootingHints.length > 0 ? (
          <ul className="appStatus__hints">
            {troubleshootingHints.map((hint) => (
              <li key={hint}>{hint}</li>
            ))}
          </ul>
        ) : null}
        <p className="appStatus__hint">Source: {diagnostics.parquetGlobUrl}</p>
        <div className="appStatus__actions">
          <button
            type="button"
            className="appStatus__action"
            onClick={() => setMode("live")}
          >
            Switch to Live Stations
          </button>
        </div>
      </main>
    );
  }

  if (mode === "tube" && tubeTopologyStatus === "loading") {
    return (
      <main className="appStatus">
        <h1>London Tube</h1>
        <p>Loading Tube network topology...</p>
        <div className="appStatus__progress">
          <p>
            {tubeTopologyProgressLabel} ({tubeTopologyPercent}%)
          </p>
          <div
            className="appStatus__progressBar"
            role="progressbar"
            aria-label="Tube topology loading progress"
            aria-valuemin={0}
            aria-valuemax={100}
            aria-valuenow={tubeTopologyPercent}
          >
            <span
              className="appStatus__progressFill"
              style={{ width: `${tubeTopologyPercent}%` }}
            />
          </div>
        </div>
      </main>
    );
  }

  if (mode === "tube" && tubeTopologyStatus === "error") {
    return (
      <main className="appStatus">
        <h1>London Tube</h1>
        <p>{tubeError ?? "Unexpected error while loading Tube data."}</p>
        <div className="appStatus__actions">
          <button
            type="button"
            className="appStatus__action"
            onClick={() => {
              setTubeTopologyStatus("idle");
              setTubeTopologyProgress({ ...DEFAULT_TUBE_TOPOLOGY_PROGRESS });
              setTubeError(null);
            }}
          >
            Retry Tube Load
          </button>
        </div>
      </main>
    );
  }

  return (
    <main className={`appRoot ${theme === "light" ? "appRoot--light" : ""}`}>
      <DeckGL
        viewState={viewState}
        onViewStateChange={({ viewState: nextViewState, interactionState }) => {
          const isUserInteracting = Boolean(
            interactionState?.isDragging
            || interactionState?.isPanning
            || interactionState?.isRotating
            || interactionState?.isZooming,
          );
          if (cinematicTourPlaying && isUserInteracting) {
            setCinematicTourPlaying(false);
          }
          setViewState(nextViewState as MapViewState);
        }}
        getCursor={({ isHovering }) => (isHovering ? "pointer" : "default")}
        onClick={({ object }) => {
          if (object) {
            return;
          }
          if (mode === "playback") {
            clearTripFocus();
            return;
          }
          if (mode === "live") {
            setHoveredStation(null);
            setSelectedStation(null);
            return;
          }
          setTubeHoveredStation(null);
          setTubeSelectedStation(null);
          setTubeHoveredTrain(null);
          setTubeSelectedTrain(null);
        }}
        controller
        layers={mode === "live" ? liveLayers : mode === "tube" ? tubeLayers : layers}
        style={{
          position: "absolute",
          top: "0",
          right: "0",
          bottom: "0",
          left: "0",
        }}
      >
        <MapView
          ref={mapRef}
          reuseMaps
          mapStyle={mapStyleUrl}
          onLoad={() => {
            updateMapThreeDLayer(is3DMode);
          }}
          onStyleData={() => {
            updateMapThreeDLayer(is3DMode);
          }}
        />
      </DeckGL>

      <section className="hudMode" role="group" aria-label="Mode">
        <button
          type="button"
          className={`hudMode__button ${mode === "playback" ? "isActive" : ""}`}
          onClick={() => setMode("playback")}
        >
          Playback
        </button>
        <button
          type="button"
          className={`hudMode__button ${mode === "live" ? "isActive" : ""}`}
          onClick={() => setMode("live")}
        >
          Live
        </button>
        <button
          type="button"
          className={`hudMode__button ${mode === "tube" ? "isActive" : ""}`}
          onClick={() => setMode("tube")}
        >
          Tube
        </button>
        <button
          type="button"
          className={`hudMode__button ${is3DMode ? "isActive" : ""}`}
          onClick={() => setIs3DMode((current) => !current)}
        >
          3D
        </button>
        <button
          type="button"
          className="hudMode__button hudMode__button--theme"
          onClick={() => setTheme((current) => (current === "dark" ? "light" : "dark"))}
        >
          {theme === "dark" ? "Light" : "Dark"}
        </button>
      </section>

      <section className="hudClock" aria-live="polite">
        <p className="hudClock__date">{hudDateLabel}</p>
        <p className="hudClock__time">{hudTimeLabel}</p>
      </section>

      <aside className="hudStats">
        {mode === "playback" ? (
          <>
            <p className="hudStats__label">RIDES</p>
            <p className="hudStats__value">{visibleTrips.length.toLocaleString()}</p>
            <p className="hudStats__meta">
              OSRM {osrmCount.toLocaleString()} | fallback {fallbackCount.toLocaleString()}
            </p>
            <p className="hudStats__meta">{runtimeMessage ?? "Ready"}</p>
            <div className="hudStats__legend">
              <span className="hudStats__legendItem">
                <i className="hudStats__swatch hudStats__swatch--ebike" />
                E-bike
              </span>
              <span className="hudStats__legendItem">
                <i className="hudStats__swatch hudStats__swatch--classic" />
                Classic bike
              </span>
              <span className="hudStats__legendItem">
                <i className="hudStats__swatch hudStats__swatch--unlocked" />
                Bike unlocked
              </span>
              <span className="hudStats__legendItem">
                <i className="hudStats__swatch hudStats__swatch--docked" />
                Bike docked
              </span>
            </div>
            <p className="hudStats__meta hudStats__meta--subtle">
              Bike class colors are inferred from trip id; this dataset has no bike-type/scooter
              field.
            </p>
            <div className="hudStats__filters" role="group" aria-label="Route source filter">
              <button
                type="button"
                className={`hudStats__chip ${routeFilter === "all" ? "isActive" : ""}`}
                onClick={() => setRouteFilter("all")}
              >
                All
              </button>
              <button
                type="button"
                className={`hudStats__chip ${routeFilter === "osrm" ? "isActive" : ""}`}
                onClick={() => setRouteFilter("osrm")}
              >
                OSRM
              </button>
              <button
                type="button"
                className={`hudStats__chip ${routeFilter === "fallback" ? "isActive" : ""}`}
                onClick={() => setRouteFilter("fallback")}
              >
                Fallback
              </button>
            </div>
            {tripForInspector ? (
              <p className="hudStats__note">
                {selectedTrip ? "Selected" : "Hover"}: {tripForInspector.tripId} |{" "}
                {formatDistance(tripForInspector.routeDistanceM)} |{" "}
                {formatDuration(tripForInspector.routeDurationS)}
              </p>
            ) : null}
          </>
        ) : mode === "live" ? (
          <>
            <p className="hudStats__label">STATIONS</p>
            <p className="hudStats__value">{liveSummary.stations.toLocaleString()}</p>
            <p className="hudStats__meta">
              Bikes {liveSummary.bikes.toLocaleString()} | e-bikes{" "}
              {liveSummary.ebikes.toLocaleString()}
            </p>
            <p className="hudStats__meta">
              Empty docks {liveSummary.docks.toLocaleString()} | refresh{" "}
              {Math.round(LIVE_STATION_REFRESH_MS / 1000)}s
            </p>
            {liveStatus === "error" && liveError ? (
              <p className="hudStats__note">Live error: {liveError}</p>
            ) : null}
            {stationForInspector ? (
              <p className="hudStats__note">
                {selectedStation ? "Selected" : "Hover"}: {stationForInspector.name} | bikes{" "}
                {stationForInspector.availableBikes} | docks{" "}
                {stationForInspector.availableDocks}/{stationForInspector.totalDocks}
              </p>
            ) : null}
          </>
        ) : (
          <>
            <p className="hudStats__label">
              TUBE {tubeMode === "historical" ? "HISTORICAL" : "LIVE"}
            </p>
            <p className="hudStats__value">
              {tubeMode === "historical"
                ? tubeHistoricalSummary.runs.toLocaleString()
                : tubeLiveSummary.trains.toLocaleString()}
            </p>
            <div className="hudStats__filters" role="group" aria-label="Tube view mode">
              <button
                type="button"
                className={`hudStats__chip ${tubeMode === "historical" ? "isActive" : ""}`}
                onClick={() => setTubeMode("historical")}
              >
                Historical
              </button>
              <button
                type="button"
                className={`hudStats__chip ${tubeMode === "live" ? "isActive" : ""}`}
                onClick={() => setTubeMode("live")}
              >
                Live
              </button>
            </div>
            <p className="hudStats__meta">
              Lines {tubeLiveSummary.lines} | Stations {tubeLiveSummary.stations} | Disruptions{" "}
              {tubeLiveSummary.disruptions}
            </p>
            {tubeMode === "live" ? (
              <p className="hudStats__meta">
                Trains {tubeLiveSummary.trains} | refresh {Math.round(TUBE_REFRESH_MS / 1000)}s
              </p>
            ) : (
              <p className="hudStats__meta">
                Active trains {tubeHistoricalSummary.active} | speed {tubeHistoricalSpeed}x
              </p>
            )}
            {tubeError ? <p className="hudStats__note">Tube error: {tubeError}</p> : null}
            {tubeStationForInspector ? (
              <p className="hudStats__note">
                {tubeSelectedStation ? "Selected" : "Hover"} station: {tubeStationForInspector.name} |{" "}
                lines {tubeStationForInspector.lines.length} | zone{" "}
                {tubeStationForInspector.zone ?? "?"}
              </p>
            ) : null}
            {tubeTrainForInspector ? (
              <p className="hudStats__note">
                {tubeSelectedTrain ? "Selected" : "Hover"} train: {tubeTrainForInspector.lineId} |{" "}
                {tubeTrainForInspector.destinationName ?? "En route"} |{" "}
                {tubeTrainForInspector.isInterpolated ? "interpolated" : "at station"}
              </p>
            ) : null}
          </>
        )}
      </aside>

      {mode === "playback" && bounds ? (
        <PlaybackControls
          minTimeMs={bounds.minMs}
          maxTimeMs={bounds.maxMs}
          currentTimeMs={currentTimeMs}
          isPlaying={isPlaying}
          isRandomRideFollowEnabled={autoRandomFollowEnabled}
          speed={speed}
          cinematicTourId={cinematicTourId}
          cinematicTourOptions={cinematicTourOptions}
          cinematicTourSubtitle={selectedCinematicTour?.subtitle ?? ""}
          isCinematicTourPlaying={cinematicTourPlaying}
          onSetTime={setPlaybackTime}
          onTogglePlay={togglePlay}
          onRandomRideFollow={() => {
            if (autoRandomFollowEnabled) {
              clearTripFocus();
              return;
            }
            startRandomRideFollow();
          }}
          onRandomTimeJump={jumpToRandomTime}
          onSetSpeed={(nextSpeed) => setSpeed(nextSpeed)}
          onSetCinematicTour={(nextTourId) => {
            activeCinematicKeyframesRef.current = null;
            setCinematicTourPlaying(false);
            setCinematicTourId(nextTourId as CinematicTourId);
          }}
          onToggleCinematicTour={toggleCinematicTourPlayback}
          onNaturalLanguageJump={jumpToNaturalLanguage}
          jumpError={jumpError}
        />
      ) : null}

      {mode === "tube" ? (
        <section className="commandRail">
          <div className="commandRail__row commandRail__row--speed">
            <span className="commandRail__icon" aria-hidden="true">
              T
            </span>
            <span>Tube View</span>
            <select
              className="commandRail__select"
              value={tubeMode}
              onChange={(event) => {
                const nextMode = event.target.value === "historical" ? "historical" : "live";
                setTubeMode(nextMode);
                if (nextMode === "live") {
                  setTubeHistoricalPlaying(false);
                }
              }}
            >
              <option value="historical">Historical</option>
              <option value="live">Live</option>
            </select>
          </div>

          {tubeMode === "historical" ? (
            <>
              <label className="commandRail__row commandRail__row--speed">
                <span className="commandRail__icon" aria-hidden="true">
                  D
                </span>
                <span>Date</span>
                <input
                  className="commandRail__select"
                  type="date"
                  value={tubeHistoricalDate}
                  onChange={(event) => {
                    setTubeHistoricalPlaying(false);
                    setTubeHistoricalDate(event.target.value);
                  }}
                />
              </label>
              <button
                type="button"
                className="commandRail__row"
                onClick={() => setTubeHistoricalPlaying((playing) => !playing)}
                disabled={tubeHistoricalStatus === "loading"}
              >
                <span className="commandRail__icon" aria-hidden="true">
                  {tubeHistoricalPlaying ? "||" : ">"}
                </span>
                <span>{tubeHistoricalPlaying ? "Pause" : "Play"} Trains</span>
                <kbd className="commandRail__key">Space</kbd>
              </button>
              <label className="commandRail__row commandRail__row--speed">
                <span className="commandRail__icon" aria-hidden="true">
                  S
                </span>
                <span>Speed</span>
                <select
                  className="commandRail__select"
                  value={tubeHistoricalSpeed}
                  onChange={(event) =>
                    setTubeHistoricalSpeed(Math.max(1, Number(event.target.value) || 1))
                  }
                >
                  <option value={1}>1x</option>
                  <option value={10}>10x</option>
                  <option value={30}>30x</option>
                  <option value={60}>60x</option>
                </select>
              </label>
              <div className="commandRail__timeline">
                <div className="commandRail__timeRow">
                  <p className="commandRail__time">
                    {new Date(tubeHistoricalCurrentMs || Date.now()).toLocaleString("en-GB", {
                      day: "2-digit",
                      month: "short",
                      hour: "2-digit",
                      minute: "2-digit",
                    })}
                  </p>
                  <button
                    type="button"
                    className="commandRail__timeRandom"
                    onClick={() => {
                      if (!tubeHistoricalFrame) {
                        return;
                      }
                      const span = Math.max(1, tubeHistoricalFrame.maxMs - tubeHistoricalFrame.minMs);
                      const target = tubeHistoricalFrame.minMs + Math.floor(Math.random() * span);
                      setTubeHistoricalCurrentMs(target);
                    }}
                  >
                    Random time
                  </button>
                </div>
                <input
                  type="range"
                  className="commandRail__slider"
                  min={tubeHistoricalFrame?.minMs ?? 0}
                  max={tubeHistoricalFrame?.maxMs ?? 1}
                  step={15_000}
                  value={tubeHistoricalCurrentMs}
                  onChange={(event) => setTubeHistoricalCurrentMs(Number(event.target.value))}
                  aria-label="Tube historical timeline"
                />
              </div>
            </>
          ) : (
            <button
              type="button"
              className="commandRail__row"
              onClick={async () => {
                try {
                  const [statuses, liveActions, arrivals] = await Promise.all([
                    fetchTubeLineStatuses(),
                    fetchTubeLiveActions().catch(() => ({})),
                    fetchTubeArrivals().catch(() => []),
                  ]);
                  setTubeStatuses(statuses);
                  setTubeLiveActions(liveActions);
                  setTubeArrivals(arrivals);
                  setTubeLastRefreshMs(Date.now());
                  setTubeLiveClockMs(Date.now());
                  setTubeError(null);
                } catch (refreshError) {
                  setTubeError(
                    refreshError instanceof Error
                      ? refreshError.message
                      : "Failed to refresh Tube live data.",
                  );
                }
              }}
            >
              <span className="commandRail__icon" aria-hidden="true">
                R
              </span>
              <span>Refresh Tube Live</span>
              <kbd className="commandRail__key">{Math.round(TUBE_REFRESH_MS / 1000)}s</kbd>
            </button>
          )}

          <div
            className="hudStats__filters commandRail__lineFilters"
            role="group"
            aria-label="Tube line filter"
          >
            <button
              type="button"
              className={`hudStats__chip ${isAllTubeLineFilterActive ? "isActive" : ""}`}
              onClick={() => setTubeLineFilters([])}
            >
              All
            </button>
            {tubeLines.map((line) => (
              <button
                key={line.lineId}
                type="button"
                className={`hudStats__chip ${tubeLineFilters.includes(line.lineId) ? "isActive" : ""}`}
                onClick={() =>
                  setTubeLineFilters((previous) =>
                    previous.includes(line.lineId)
                      ? previous.filter((lineId) => lineId !== line.lineId)
                      : [...previous, line.lineId],
                  )
                }
              >
                {line.lineName}
              </button>
            ))}
          </div>

          <section className="lineInspector" aria-label="Tube line inspector">
            <header className="lineInspector__header">
              <p className="lineInspector__kicker">Line Inspector</p>
              <p className="lineInspector__title">
                {tubeInspectorLine?.lineName ?? "No line selected"}
              </p>
            </header>

            {tubeInspectorLine ? (
              <>
                <div className="lineInspector__controls">
                  <label className="lineInspector__field">
                    <span>Line</span>
                    <select
                      className="commandRail__select lineInspector__select"
                      value={tubeInspectorLineId}
                      onChange={(event) => setTubeInspectorLineId(event.target.value)}
                    >
                      {tubeInspectorLineOptions.map((line) => (
                        <option key={line.lineId} value={line.lineId}>
                          {line.lineName}
                        </option>
                      ))}
                    </select>
                  </label>
                  <div className="lineInspector__direction" role="group" aria-label="Direction">
                    <button
                      type="button"
                      className={`lineInspector__dirButton ${tubeInspectorDirection === "outbound" ? "isActive" : ""}`}
                      onClick={() => setTubeInspectorDirection("outbound")}
                    >
                      Outbound
                    </button>
                    <button
                      type="button"
                      className={`lineInspector__dirButton ${tubeInspectorDirection === "inbound" ? "isActive" : ""}`}
                      onClick={() => setTubeInspectorDirection("inbound")}
                    >
                      Inbound
                    </button>
                  </div>
                </div>

                {tubeInspectorBranches.length > 1 ? (
                  <label className="lineInspector__field">
                    <span>Branch</span>
                    <select
                      className="commandRail__select lineInspector__select"
                      value={tubeInspectorBranch?.key ?? ""}
                      onChange={(event) => setTubeInspectorBranchKey(event.target.value)}
                    >
                      {tubeInspectorBranches.map((branch) => (
                        <option key={branch.key} value={branch.key}>
                          {branch.label}
                        </option>
                      ))}
                    </select>
                  </label>
                ) : null}

                <div
                  className="lineInspector__stationList"
                  role="list"
                  aria-label={`${tubeInspectorLine.lineName} stations in order`}
                >
                  {tubeInspectorStations.map((entry) => (
                    <button
                      key={entry.stationId}
                      type="button"
                      className={`lineInspector__station ${tubeInspectorStationId === entry.stationId ? "isActive" : ""}`}
                      onClick={() => {
                        setTubeInspectorStationId(entry.stationId);
                        const station = tubeStationsById.get(entry.stationId);
                        if (station) {
                          setTubeSelectedStation(station);
                          setTubeHoveredStation(null);
                        }
                      }}
                    >
                      <span className="lineInspector__stationIndex">{entry.index}</span>
                      <span className="lineInspector__stationBody">
                        <span className="lineInspector__stationName">{entry.name}</span>
                        <span className="lineInspector__stationMeta">
                          Zone {entry.zone ?? "?"}
                          {entry.hasInterchange ? " | interchange" : ""}
                        </span>
                      </span>
                      <span className="lineInspector__stationCount" aria-hidden="true">
                        {entry.departures > 0 ? entry.departures : "-"}
                      </span>
                    </button>
                  ))}
                </div>

                <div className="lineInspector__board" aria-live="polite">
                  <p className="lineInspector__boardTitle">
                    Departures board
                    {tubeInspectorStation ? ` - ${tubeInspectorStation.name}` : ""}
                  </p>
                  {tubeMode !== "live" ? (
                    <p className="lineInspector__empty">
                      Switch to Live mode for real-time departure boards.
                    </p>
                  ) : tubeInspectorDepartures.length === 0 ? (
                    <p className="lineInspector__empty">No live departures available right now.</p>
                  ) : (
                    <ul className="lineInspector__boardList">
                      {tubeInspectorDepartures.map((departure) => (
                        <li key={departure.id} className="lineInspector__boardItem">
                          <span
                            className="lineInspector__linePill"
                            style={{
                              backgroundColor: `rgb(${departure.lineColor[0]}, ${departure.lineColor[1]}, ${departure.lineColor[2]})`,
                            }}
                          >
                            {departure.lineName}
                          </span>
                          <span className="lineInspector__destination">{departure.destination}</span>
                          <span className="lineInspector__eta">{departure.etaLabel}</span>
                          <span className="lineInspector__clock">{departure.expectedTimeLabel}</span>
                        </li>
                      ))}
                    </ul>
                  )}
                </div>
              </>
            ) : (
              <p className="lineInspector__empty">No Tube lines are currently available.</p>
            )}
          </section>
        </section>
      ) : null}
    </main>
  );
}

export default App;
