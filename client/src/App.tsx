import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import DeckGL from "@deck.gl/react";
import { TripsLayer } from "@deck.gl/geo-layers";
import { IconLayer, PathLayer, ScatterplotLayer, SolidPolygonLayer } from "@deck.gl/layers";
import MapView from "react-map-gl/maplibre";
import type { Layer, MapViewState, PickingInfo } from "@deck.gl/core";

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
const THEME_STORAGE_KEY = "locatr.theme";
const EVENT_PULSE_WINDOW_MS = 14_000;
const ARROW_LEAD_SEGMENTS = 0.08;
const TRIP_TRAIL_LENGTH_SECONDS = 180;
const TRIP_TRAIL_OPACITY = 0.24;
const TRIP_TRAIL_MIN_WIDTH_PX = 3;
const FOLLOW_BLEND_FACTOR = 0.2;
const FOLLOW_LOOKAHEAD_SEGMENTS = 0.28;
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
const ARROW_COLORS: Record<ArrowCategory, [number, number, number, number]> = {
  ebike: [110, 196, 255, 230],
  classic: [183, 146, 255, 230],
  unlocked: [86, 224, 168, 230],
  docked: [255, 126, 173, 230],
};
const TUBE_TRAIN_COLOR: [number, number, number, number] = [255, 236, 92, 250];
const TUBE_TRAIN_GLOW_COLOR: [number, number, number, number] = [255, 132, 48, 172];
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

function App() {
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
  } = useTripPlayback();
  const [mode, setMode] = useState<AppMode>("playback");
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
  const [routeFilter, setRouteFilter] = useState<RouteFilterMode>("osrm");
  const [hoveredTrip, setHoveredTrip] = useState<DecodedTrip | null>(null);
  const [selectedTrip, setSelectedTrip] = useState<DecodedTrip | null>(null);
  const [autoRandomFollowEnabled, setAutoRandomFollowEnabled] = useState(false);
  const [viewState, setViewState] = useState<MapViewState>(LONDON_VIEW_STATE);
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
  const autoAdvancedTripIdRef = useRef<string | null>(null);

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
    () => buildTubeLiveTrainMarkers(filteredTubeArrivals, filteredTubeLines, filteredTubeStations),
    [filteredTubeArrivals, filteredTubeLines, filteredTubeStations],
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

  const clearTripFocus = useCallback(() => {
    setSelectedTrip(null);
    setHoveredTrip(null);
    setAutoRandomFollowEnabled(false);
    autoAdvancedTripIdRef.current = null;
  }, []);

  useEffect(() => {
    if (typeof window !== "undefined") {
      window.localStorage.setItem(THEME_STORAGE_KEY, theme);
    }
  }, [theme]);

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
    const timer = window.setInterval(() => {
      setTubeLiveClockMs(Date.now());
    }, 1_000);
    return () => window.clearInterval(timer);
  }, [mode, tubeMode]);

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
    if (
      mode !== "tube" ||
      tubeMode !== "historical" ||
      !tubeHistoricalPlaying ||
      !tubeHistoricalFrame
    ) {
      return;
    }
    let animationFrameId = 0;
    let previousTimestamp = performance.now();

    const step = (timestamp: number): void => {
      const deltaMs = Math.max(0, timestamp - previousTimestamp);
      previousTimestamp = timestamp;

      setTubeHistoricalCurrentMs((previous) => {
        const next = previous + deltaMs * tubeHistoricalSpeed;
        if (next >= tubeHistoricalFrame.maxMs) {
          setTubeHistoricalPlaying(false);
          return tubeHistoricalFrame.maxMs;
        }
        return next;
      });
      animationFrameId = window.requestAnimationFrame(step);
    };

    animationFrameId = window.requestAnimationFrame(step);
    return () => window.cancelAnimationFrame(animationFrameId);
  }, [mode, tubeHistoricalFrame, tubeHistoricalPlaying, tubeHistoricalSpeed, tubeMode]);

  const followedTripPosition = useMemo(() => {
    if (mode !== "playback" || !autoRandomFollowEnabled || !selectedTrip) {
      return null;
    }
    return tripPositionAtTime(selectedTrip, currentTimeMs, FOLLOW_LOOKAHEAD_SEGMENTS);
  }, [autoRandomFollowEnabled, currentTimeMs, mode, selectedTrip]);

  useEffect(() => {
    if (mode !== "playback" || !autoRandomFollowEnabled || !followedTripPosition) {
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
  }, [autoRandomFollowEnabled, followedTripPosition, mode]);

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
    const stationArrivalCounts = new Map<string, number>();
    for (const arrival of filteredTubeArrivals) {
      stationArrivalCounts.set(
        arrival.naptanId,
        (stationArrivalCounts.get(arrival.naptanId) ?? 0) + 1,
      );
    }

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
        new ScatterplotLayer<TubeLiveTrainMarker>({
          id: "tube-live-train-glow",
          data: tubeLiveTrainMarkers,
          pickable: false,
          stroked: false,
          filled: true,
          radiusUnits: "meters",
          radiusMinPixels: 12,
          getPosition: (marker) => marker.position,
          getRadius: () => 56,
          getFillColor: () => TUBE_TRAIN_GLOW_COLOR,
          opacity: 0.9,
        }),
      );
      output.push(
        new IconLayer<TubeLiveTrainMarker>({
          id: "tube-live-trains",
          data: tubeLiveTrainMarkers,
          pickable: true,
          iconAtlas: ARROW_ICON_ATLAS,
          iconMapping: ARROW_ICON_MAPPING,
          getIcon: () => "arrow",
          getPosition: (marker) => marker.position,
          getColor: () => TUBE_TRAIN_COLOR,
          getAngle: (marker) => marker.headingDeg,
          getSize: () => 14,
          sizeScale: 1,
          sizeUnits: "pixels",
          sizeMinPixels: 11,
          sizeMaxPixels: 22,
          billboard: true,
          onHover: ({ object }: PickingInfo<TubeLiveTrainMarker>) => {
            setTubeHoveredTrain(object ?? null);
          },
          onClick: ({ object }: PickingInfo<TubeLiveTrainMarker>) => {
            setTubeSelectedTrain(object ?? null);
          },
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
    filteredTubeArrivals,
    filteredTubeLines,
    filteredTubeStations,
    tubeHistoricalActiveMarkers,
    tubeHistoricalCurrentMs,
    tubeHistoricalRunsWithColor,
    tubeLiveTrainMarkers,
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
        onViewStateChange={({ viewState: nextViewState }) => {
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
        <MapView reuseMaps mapStyle={mapStyleUrl} />
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
        </section>
      ) : null}
    </main>
  );
}

export default App;
