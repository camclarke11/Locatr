export type EncodedTrip = {
  trip_id: string;
  start_time_ms: number;
  end_time_ms: number;
  route_geometry: string;
  route_source: string;
  route_distance_m: number;
  route_duration_s: number;
};

export type DecodedTrip = {
  tripId: string;
  startTimeMs: number;
  endTimeMs: number;
  path: [number, number][];
  timestamps: number[];
  pointCount: number;
  routeSource: string;
  routeDistanceM: number;
  routeDurationS: number;
  isLikelyFallback: boolean;
};

export type TimeBounds = {
  minMs: number;
  maxMs: number;
};

export type PlaybackSpeed = 1 | 10 | 60 | 100;

export type TubeMode = "historical" | "live";

export type TubeDirection = "inbound" | "outbound";

export type TubeBranchSequence = {
  stationIds: string[];
  path: [number, number][];
};

export type TubeLineMeta = {
  lineId: string;
  lineName: string;
  color: [number, number, number];
  routePolylines: [number, number][][];
  orderedStations: Record<TubeDirection, string[]>;
  stationPaths: Record<TubeDirection, [number, number][]>;
  branchSequences: Record<TubeDirection, TubeBranchSequence[]>;
  segmentDurationsSec: Record<TubeDirection, number[]>;
};

export type TubeStation = {
  id: string;
  name: string;
  lat: number;
  lon: number;
  lines: string[];
  zone: string | null;
  isInterchange: boolean;
};

export type TubeLineStatus = {
  lineId: string;
  severity: number;
  severityLabel: string;
  reason: string | null;
  hasDisruption: boolean;
};

export type TubeArrival = {
  vehicleId: string;
  lineId: string;
  naptanId: string;
  destinationNaptanId: string | null;
  expectedArrival: string;
  timeToStation: number;
  timestamp: string;
  towards: string | null;
};

export type TubeLiveTrainMarker = {
  trainKey: string;
  lineId: string;
  position: [number, number];
  headingDeg: number;
  nextStopId: string;
  progress01: number;
  isInterpolated: boolean;
  destinationName: string | null;
  timeToStation: number;
};

export type TubeLiveActionStep = {
  fromStopId: string;
  toStopId: string;
  fromTimestamp: number;
  toTimestamp: number;
};

export type TubeLiveActionsByLine = Record<string, Record<string, TubeLiveActionStep[]>>;

export type TubeHistoricalRun = {
  runId: string;
  lineId: string;
  direction: TubeDirection;
  departureTimeMs: number;
  path: [number, number][];
  timestamps: number[];
};

export type TubeHistoricalFrame = {
  selectedDate: string;
  minMs: number;
  maxMs: number;
  runs: TubeHistoricalRun[];
};
