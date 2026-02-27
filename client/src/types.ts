export type EncodedTrip = {
  trip_id: string;
  start_time_ms: number;
  end_time_ms: number;
  route_geometry: string;
};

export type DecodedTrip = {
  tripId: string;
  startTimeMs: number;
  endTimeMs: number;
  path: [number, number][];
  timestamps: number[];
};

export type TimeBounds = {
  minMs: number;
  maxMs: number;
};

export type PlaybackSpeed = 1 | 10 | 60;
