/// <reference lib="webworker" />

import polyline from "@mapbox/polyline";

import type { DecodedTrip, EncodedTrip } from "../types";

type DecodeMessage = {
  requestId: number;
  trips: EncodedTrip[];
  timeOffsetMs: number;
};

type DecodeResultMessage = {
  requestId: number;
  trips: DecodedTrip[];
};

function buildTimestamps(
  startMs: number,
  endMs: number,
  length: number,
  timeOffsetMs: number,
): number[] {
  const start = (startMs - timeOffsetMs) / 1000;
  const end = (endMs - timeOffsetMs) / 1000;
  if (length <= 1) {
    return [start];
  }
  const duration = Math.max(1 / 1000, end - start);
  const step = duration / (length - 1);
  const timestamps = new Array<number>(length);
  for (let i = 0; i < length; i += 1) {
    timestamps[i] = start + i * step;
  }
  return timestamps;
}

self.onmessage = (event: MessageEvent<DecodeMessage>): void => {
  const { requestId, trips, timeOffsetMs } = event.data;
  const decoded: DecodedTrip[] = [];

  for (const trip of trips) {
    try {
      const coordinates = polyline.decode(trip.route_geometry, 6);
      if (coordinates.length === 0) {
        continue;
      }
      const path = coordinates.map(([lat, lon]) => [lon, lat] as [number, number]);
      const pointCount = path.length;
      const routeSource = trip.route_source || "unknown";
      decoded.push({
        tripId: trip.trip_id,
        startTimeMs: trip.start_time_ms,
        endTimeMs: trip.end_time_ms,
        path,
        timestamps: buildTimestamps(
          trip.start_time_ms,
          trip.end_time_ms,
          path.length,
          timeOffsetMs,
        ),
        pointCount,
        routeSource,
        routeDistanceM: trip.route_distance_m,
        routeDurationS: trip.route_duration_s,
        isLikelyFallback: routeSource.startsWith("fallback") || pointCount <= 2,
      });
    } catch {
      // Skip malformed polyline strings; keeping UI responsive is the priority.
    }
  }

  const message: DecodeResultMessage = { requestId, trips: decoded };
  self.postMessage(message);
};
