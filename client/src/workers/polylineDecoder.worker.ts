/// <reference lib="webworker" />

import polyline from "@mapbox/polyline";

import type { DecodedTrip, EncodedTrip } from "../types";

type DecodeMessage = {
  requestId: number;
  trips: EncodedTrip[];
};

type DecodeResultMessage = {
  requestId: number;
  trips: DecodedTrip[];
};

function buildTimestamps(startMs: number, endMs: number, length: number): number[] {
  if (length <= 1) {
    return [startMs];
  }
  const duration = Math.max(1, endMs - startMs);
  const step = duration / (length - 1);
  const timestamps = new Array<number>(length);
  for (let i = 0; i < length; i += 1) {
    timestamps[i] = startMs + i * step;
  }
  return timestamps;
}

self.onmessage = (event: MessageEvent<DecodeMessage>): void => {
  const { requestId, trips } = event.data;
  const decoded: DecodedTrip[] = [];

  for (const trip of trips) {
    try {
      const coordinates = polyline.decode(trip.route_geometry, 6);
      if (coordinates.length === 0) {
        continue;
      }
      const path = coordinates.map(([lat, lon]) => [lon, lat] as [number, number]);
      decoded.push({
        tripId: trip.trip_id,
        startTimeMs: trip.start_time_ms,
        endTimeMs: trip.end_time_ms,
        path,
        timestamps: buildTimestamps(trip.start_time_ms, trip.end_time_ms, path.length),
      });
    } catch {
      // Skip malformed polyline strings; keeping UI responsive is the priority.
    }
  }

  const message: DecodeResultMessage = { requestId, trips: decoded };
  self.postMessage(message);
};
