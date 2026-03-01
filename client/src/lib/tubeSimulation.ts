import type {
  TubeArrival,
  TubeLiveActionsByLine,
  TubeDirection,
  TubeHistoricalFrame,
  TubeHistoricalRun,
  TubeLineMeta,
  TubeLiveTrainMarker,
  TubeStation,
} from "../types";
import type { TubeTimetableResponse } from "./tubeClient";

function clamp01(value: number): number {
  return Math.max(0, Math.min(1, value));
}

function headingDegrees(from: [number, number], to: [number, number]): number {
  const dx = to[0] - from[0];
  const dy = to[1] - from[1];
  if (Math.abs(dx) < 1e-12 && Math.abs(dy) < 1e-12) {
    return 0;
  }
  return (Math.atan2(dy, dx) * 180) / Math.PI;
}

function interpolatePoint(
  a: [number, number],
  b: [number, number],
  t: number,
): [number, number] {
  return [a[0] + (b[0] - a[0]) * t, a[1] + (b[1] - a[1]) * t];
}

function utcSecondsSinceMidnight(timestampMs: number): number {
  const date = new Date(timestampMs);
  return (
    date.getUTCHours() * 3600
    + date.getUTCMinutes() * 60
    + date.getUTCSeconds()
    + date.getUTCMilliseconds() / 1000
  );
}

function sortAscending(values: number[]): number[] {
  return [...values].sort((a, b) => a - b);
}

function directionOriginStopIds(
  line: TubeLineMeta,
  direction: TubeDirection,
): Set<string> {
  const origins = new Set<string>();
  for (const sequence of line.branchSequences[direction] ?? []) {
    const origin = sequence.stationIds[0];
    if (origin) {
      origins.add(origin);
    }
  }
  if (origins.size === 0) {
    const fallbackOrigin = line.orderedStations[direction][0];
    if (fallbackOrigin) {
      origins.add(fallbackOrigin);
    }
  }
  return origins;
}

function normalizeSegmentDurations(
  values: number[] | undefined,
  segments: number,
): number[] {
  if (!values || values.length === 0) {
    return Array.from({ length: segments }, () => 120);
  }
  const output = values
    .map((value) => Math.max(20, Math.min(1_200, Math.round(value))))
    .slice(0, segments);
  while (output.length < segments) {
    output.push(output[output.length - 1] ?? 120);
  }
  return output;
}

function extractDirectionDurations(
  line: TubeLineMeta,
  direction: TubeDirection,
  timetables: TubeTimetableResponse[],
): number[] {
  const segmentCount = Math.max(0, line.stationPaths[direction].length - 1);
  if (segmentCount === 0) {
    return [];
  }
  const candidateDurations: number[][] = [];
  const origins = directionOriginStopIds(line, direction);
  for (const timetable of timetables) {
    if (!origins.has(timetable.departureStopId)) {
      continue;
    }
    for (const route of timetable.routes) {
      for (const interval of route.stationIntervals) {
        if (interval.intervalsSec.length === 0) {
          continue;
        }
        candidateDurations.push(interval.intervalsSec);
      }
    }
  }
  if (candidateDurations.length === 0) {
    return normalizeSegmentDurations(undefined, segmentCount);
  }

  const best = candidateDurations.sort((a, b) => {
    const deltaA = Math.abs(a.length - segmentCount);
    const deltaB = Math.abs(b.length - segmentCount);
    return deltaA - deltaB;
  })[0];
  return normalizeSegmentDurations(best, segmentCount);
}

export function mergeLineSegmentDurations(
  lines: TubeLineMeta[],
  timetablesByLineId: Record<string, TubeTimetableResponse[]>,
): TubeLineMeta[] {
  return lines.map((line) => {
    const timetables = timetablesByLineId[line.lineId] ?? [];
    return {
      ...line,
      segmentDurationsSec: {
        inbound: extractDirectionDurations(line, "inbound", timetables),
        outbound: extractDirectionDurations(line, "outbound", timetables),
      },
    };
  });
}

function collectDeparturesMs(
  selectedDate: string,
  timetables: TubeTimetableResponse[],
  originStopIds: Set<string>,
): number[] {
  const dayStartMs = new Date(`${selectedDate}T00:00:00`).getTime();
  if (!Number.isFinite(dayStartMs)) {
    return [];
  }
  const departures: number[] = [];
  for (const timetable of timetables) {
    if (!originStopIds.has(timetable.departureStopId)) {
      continue;
    }
    for (const route of timetable.routes) {
      for (const journey of route.knownJourneys) {
        departures.push(dayStartMs + (journey.hour * 60 + journey.minute) * 60_000);
      }
    }
  }
  return sortAscending(departures);
}

function buildRunTimestamps(departureMs: number, segmentDurationsSec: number[]): number[] {
  const timestamps: number[] = [departureMs];
  let current = departureMs;
  for (const durationSec of segmentDurationsSec) {
    current += Math.max(20, durationSec) * 1_000;
    timestamps.push(current);
  }
  return timestamps;
}

export function buildTubeHistoricalFrame(
  selectedDate: string,
  lines: TubeLineMeta[],
  timetablesByLineId: Record<string, TubeTimetableResponse[]>,
): TubeHistoricalFrame {
  const runs: TubeHistoricalRun[] = [];
  let runSequence = 0;
  const dayStartMs = new Date(`${selectedDate}T00:00:00`).getTime();
  const dayEndMs = dayStartMs + 24 * 60 * 60 * 1_000;

  for (const line of lines) {
    const timetables = timetablesByLineId[line.lineId] ?? [];
    for (const direction of ["inbound", "outbound"] as const) {
      const branchCandidates = line.branchSequences[direction]?.length
        ? line.branchSequences[direction]
        : [
          {
            stationIds: line.orderedStations[direction],
            path: line.stationPaths[direction],
          },
        ];
      for (let branchIndex = 0; branchIndex < branchCandidates.length; branchIndex += 1) {
        const branch = branchCandidates[branchIndex];
        const path = branch?.path ?? [];
        if (path.length < 2) {
          continue;
        }
        const branchOriginIds = new Set<string>();
        const branchOrigin = branch.stationIds[0];
        if (branchOrigin) {
          branchOriginIds.add(branchOrigin);
        }
        const departures = collectDeparturesMs(
          selectedDate,
          timetables,
          branchOriginIds.size > 0 ? branchOriginIds : directionOriginStopIds(line, direction),
        );
        const durations = normalizeSegmentDurations(
          line.segmentDurationsSec[direction],
          path.length - 1,
        );
        for (const departureMs of departures) {
          if (departureMs < dayStartMs || departureMs > dayEndMs) {
            continue;
          }
          const timestamps = buildRunTimestamps(departureMs, durations);
          runSequence += 1;
          runs.push({
            runId: `${line.lineId}:${direction}:b${branchIndex}:${departureMs}:${runSequence}`,
            lineId: line.lineId,
            direction,
            departureTimeMs: departureMs,
            path,
            timestamps,
          });
        }
      }
    }
  }

  const minMs = runs.length > 0
    ? Math.min(...runs.map((run) => run.departureTimeMs))
    : dayStartMs;
  const maxMs = runs.length > 0
    ? Math.max(...runs.map((run) => run.timestamps[run.timestamps.length - 1] ?? run.departureTimeMs))
    : dayEndMs;

  return {
    selectedDate,
    minMs,
    maxMs,
    runs: runs.sort((a, b) => a.departureTimeMs - b.departureTimeMs),
  };
}

function chooseDirection(
  line: TubeLineMeta,
  nextStopId: string,
  destinationStopId: string | null,
): TubeDirection | null {
  const inboundIdx = line.orderedStations.inbound.indexOf(nextStopId);
  const outboundIdx = line.orderedStations.outbound.indexOf(nextStopId);

  if (destinationStopId) {
    if (outboundIdx >= 0) {
      const destIdx = line.orderedStations.outbound.indexOf(destinationStopId);
      if (destIdx >= outboundIdx) {
        return "outbound";
      }
    }
    if (inboundIdx >= 0) {
      const destIdx = line.orderedStations.inbound.indexOf(destinationStopId);
      if (destIdx >= inboundIdx) {
        return "inbound";
      }
    }
  }

  if (outboundIdx >= 0) {
    return "outbound";
  }
  if (inboundIdx >= 0) {
    return "inbound";
  }
  return null;
}

function nearestArrivalPerTrain(arrivals: TubeArrival[]): TubeArrival[] {
  const bestByTrain = new Map<string, TubeArrival>();
  for (const arrival of arrivals) {
    const keyBase = arrival.vehicleId.trim();
    const trainKey = `${arrival.lineId}:${keyBase.length > 0 ? keyBase : `${arrival.naptanId}:${arrival.timestamp}`}`;
    const existing = bestByTrain.get(trainKey);
    if (!existing || arrival.timeToStation < existing.timeToStation) {
      bestByTrain.set(trainKey, arrival);
    }
  }
  return Array.from(bestByTrain.values());
}

function normalizeActionWindow(
  fromTimestamp: number,
  toTimestamp: number,
): { startSec: number; endSec: number } {
  const startSec = Math.min(fromTimestamp, toTimestamp);
  const endSec = Math.max(fromTimestamp, toTimestamp);
  return { startSec, endSec };
}

function chooseBestActionStep(
  steps: Array<{
    fromStopId: string;
    toStopId: string;
    fromTimestamp: number;
    toTimestamp: number;
  }>,
  nowSec: number,
): {
  fromStopId: string;
  toStopId: string;
  startSec: number;
  endSec: number;
} | null {
  let best:
    | {
      fromStopId: string;
      toStopId: string;
      startSec: number;
      endSec: number;
    }
    | null = null;
  let bestDistance = Number.POSITIVE_INFINITY;
  for (const step of steps) {
    const { startSec, endSec } = normalizeActionWindow(step.fromTimestamp, step.toTimestamp);
    const distance = nowSec < startSec
      ? startSec - nowSec
      : nowSec > endSec
        ? nowSec - endSec
        : 0;
    if (distance < bestDistance) {
      bestDistance = distance;
      best = {
        fromStopId: step.fromStopId,
        toStopId: step.toStopId,
        startSec,
        endSec,
      };
      if (distance === 0) {
        break;
      }
    }
  }
  return best;
}

export function buildTubeLiveTrainMarkersFromActions(
  actionsByLine: TubeLiveActionsByLine,
  lines: TubeLineMeta[],
  stations: TubeStation[],
  timestampMs = Date.now(),
): TubeLiveTrainMarker[] {
  const stationById = new Map(stations.map((station) => [station.id, station]));
  const markers: TubeLiveTrainMarker[] = [];
  const nowSec = utcSecondsSinceMidnight(timestampMs);

  for (const line of lines) {
    const lineActions = actionsByLine[line.lineId];
    if (!lineActions) {
      continue;
    }
    for (const [trainId, steps] of Object.entries(lineActions)) {
      if (!Array.isArray(steps) || steps.length === 0) {
        continue;
      }
      const selected = chooseBestActionStep(steps, nowSec);
      if (!selected) {
        continue;
      }
      const fromStation = stationById.get(selected.fromStopId);
      const toStation = stationById.get(selected.toStopId);
      if (!fromStation || !toStation) {
        continue;
      }

      const spanSec = Math.max(1, selected.endSec - selected.startSec);
      const progress01 = clamp01((nowSec - selected.startSec) / spanSec);
      const isSameStop = selected.fromStopId === selected.toStopId;
      const from: [number, number] = [fromStation.lon, fromStation.lat];
      const to: [number, number] = [toStation.lon, toStation.lat];
      const position = isSameStop ? to : interpolatePoint(from, to, progress01);

      markers.push({
        trainKey: `${line.lineId}:${trainId}`,
        lineId: line.lineId,
        position,
        headingDeg: isSameStop ? 0 : headingDegrees(from, to),
        nextStopId: selected.toStopId,
        progress01: isSameStop ? 0 : progress01,
        isInterpolated: !isSameStop,
        destinationName: toStation.name,
        timeToStation: Math.max(0, Math.round(selected.endSec - nowSec)),
      });
    }
  }

  return markers;
}

export function buildTubeLiveTrainMarkers(
  arrivals: TubeArrival[],
  lines: TubeLineMeta[],
  stations: TubeStation[],
  timestampMs = Date.now(),
): TubeLiveTrainMarker[] {
  const linesById = new Map(lines.map((line) => [line.lineId, line]));
  const stationById = new Map(stations.map((station) => [station.id, station]));
  const markers: TubeLiveTrainMarker[] = [];

  for (const arrival of nearestArrivalPerTrain(arrivals)) {
    const line = linesById.get(arrival.lineId);
    if (!line) {
      continue;
    }
    const nextStation = stationById.get(arrival.naptanId);
    if (!nextStation) {
      continue;
    }
    const arrivalTimestampMs = Date.parse(arrival.timestamp);
    const elapsedSec = Number.isFinite(arrivalTimestampMs)
      ? Math.max(0, (timestampMs - arrivalTimestampMs) / 1000)
      : 0;
    const adjustedTimeToStation = Math.max(0, arrival.timeToStation - elapsedSec);

    const direction = chooseDirection(line, arrival.naptanId, arrival.destinationNaptanId);
    const trainKey = `${arrival.lineId}:${arrival.vehicleId || `${arrival.naptanId}:${arrival.timestamp}`}`;
    if (!direction) {
      markers.push({
        trainKey,
        lineId: arrival.lineId,
        position: [nextStation.lon, nextStation.lat],
        headingDeg: 0,
        nextStopId: arrival.naptanId,
        progress01: 0,
        isInterpolated: false,
        destinationName: arrival.towards,
        timeToStation: adjustedTimeToStation,
      });
      continue;
    }

    const ordered = line.orderedStations[direction];
    const nextIndex = ordered.indexOf(arrival.naptanId);
    const prevStopId = nextIndex > 0 ? ordered[nextIndex - 1] : null;
    const prevStation = prevStopId ? stationById.get(prevStopId) : null;
    const segmentDuration = line.segmentDurationsSec[direction][Math.max(0, nextIndex - 1)] ?? 120;
    if (!prevStation || segmentDuration <= 0 || nextIndex <= 0) {
      markers.push({
        trainKey,
        lineId: arrival.lineId,
        position: [nextStation.lon, nextStation.lat],
        headingDeg: 0,
        nextStopId: arrival.naptanId,
        progress01: 0,
        isInterpolated: false,
        destinationName: arrival.towards,
        timeToStation: adjustedTimeToStation,
      });
      continue;
    }

    const progress01 = clamp01(1 - adjustedTimeToStation / segmentDuration);
    const from: [number, number] = [prevStation.lon, prevStation.lat];
    const to: [number, number] = [nextStation.lon, nextStation.lat];
    markers.push({
      trainKey,
      lineId: arrival.lineId,
      position: interpolatePoint(from, to, progress01),
      headingDeg: headingDegrees(from, to),
      nextStopId: arrival.naptanId,
      progress01,
      isInterpolated: true,
      destinationName: arrival.towards,
      timeToStation: adjustedTimeToStation,
    });
  }

  return markers;
}
