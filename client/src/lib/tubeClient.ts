import { TFL_APP_ID, TFL_APP_KEY, TUBE_REQUEST_TIMEOUT_MS } from "../config";
import type {
  TubeArrival,
  TubeBranchSequence,
  TubeDirection,
  TubeLiveActionStep,
  TubeLiveActionsByLine,
  TubeLineMeta,
  TubeLineStatus,
  TubeStation,
} from "../types";

type UnknownRecord = Record<string, unknown>;

type TubeLineApiRecord = {
  id?: string;
  name?: string;
  colour?: string;
  lineColour?: string;
};

type TubeRouteStopRecord = {
  id?: string;
  stationId?: string;
  name?: string;
  lat?: number;
  lon?: number;
};

type TubeRouteSequenceApiRecord = {
  lineStrings?: string[];
  stopPointSequences?: Array<{
    direction?: string;
    stopPoint?: TubeRouteStopRecord[];
  }>;
};

type TubeStationApiRecord = {
  id?: string;
  stationNaptan?: string;
  commonName?: string;
  lat?: number;
  lon?: number;
  zone?: string;
  lines?: Array<{ id?: string }>;
};

export type TubeTimetableJourney = {
  hour: number;
  minute: number;
  intervalId: number;
};

export type TubeTimetableRoute = {
  stationIntervals: Array<{
    id: string;
    intervalsSec: number[];
  }>;
  knownJourneys: TubeTimetableJourney[];
};

export type TubeTimetableResponse = {
  lineId: string;
  departureStopId: string;
  routes: TubeTimetableRoute[];
};

export type TubeTopologyProgressEvent = {
  phase: "starting" | "lines" | "routes" | "stations" | "finalizing" | "done";
  percent: number;
  completedLines: number;
  totalLines: number;
};

const TUBE_BASE_URL = "https://api.tfl.gov.uk";
const LUL_ACTIONS_URL = "https://api.londonunderground.live/get_actions_for_all_trains";

const TUBE_LINE_COLORS: Record<string, [number, number, number]> = {
  bakerloo: [166, 90, 42],
  central: [220, 36, 31],
  circle: [255, 205, 0],
  district: [0, 125, 50],
  "hammersmith-city": [244, 169, 190],
  jubilee: [161, 165, 167],
  metropolitan: [155, 0, 88],
  northern: [0, 0, 0],
  piccadilly: [0, 25, 168],
  victoria: [0, 152, 216],
  "waterloo-city": [149, 205, 186],
};
const FALLBACK_TUBE_LINES: Array<{ id: string; name: string }> = [
  { id: "bakerloo", name: "Bakerloo" },
  { id: "central", name: "Central" },
  { id: "circle", name: "Circle" },
  { id: "district", name: "District" },
  { id: "hammersmith-city", name: "Hammersmith & City" },
  { id: "jubilee", name: "Jubilee" },
  { id: "metropolitan", name: "Metropolitan" },
  { id: "northern", name: "Northern" },
  { id: "piccadilly", name: "Piccadilly" },
  { id: "victoria", name: "Victoria" },
  { id: "waterloo-city", name: "Waterloo & City" },
];

function toNumber(value: unknown, fallback = 0): number {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === "string") {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : fallback;
  }
  return fallback;
}

function normalizeDirection(value: string | undefined): TubeDirection | null {
  if (!value) {
    return null;
  }
  const lower = value.toLowerCase();
  if (lower === "inbound" || lower === "outbound") {
    return lower;
  }
  return null;
}

function cleanStationName(name: string): string {
  return name.replace(/\s+Underground Station$/i, "").trim();
}

function isPlaceholderVehicleId(value: string): boolean {
  const trimmed = value.trim();
  if (trimmed.length === 0) {
    return true;
  }
  if (/^0+$/.test(trimmed)) {
    return true;
  }
  const lower = trimmed.toLowerCase();
  return lower === "null" || lower === "unknown" || lower === "tbc";
}

function parseColorHex(color: string): [number, number, number] | null {
  const cleaned = color.replace("#", "").trim();
  if (!/^[a-fA-F0-9]{6}$/.test(cleaned)) {
    return null;
  }
  return [
    Number.parseInt(cleaned.slice(0, 2), 16),
    Number.parseInt(cleaned.slice(2, 4), 16),
    Number.parseInt(cleaned.slice(4, 6), 16),
  ];
}

function lineColor(lineId: string, record: TubeLineApiRecord): [number, number, number] {
  const apiColor = parseColorHex(record.colour ?? record.lineColour ?? "");
  if (apiColor) {
    return apiColor;
  }
  return TUBE_LINE_COLORS[lineId] ?? [132, 170, 220];
}

function buildApiUrl(path: string): string {
  const url = new URL(path, TUBE_BASE_URL);
  if (TFL_APP_ID) {
    url.searchParams.set("app_id", TFL_APP_ID);
  }
  if (TFL_APP_KEY) {
    url.searchParams.set("app_key", TFL_APP_KEY);
  }
  return url.toString();
}

async function fetchJson<T>(path: string): Promise<T> {
  const response = await fetchWithTimeout(path);
  if (!response.ok) {
    throw new Error(`TfL request failed (${response.status}) for ${path}`);
  }
  return await parseJsonWithTimeout<T>(response, path);
}

async function parseJsonWithTimeout<T>(response: Response, path: string): Promise<T> {
  let timeoutId: ReturnType<typeof setTimeout> | null = null;
  const parseTimeoutPromise = new Promise<never>((_, reject) => {
    timeoutId = globalThis.setTimeout(() => {
      reject(
        new Error(
          `TfL response parse timed out after ${Math.round(TUBE_REQUEST_TIMEOUT_MS / 1000)}s for ${path}`,
        ),
      );
    }, TUBE_REQUEST_TIMEOUT_MS);
  });
  try {
    const parsed = await Promise.race([response.json() as Promise<T>, parseTimeoutPromise]);
    return parsed;
  } finally {
    if (timeoutId !== null) {
      globalThis.clearTimeout(timeoutId);
    }
  }
}

async function fetchWithTimeout(path: string): Promise<Response> {
  const controller = new AbortController();
  let timeoutId: ReturnType<typeof setTimeout> | null = null;
  const timeoutPromise = new Promise<never>((_, reject) => {
    timeoutId = globalThis.setTimeout(() => {
      controller.abort();
      reject(
        new Error(
          `TfL request timed out after ${Math.round(TUBE_REQUEST_TIMEOUT_MS / 1000)}s for ${path}`,
        ),
      );
    }, TUBE_REQUEST_TIMEOUT_MS);
  });
  try {
    const response = await Promise.race([
      fetch(buildApiUrl(path), { signal: controller.signal }),
      timeoutPromise,
    ]);
    return response;
  } catch (error) {
    if (error instanceof DOMException && error.name === "AbortError") {
      throw new Error(
        `TfL request timed out after ${Math.round(TUBE_REQUEST_TIMEOUT_MS / 1000)}s for ${path}`,
      );
    }
    throw error;
  } finally {
    if (timeoutId !== null) {
      globalThis.clearTimeout(timeoutId);
    }
  }
}

async function fetchAbsoluteJson<T>(url: string): Promise<T> {
  const controller = new AbortController();
  let timeoutId: ReturnType<typeof setTimeout> | null = null;
  const timeoutPromise = new Promise<never>((_, reject) => {
    timeoutId = globalThis.setTimeout(() => {
      controller.abort();
      reject(
        new Error(
          `Request timed out after ${Math.round(TUBE_REQUEST_TIMEOUT_MS / 1000)}s for ${url}`,
        ),
      );
    }, TUBE_REQUEST_TIMEOUT_MS);
  });
  try {
    const response = await Promise.race([
      fetch(url, { signal: controller.signal }),
      timeoutPromise,
    ]);
    if (!response.ok) {
      throw new Error(`Request failed (${response.status}) for ${url}`);
    }
    return await parseJsonWithTimeout<T>(response, url);
  } finally {
    if (timeoutId !== null) {
      globalThis.clearTimeout(timeoutId);
    }
  }
}

function parseLineStrings(lineStrings: unknown): [number, number][][] {
  if (!Array.isArray(lineStrings)) {
    return [];
  }
  const parsed: [number, number][][] = [];
  for (const item of lineStrings) {
    if (typeof item !== "string") {
      continue;
    }
    try {
      const geo = JSON.parse(item) as unknown;
      if (!Array.isArray(geo)) {
        continue;
      }
      for (const part of geo) {
        if (!Array.isArray(part)) {
          continue;
        }
        const path: [number, number][] = [];
        for (const coord of part) {
          if (!Array.isArray(coord) || coord.length < 2) {
            continue;
          }
          const lon = toNumber(coord[0], Number.NaN);
          const lat = toNumber(coord[1], Number.NaN);
          if (Number.isFinite(lon) && Number.isFinite(lat)) {
            path.push([lon, lat]);
          }
        }
        if (path.length > 1) {
          parsed.push(path);
        }
      }
    } catch {
      // Ignore malformed line strings and continue.
    }
  }
  return parsed;
}

function fallbackStationPath(routePolylines: [number, number][][]): [number, number][] {
  if (routePolylines.length === 0) {
    return [];
  }
  return routePolylines[0] ?? [];
}

function coordsAlmostEqual(a: [number, number], b: [number, number]): boolean {
  return Math.abs(a[0] - b[0]) < 1e-9 && Math.abs(a[1] - b[1]) < 1e-9;
}

function mergeBranchSequences(
  first: TubeBranchSequence,
  second: TubeBranchSequence,
): TubeBranchSequence | null {
  const firstTail = first.stationIds[first.stationIds.length - 1];
  const secondHead = second.stationIds[0];
  if (!firstTail || !secondHead || firstTail !== secondHead) {
    return null;
  }

  const stationIds = [...first.stationIds, ...second.stationIds.slice(1)];
  let path: [number, number][] = [...first.path];
  if (second.path.length > 0) {
    const secondTail = [...second.path];
    if (
      path.length > 0 &&
      secondTail.length > 0 &&
      coordsAlmostEqual(path[path.length - 1] as [number, number], secondTail[0] as [number, number])
    ) {
      secondTail.shift();
    }
    path = [...path, ...secondTail];
  }
  return { stationIds, path };
}

function stitchBranchSequences(sequences: TubeBranchSequence[]): TubeBranchSequence[] {
  const normalized = sequences.filter((sequence) => sequence.stationIds.length > 1);
  if (normalized.length <= 1) {
    return normalized;
  }

  const byStart = new Map<string, number[]>();
  const endIds = new Set<string>();
  normalized.forEach((sequence, index) => {
    const start = sequence.stationIds[0];
    const end = sequence.stationIds[sequence.stationIds.length - 1];
    if (!start || !end) {
      return;
    }
    endIds.add(end);
    const bucket = byStart.get(start) ?? [];
    bucket.push(index);
    byStart.set(start, bucket);
  });

  const startIndexes = normalized
    .map((sequence, index) => ({ sequence, index }))
    .filter(({ sequence }) => {
      const start = sequence.stationIds[0];
      return !!start && !endIds.has(start);
    })
    .map(({ index }) => index);
  const seedIndexes = startIndexes.length > 0
    ? startIndexes
    : normalized.map((_, index) => index);

  const maxRoutes = 48;
  const maxDepth = 16;
  const output: TubeBranchSequence[] = [];
  const seen = new Set<string>();

  const dfs = (
    current: TubeBranchSequence,
    usedIndexes: Set<number>,
    depth: number,
  ): void => {
    if (depth >= maxDepth || output.length >= maxRoutes) {
      const key = current.stationIds.join(">");
      if (!seen.has(key)) {
        seen.add(key);
        output.push(current);
      }
      return;
    }

    const tail = current.stationIds[current.stationIds.length - 1];
    const nextIndexes = tail ? byStart.get(tail) ?? [] : [];
    let extended = false;
    for (const nextIndex of nextIndexes) {
      if (usedIndexes.has(nextIndex)) {
        continue;
      }
      const next = normalized[nextIndex];
      const appendedStops = next.stationIds.slice(1);
      if (appendedStops.some((stopId) => current.stationIds.includes(stopId))) {
        continue;
      }
      const merged = mergeBranchSequences(current, next);
      if (!merged) {
        continue;
      }
      extended = true;
      const nextUsed = new Set(usedIndexes);
      nextUsed.add(nextIndex);
      dfs(merged, nextUsed, depth + 1);
      if (output.length >= maxRoutes) {
        return;
      }
    }

    if (!extended) {
      const key = current.stationIds.join(">");
      if (!seen.has(key)) {
        seen.add(key);
        output.push(current);
      }
    }
  };

  for (const seedIndex of seedIndexes) {
    dfs(normalized[seedIndex], new Set([seedIndex]), 1);
    if (output.length >= maxRoutes) {
      break;
    }
  }

  return output
    .sort((a, b) => b.stationIds.length - a.stationIds.length)
    .slice(0, maxRoutes);
}

export async function fetchTubeTopology(
  onProgress?: (event: TubeTopologyProgressEvent) => void,
): Promise<{
  lines: TubeLineMeta[];
  stations: TubeStation[];
}> {
  onProgress?.({
    phase: "starting",
    percent: 0.02,
    completedLines: 0,
    totalLines: 0,
  });
  let lineRecords: TubeLineApiRecord[];
  try {
    lineRecords = await fetchJson<TubeLineApiRecord[]>("/Line/Mode/tube");
  } catch {
    lineRecords = FALLBACK_TUBE_LINES.map((line) => ({
      id: line.id,
      name: line.name,
    }));
  }
  const validLines = lineRecords
    .map((line) => ({
      lineId: String(line.id ?? "").trim(),
      lineName: String(line.name ?? "").trim(),
      source: line,
    }))
    .filter((line) => line.lineId.length > 0 && line.lineName.length > 0);
  const lineIdSet = new Set(validLines.map((line) => line.lineId));
  const totalLines = validLines.length;
  onProgress?.({
    phase: "lines",
    percent: 0.12,
    completedLines: 0,
    totalLines,
  });

  let completedLines = 0;
  const routeResponses = await Promise.all(
    validLines.map(async (line) => {
      try {
        const response = await fetchJson<TubeRouteSequenceApiRecord>(
          `/Line/${encodeURIComponent(line.lineId)}/Route/Sequence/all`,
        );
        return { line, response };
      } catch {
        return { line, response: null };
      } finally {
        completedLines += 1;
        const routeProgress = totalLines > 0 ? completedLines / totalLines : 1;
        onProgress?.({
          phase: "routes",
          percent: 0.12 + routeProgress * 0.7,
          completedLines,
          totalLines,
        });
      }
    }),
  );

  const lines: TubeLineMeta[] = routeResponses.map(({ line, response }) => {
    const routePolylines = parseLineStrings(response?.lineStrings);
    const orderedStations: Record<TubeDirection, string[]> = {
      inbound: [],
      outbound: [],
    };
    const stationPaths: Record<TubeDirection, [number, number][]> = {
      inbound: [],
      outbound: [],
    };
    const branchSequences: Record<TubeDirection, TubeBranchSequence[]> = {
      inbound: [],
      outbound: [],
    };
    const seenBranchKeys: Record<TubeDirection, Set<string>> = {
      inbound: new Set<string>(),
      outbound: new Set<string>(),
    };
    if (Array.isArray(response?.stopPointSequences)) {
      for (const sequence of response.stopPointSequences) {
        const direction = normalizeDirection(sequence.direction);
        if (!direction || !Array.isArray(sequence.stopPoint)) {
          continue;
        }
        const ids: string[] = [];
        const coords: [number, number][] = [];
        for (const stop of sequence.stopPoint) {
          const stopId = String(stop.id ?? stop.stationId ?? "").trim();
          if (!stopId) {
            continue;
          }
          ids.push(stopId);
          const lon = toNumber(stop.lon, Number.NaN);
          const lat = toNumber(stop.lat, Number.NaN);
          if (Number.isFinite(lon) && Number.isFinite(lat)) {
            coords.push([lon, lat]);
          }
        }
        if (ids.length > 1) {
          const branchKey = ids.join(">");
          if (!seenBranchKeys[direction].has(branchKey)) {
            seenBranchKeys[direction].add(branchKey);
            branchSequences[direction].push({
              stationIds: ids,
              path: coords.length > 1 ? coords : [],
            });
          }
          const existing = orderedStations[direction];
          if (ids.length > existing.length) {
            orderedStations[direction] = ids;
          }
        }
        if (coords.length > 1) {
          const existingPath = stationPaths[direction];
          if (coords.length > existingPath.length) {
            stationPaths[direction] = coords;
          }
        }
      }
    }

    if (stationPaths.inbound.length < 2) {
      stationPaths.inbound = fallbackStationPath(routePolylines);
    }
    if (stationPaths.outbound.length < 2) {
      stationPaths.outbound = [...stationPaths.inbound].reverse();
    }
    branchSequences.inbound = stitchBranchSequences(branchSequences.inbound);
    branchSequences.outbound = stitchBranchSequences(branchSequences.outbound);
    if (branchSequences.inbound.length === 0 && stationPaths.inbound.length > 1) {
      branchSequences.inbound.push({
        stationIds: orderedStations.inbound,
        path: stationPaths.inbound,
      });
    }
    if (branchSequences.outbound.length === 0 && stationPaths.outbound.length > 1) {
      branchSequences.outbound.push({
        stationIds: orderedStations.outbound,
        path: stationPaths.outbound,
      });
    }
    branchSequences.inbound = branchSequences.inbound.map((sequence) => ({
      stationIds: sequence.stationIds,
      path: sequence.path.length > 1 ? sequence.path : stationPaths.inbound,
    }));
    branchSequences.outbound = branchSequences.outbound.map((sequence) => ({
      stationIds: sequence.stationIds,
      path: sequence.path.length > 1 ? sequence.path : stationPaths.outbound,
    }));
    const outboundSegments = Math.max(0, stationPaths.outbound.length - 1);
    const inboundSegments = Math.max(0, stationPaths.inbound.length - 1);

    return {
      lineId: line.lineId,
      lineName: line.lineName,
      color: lineColor(line.lineId, line.source),
      routePolylines,
      orderedStations,
      stationPaths,
      branchSequences,
      segmentDurationsSec: {
        inbound: Array.from({ length: inboundSegments }, () => 120),
        outbound: Array.from({ length: outboundSegments }, () => 120),
      },
    } satisfies TubeLineMeta;
  });

  onProgress?.({
    phase: "stations",
    percent: 0.88,
    completedLines,
    totalLines,
  });
  const stationResponse = await fetchJson<{ stopPoints?: TubeStationApiRecord[] }>(
    "/StopPoint/Mode/tube",
  );
  const stationsById = new Map<string, TubeStation>();
  for (const item of stationResponse.stopPoints ?? []) {
    const id = String(item.stationNaptan ?? item.id ?? "").trim();
    if (!id) {
      continue;
    }
    const lat = toNumber(item.lat, Number.NaN);
    const lon = toNumber(item.lon, Number.NaN);
    if (!Number.isFinite(lat) || !Number.isFinite(lon)) {
      continue;
    }
    const linesForStation = (item.lines ?? [])
      .map((line) => String(line.id ?? "").trim())
      .filter((lineId) => lineIdSet.has(lineId));
    const mergedLineSet = new Set([
      ...(stationsById.get(id)?.lines ?? []),
      ...linesForStation,
    ]);
    stationsById.set(id, {
      id,
      name: cleanStationName(String(item.commonName ?? "Unknown")),
      lat,
      lon,
      lines: Array.from(mergedLineSet),
      zone: typeof item.zone === "string" ? item.zone : null,
      isInterchange: mergedLineSet.size > 1,
    });
  }

  onProgress?.({
    phase: "finalizing",
    percent: 0.98,
    completedLines,
    totalLines,
  });

  const topology = {
    lines,
    stations: Array.from(stationsById.values()),
  };

  onProgress?.({
    phase: "done",
    percent: 1,
    completedLines,
    totalLines,
  });
  return topology;
}

export async function fetchTubeLineStatuses(): Promise<TubeLineStatus[]> {
  const data = await fetchJson<UnknownRecord[]>("/Line/Mode/tube/Status");
  const statuses: TubeLineStatus[] = [];
  for (const line of data) {
    const lineId = String(line.id ?? "").trim();
    if (!lineId) {
      continue;
    }
    const lineStatuses = Array.isArray(line.lineStatuses)
      ? (line.lineStatuses as UnknownRecord[])
      : [];
    let worstSeverity = 10;
    let worstSeverityLabel = "Good Service";
    let worstReason: string | null = null;
    for (const status of lineStatuses) {
      const severity = toNumber(status.statusSeverity, 10);
      if (severity < worstSeverity) {
        worstSeverity = severity;
        worstSeverityLabel = String(status.statusSeverityDescription ?? "Unknown");
        worstReason =
          typeof status.reason === "string" && status.reason.trim().length > 0
            ? status.reason.trim()
            : null;
      }
    }
    const disruptions = Array.isArray(line.disruptions) ? line.disruptions : [];
    statuses.push({
      lineId,
      severity: worstSeverity,
      severityLabel: worstSeverityLabel,
      reason: worstReason,
      hasDisruption: disruptions.length > 0 || worstSeverity < 10,
    });
  }
  return statuses;
}

export async function fetchTubeArrivals(): Promise<TubeArrival[]> {
  const data = await fetchJson<UnknownRecord[]>("/Mode/tube/Arrivals");
  const arrivals: TubeArrival[] = [];
  for (const item of data) {
    const lineId = String(item.lineId ?? "").trim();
    const naptanId = String(item.naptanId ?? "").trim();
    const expectedArrival = String(item.expectedArrival ?? "").trim();
    const timestamp = String(item.timestamp ?? "").trim();
    const timeToStation = toNumber(item.timeToStation, Number.NaN);
    if (
      lineId.length === 0 ||
      naptanId.length === 0 ||
      expectedArrival.length === 0 ||
      timestamp.length === 0 ||
      !Number.isFinite(timeToStation)
    ) {
      continue;
    }
    const vehicleIdRaw = String(item.vehicleId ?? "").trim();
    const fallbackId = String(item.id ?? "").trim();
    const vehicleId =
      !isPlaceholderVehicleId(vehicleIdRaw) && vehicleIdRaw.length > 0
        ? vehicleIdRaw
        : fallbackId.length > 0
          ? fallbackId
          : `${lineId}:${naptanId}:${expectedArrival}`;
    arrivals.push({
      vehicleId,
      lineId,
      naptanId,
      destinationNaptanId:
        typeof item.destinationNaptanId === "string" && item.destinationNaptanId.trim().length > 0
          ? item.destinationNaptanId.trim()
          : null,
      expectedArrival,
      timeToStation,
      timestamp,
      towards: typeof item.towards === "string" && item.towards.trim().length > 0
        ? item.towards.trim()
        : null,
    });
  }
  return arrivals;
}

function parseLiveActionStep(value: unknown): TubeLiveActionStep | null {
  if (!value || typeof value !== "object") {
    return null;
  }
  const record = value as UnknownRecord;
  const fromStopId = String(record.from_stop_id ?? "").trim();
  const toStopId = String(record.to_stop_id ?? "").trim();
  const fromTimestamp = toNumber(record.from_timestamp, Number.NaN);
  const toTimestamp = toNumber(record.to_timestamp, Number.NaN);
  if (
    fromStopId.length === 0 ||
    toStopId.length === 0 ||
    !Number.isFinite(fromTimestamp) ||
    !Number.isFinite(toTimestamp)
  ) {
    return null;
  }
  return {
    fromStopId,
    toStopId,
    fromTimestamp,
    toTimestamp,
  };
}

export async function fetchTubeLiveActions(): Promise<TubeLiveActionsByLine> {
  const payload = await fetchAbsoluteJson<UnknownRecord>(LUL_ACTIONS_URL);
  const actions = payload.actions as UnknownRecord | undefined;
  if (!actions || typeof actions !== "object") {
    return {};
  }
  const parsed: TubeLiveActionsByLine = {};
  for (const [lineIdRaw, lineValue] of Object.entries(actions)) {
    const lineId = String(lineIdRaw ?? "").trim().toLowerCase();
    if (lineId.length === 0 || !lineValue || typeof lineValue !== "object") {
      continue;
    }
    const trains = lineValue as UnknownRecord;
    const parsedTrains: Record<string, TubeLiveActionStep[]> = {};
    for (const [trainIdRaw, trainActions] of Object.entries(trains)) {
      const trainId = String(trainIdRaw ?? "").trim();
      if (!Array.isArray(trainActions) || trainId.length === 0) {
        continue;
      }
      const steps = trainActions
        .map(parseLiveActionStep)
        .filter((step): step is TubeLiveActionStep => step !== null);
      if (steps.length > 0) {
        parsedTrains[trainId] = steps;
      }
    }
    if (Object.keys(parsedTrains).length > 0) {
      parsed[lineId] = parsedTrains;
    }
  }
  return parsed;
}

function parseKnownJourney(value: unknown): TubeTimetableJourney | null {
  if (!value || typeof value !== "object") {
    return null;
  }
  const row = value as UnknownRecord;
  const hour = toNumber(row.hour, Number.NaN);
  const minute = toNumber(row.minute, Number.NaN);
  const intervalId = toNumber(row.intervalId, 0);
  if (!Number.isFinite(hour) || !Number.isFinite(minute)) {
    return null;
  }
  return {
    hour: Math.max(0, Math.min(23, Math.trunc(hour))),
    minute: Math.max(0, Math.min(59, Math.trunc(minute))),
    intervalId: Math.max(0, Math.trunc(intervalId)),
  };
}

function parseIntervalSeconds(value: unknown): number[] {
  if (!Array.isArray(value)) {
    return [];
  }
  const out: number[] = [];
  for (const interval of value) {
    if (!interval || typeof interval !== "object") {
      continue;
    }
    const intervalRecord = interval as UnknownRecord;
    const tta = toNumber(intervalRecord.timeToArrival, Number.NaN);
    if (Number.isFinite(tta) && tta > 0) {
      out.push(tta);
    }
  }
  return out;
}

export async function fetchTubeTimetable(
  lineId: string,
  fromStopPointId: string,
): Promise<TubeTimetableResponse | null> {
  const path = `/Line/${encodeURIComponent(lineId)}/Timetable/${encodeURIComponent(fromStopPointId)}`;
  const response = await fetchWithTimeout(path);
  if (response.status === 404) {
    return null;
  }
  if (!response.ok) {
    throw new Error(`TfL request failed (${response.status}) for ${path}`);
  }
  const data = await parseJsonWithTimeout<UnknownRecord>(response, path);
  const timetable = data.timetable as UnknownRecord | undefined;
  const routesRaw = Array.isArray(timetable?.routes)
    ? (timetable?.routes as UnknownRecord[])
    : [];
  const parsedRoutes: TubeTimetableRoute[] = [];
  for (const route of routesRaw) {
    const stationIntervalsRaw = Array.isArray(route.stationIntervals)
      ? (route.stationIntervals as UnknownRecord[])
      : [];
    const stationIntervals = stationIntervalsRaw.map((stationInterval) => ({
      id: String(stationInterval.id ?? ""),
      intervalsSec: parseIntervalSeconds(stationInterval.intervals),
    }));
    const schedules = Array.isArray(route.schedules) ? (route.schedules as UnknownRecord[]) : [];
    const knownJourneys: TubeTimetableJourney[] = [];
    for (const schedule of schedules) {
      const scheduleJourneys = Array.isArray(schedule.knownJourneys)
        ? schedule.knownJourneys
        : [];
      for (const journey of scheduleJourneys) {
        const parsed = parseKnownJourney(journey);
        if (parsed) {
          knownJourneys.push(parsed);
        }
      }
    }
    if (stationIntervals.length > 0 && knownJourneys.length > 0) {
      parsedRoutes.push({ stationIntervals, knownJourneys });
    }
  }
  if (parsedRoutes.length === 0) {
    return null;
  }
  return {
    lineId: String(data.lineId ?? lineId),
    departureStopId: String(timetable?.departureStopId ?? fromStopPointId),
    routes: parsedRoutes,
  };
}
