export type BikePointStation = {
  id: string;
  name: string;
  lat: number;
  lon: number;
  availableBikes: number;
  availableEBikes: number;
  availableDocks: number;
  totalDocks: number;
  lastUpdated: string | null;
};

type BikePointProperty = {
  key?: string;
  value?: string;
};

type BikePointApiRecord = {
  id?: string;
  commonName?: string;
  lat?: number;
  lon?: number;
  additionalProperties?: BikePointProperty[];
};

const BIKEPOINT_URL = "https://api.tfl.gov.uk/BikePoint";

function asNumber(input: unknown): number {
  if (typeof input === "number") {
    return Number.isFinite(input) ? input : 0;
  }
  if (typeof input === "string") {
    const parsed = Number(input);
    return Number.isFinite(parsed) ? parsed : 0;
  }
  return 0;
}

function buildPropertyMap(properties: BikePointProperty[] | undefined): Map<string, string> {
  const out = new Map<string, string>();
  if (!properties) {
    return out;
  }
  for (const property of properties) {
    const key = (property.key ?? "").trim().toLowerCase();
    if (!key) {
      continue;
    }
    out.set(key, property.value ?? "");
  }
  return out;
}

function getPropNumber(map: Map<string, string>, keys: string[]): number {
  for (const key of keys) {
    if (map.has(key)) {
      return asNumber(map.get(key));
    }
  }
  return 0;
}

export async function fetchBikePointStations(): Promise<BikePointStation[]> {
  const response = await fetch(BIKEPOINT_URL);
  if (!response.ok) {
    throw new Error(`BikePoint request failed with HTTP ${response.status}.`);
  }

  const payload = (await response.json()) as BikePointApiRecord[];
  if (!Array.isArray(payload)) {
    throw new Error("BikePoint response was not an array.");
  }

  return payload
    .map((record) => {
      const props = buildPropertyMap(record.additionalProperties);
      const availableBikes = getPropNumber(props, ["nbbikes", "availablebikes"]);
      const availableEBikes = getPropNumber(props, ["nbebikes", "availableebikes"]);
      const availableDocks = getPropNumber(props, ["nbemptydocks", "availabledocks"]);
      const totalDocks = getPropNumber(props, ["nbdocks", "totaldocks"]);

      return {
        id: String(record.id ?? ""),
        name: String(record.commonName ?? "Unknown station"),
        lat: asNumber(record.lat),
        lon: asNumber(record.lon),
        availableBikes,
        availableEBikes,
        availableDocks,
        totalDocks: totalDocks > 0 ? totalDocks : availableBikes + availableDocks,
        lastUpdated: props.get("modified") ?? props.get("lastupdated") ?? null,
      } satisfies BikePointStation;
    })
    .filter((station) => station.id.length > 0 && station.lat !== 0 && station.lon !== 0);
}
