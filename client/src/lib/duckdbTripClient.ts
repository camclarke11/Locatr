import * as duckdb from "@duckdb/duckdb-wasm";

import type { EncodedTrip, TimeBounds } from "../types";

const BUNDLES: duckdb.DuckDBBundles = {
  mvp: {
    mainModule: new URL(
      "@duckdb/duckdb-wasm/dist/duckdb-mvp.wasm",
      import.meta.url,
    ).toString(),
    mainWorker: new URL(
      "@duckdb/duckdb-wasm/dist/duckdb-browser-mvp.worker.js",
      import.meta.url,
    ).toString(),
  },
  eh: {
    mainModule: new URL(
      "@duckdb/duckdb-wasm/dist/duckdb-eh.wasm",
      import.meta.url,
    ).toString(),
    mainWorker: new URL(
      "@duckdb/duckdb-wasm/dist/duckdb-browser-eh.worker.js",
      import.meta.url,
    ).toString(),
  },
};

const DEFAULT_MAX_ROWS = 100_000;
const DAY_MS = 24 * 60 * 60 * 1000;

function sqlEscape(value: string): string {
  return value.replaceAll("'", "''");
}

type ParquetManifest = {
  parquet_files?: unknown;
};

export type DuckDBInitProgress = {
  phase: "manifest" | "validating" | "creating-view" | "ready";
  totalFiles: number;
  checkedFiles: number;
  usableFiles: number;
  skippedFiles: number;
  currentFile?: string;
  lastSkippedFile?: string;
  message: string;
};

function buildParquetSqlSource(urls: string[]): string {
  const escaped = urls.map((url) => `'${sqlEscape(url)}'`);
  if (escaped.length === 1) {
    return escaped[0];
  }
  return `[${escaped.join(", ")}]`;
}

function buildSessionToken(): string {
  return `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 10)}`;
}

async function resolveParquetUrls(parquetGlobUrl: string, sessionToken: string): Promise<string[]> {
  if (!parquetGlobUrl.includes("*")) {
    const directUrl = new URL(parquetGlobUrl);
    directUrl.searchParams.set("_cb", sessionToken);
    return [directUrl.toString()];
  }

  const prefix = parquetGlobUrl.slice(0, parquetGlobUrl.indexOf("*"));
  const manifestUrl = new URL("manifest.json", prefix).toString();
  const response = await fetch(manifestUrl, { cache: "no-store" });
  if (!response.ok) {
    throw new Error(
      `Could not load ${manifestUrl}. For wildcard Parquet URLs, ensure manifest.json exists.`,
    );
  }

  const manifest = (await response.json()) as ParquetManifest;
  const parquetFiles = Array.isArray(manifest.parquet_files)
    ? manifest.parquet_files.filter(
        (value): value is string => typeof value === "string" && value.endsWith(".parquet"),
      )
    : [];

  if (parquetFiles.length === 0) {
    throw new Error(`No parquet_files entries found in ${manifestUrl}.`);
  }

  return parquetFiles.map((filename) => {
    const url = new URL(filename, prefix);
    // Per-init token avoids stale/truncated cached partial responses.
    url.searchParams.set("_cb", sessionToken);
    return url.toString();
  });
}

function parseMs(value: unknown): number {
  if (typeof value === "bigint") {
    return Number(value);
  }
  if (typeof value === "number") {
    return value;
  }
  if (typeof value === "string") {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : Number.NaN;
  }
  return Number.NaN;
}

function parseNumber(value: unknown): number {
  if (typeof value === "number") {
    return value;
  }
  if (typeof value === "bigint") {
    return Number(value);
  }
  if (typeof value === "string") {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : Number.NaN;
  }
  return Number.NaN;
}

function extractFailingParquetUrl(errorMessage: string): string | null {
  const match = errorMessage.match(/File '([^']+\.parquet(?:\?[^']*)?)'/i);
  return match?.[1] ?? null;
}

function normalizeUrlForCompare(value: string): string {
  try {
    const parsed = new URL(value);
    return `${parsed.origin}${parsed.pathname}`;
  } catch {
    return value.split("?")[0] ?? value;
  }
}

function fileNameFromUrl(value: string): string {
  try {
    const parsed = new URL(value);
    const pathParts = parsed.pathname.split("/");
    return pathParts[pathParts.length - 1] ?? value;
  } catch {
    const pathParts = value.split(/[\\/]/);
    return pathParts[pathParts.length - 1] ?? value;
  }
}

function findUrlIndex(urls: string[], failingUrl: string): number {
  const exact = urls.indexOf(failingUrl);
  if (exact >= 0) {
    return exact;
  }
  const normalizedFailingUrl = normalizeUrlForCompare(failingUrl);
  const normalizedMatch = urls.findIndex((url) => normalizeUrlForCompare(url) === normalizedFailingUrl);
  if (normalizedMatch >= 0) {
    return normalizedMatch;
  }
  const failingName = fileNameFromUrl(failingUrl);
  if (!failingName) {
    return -1;
  }
  return urls.findIndex((url) => fileNameFromUrl(url) === failingName);
}

function bytesToText(bytes: Uint8Array): string {
  return new TextDecoder("utf-8").decode(bytes);
}

function isRemoteHttpUrl(value: string): boolean {
  try {
    const parsed = new URL(value);
    return parsed.protocol === "http:" || parsed.protocol === "https:";
  } catch {
    return false;
  }
}

function hasParquetMagicBytes(bytes: Uint8Array): boolean {
  if (bytes.length < 12) {
    return false;
  }
  const head = bytesToText(bytes.slice(0, 4));
  const tail = bytesToText(bytes.slice(bytes.length - 4));
  return head === "PAR1" && tail === "PAR1";
}

function extractDayMsFromUrl(url: string): number | null {
  const name = fileNameFromUrl(url);
  const match = name.match(/^(\d{4}-\d{2}-\d{2})\.parquet$/);
  if (!match?.[1]) {
    return null;
  }
  const parsed = Date.parse(`${match[1]}T00:00:00.000Z`);
  return Number.isFinite(parsed) ? parsed : null;
}

async function fetchRangeBytes(url: string, start: number, end: number): Promise<Uint8Array> {
  const response = await fetch(url, {
    cache: "no-store",
    headers: {
      Range: `bytes=${start}-${end}`,
    },
  });
  if (!response.ok) {
    throw new Error(`Range request failed (${response.status}) for ${url}`);
  }
  return new Uint8Array(await response.arrayBuffer());
}

async function isHealthyParquetUrl(url: string): Promise<boolean> {
  for (let attempt = 0; attempt < 2; attempt += 1) {
    try {
      const headResponse = await fetch(url, { method: "HEAD", cache: "no-store" });
      if (!headResponse.ok) {
        return false;
      }
      const contentLengthHeader = headResponse.headers.get("content-length");
      const contentLength = contentLengthHeader ? Number(contentLengthHeader) : Number.NaN;
      if (!Number.isFinite(contentLength) || contentLength < 12) {
        return false;
      }

      const startBytes = await fetchRangeBytes(url, 0, 3);
      const endBytes = await fetchRangeBytes(url, contentLength - 4, contentLength - 1);
      const startMagic = bytesToText(startBytes.slice(0, 4));
      const endMagic = bytesToText(endBytes.slice(Math.max(0, endBytes.length - 4)));
      if (startMagic === "PAR1" && endMagic === "PAR1") {
        return true;
      }
    } catch {
      // Retry once to smooth transient local dev-server range glitches.
    }
  }
  return false;
}

async function validateParquetUrls(
  urls: string[],
  onProgress?: (progress: DuckDBInitProgress) => void,
): Promise<{ validUrls: string[]; skippedUrls: string[] }> {
  if (urls.length === 0) {
    return { validUrls: [], skippedUrls: [] };
  }

  const healthyByIndex = new Array<boolean>(urls.length).fill(false);
  const concurrency = Math.min(6, Math.max(2, Math.floor(urls.length / 80)));
  let cursor = 0;
  let checked = 0;
  let skipped = 0;
  let lastSkippedFile: string | undefined;

  const workers = Array.from({ length: Math.min(concurrency, urls.length) }, async () => {
    while (true) {
      const index = cursor;
      cursor += 1;
      if (index >= urls.length) {
        return;
      }
      const url = urls[index];
      const healthy = await isHealthyParquetUrl(url);
      healthyByIndex[index] = healthy;
      checked += 1;
      if (!healthy) {
        skipped += 1;
        lastSkippedFile = fileNameFromUrl(url);
      }
      const shouldNotify =
        checked === 1 || checked === urls.length || checked % 10 === 0 || !healthy;
      if (shouldNotify && onProgress) {
        onProgress({
          phase: "validating",
          totalFiles: urls.length,
          checkedFiles: checked,
          usableFiles: checked - skipped,
          skippedFiles: skipped,
          currentFile: fileNameFromUrl(url),
          lastSkippedFile,
          message: `Validating parquet files ${checked}/${urls.length} (skipped ${skipped})...`,
        });
      }
    }
  });

  await Promise.all(workers);
  return {
    validUrls: urls.filter((_, index) => healthyByIndex[index]),
    skippedUrls: urls.filter((_, index) => !healthyByIndex[index]),
  };
}

type ParquetFile = {
  key: string;
  url: string;
  dayMs: number | null;
  label: string;
};

type QuerySource = {
  key: string;
  path: string;
  remoteUrl: string | null;
  label: string;
};

export class DuckDBTripClient {
  private db: duckdb.AsyncDuckDB | null = null;

  private conn: duckdb.AsyncDuckDBConnection | null = null;

  private ready = false;

  private parquetGlob = "";

  private timeBounds: TimeBounds | null = null;

  private parquetFiles: ParquetFile[] = [];

  private unusableFileKeys = new Set<string>();

  private materializedPathByKey = new Map<string, string>();

  private async queryBoundFromSource(
    sourcePath: string,
    bound: "min" | "max",
  ): Promise<number> {
    if (!this.conn) {
      throw new Error("DuckDB client is not initialized.");
    }

    const aggregate = bound === "min" ? "min(start_time)" : "max(end_time)";
    const result = await this.conn.query(`
      SELECT epoch_ms(${aggregate}) AS value_ms
      FROM read_parquet(${buildParquetSqlSource([sourcePath])}, union_by_name=true);
    `);
    const [row] = result.toArray() as Array<{ value_ms: unknown }>;
    const value = parseMs(row?.value_ms);
    if (!Number.isFinite(value)) {
      throw new Error(`Could not determine ${bound} bound from ${sourcePath}.`);
    }
    return value;
  }

  private async materializeParquetUrl(url: string, key: string): Promise<string> {
    if (!this.db) {
      throw new Error("DuckDB client is not initialized.");
    }
    const existing = this.materializedPathByKey.get(key);
    if (existing) {
      return existing;
    }

    const baseName = fileNameFromUrl(url).replace(/[^a-zA-Z0-9._-]/g, "_");
    const stem = baseName.endsWith(".parquet") ? baseName.slice(0, -8) : baseName;
    const virtualPath = `buffered/${stem}-${Math.random().toString(36).slice(2, 10)}.parquet`;

    for (let attempt = 0; attempt < 2; attempt += 1) {
      try {
        const response = await fetch(url, { cache: "no-store" });
        if (!response.ok) {
          throw new Error(`Fetch failed (${response.status}) for ${url}`);
        }
        const bytes = new Uint8Array(await response.arrayBuffer());
        if (!hasParquetMagicBytes(bytes)) {
          throw new Error(`Fetched payload is not a valid parquet file: ${url}`);
        }
        await this.db.registerFileBuffer(virtualPath, bytes);
        this.materializedPathByKey.set(key, virtualPath);
        return virtualPath;
      } catch {
        if (attempt === 1) {
          throw new Error(`Could not materialize parquet file: ${url}`);
        }
      }
    }
    throw new Error(`Could not materialize parquet file: ${url}`);
  }

  private async queryBoundFromFile(file: ParquetFile, bound: "min" | "max"): Promise<number | null> {
    if (this.unusableFileKeys.has(file.key)) {
      return null;
    }

    let sourcePath = this.materializedPathByKey.get(file.key) ?? file.url;
    for (let attempt = 0; attempt < 2; attempt += 1) {
      try {
        return await this.queryBoundFromSource(sourcePath, bound);
      } catch {
        if (attempt === 0 && sourcePath === file.url && isRemoteHttpUrl(file.url)) {
          try {
            sourcePath = await this.materializeParquetUrl(file.url, file.key);
            continue;
          } catch {
            // Fall through and mark as unusable.
          }
        }
        break;
      }
    }

    this.unusableFileKeys.add(file.key);
    return null;
  }

  private async determineTimeBounds(): Promise<TimeBounds> {
    const ordered = [...this.parquetFiles].sort((a, b) => {
      if (a.dayMs === null && b.dayMs === null) {
        return 0;
      }
      if (a.dayMs === null) {
        return 1;
      }
      if (b.dayMs === null) {
        return -1;
      }
      return a.dayMs - b.dayMs;
    });

    let minMs: number | null = null;
    for (const file of ordered) {
      const value = await this.queryBoundFromFile(file, "min");
      if (Number.isFinite(value)) {
        minMs = value;
        break;
      }
    }

    let maxMs: number | null = null;
    for (let index = ordered.length - 1; index >= 0; index -= 1) {
      const value = await this.queryBoundFromFile(ordered[index], "max");
      if (Number.isFinite(value)) {
        maxMs = value;
        break;
      }
    }

    if (!Number.isFinite(minMs) || !Number.isFinite(maxMs) || (minMs as number) > (maxMs as number)) {
      throw new Error("Could not determine dataset time bounds from readable parquet files.");
    }

    return { minMs: minMs as number, maxMs: maxMs as number };
  }

  private buildWindowSources(startMs: number, endMs: number): QuerySource[] {
    const startDayMs = Math.floor(startMs / DAY_MS) * DAY_MS;
    const endDayMs = Math.floor(endMs / DAY_MS) * DAY_MS;
    const includeFrom = startDayMs - DAY_MS;

    const seen = new Set<string>();
    const sources: QuerySource[] = [];
    for (const file of this.parquetFiles) {
      if (this.unusableFileKeys.has(file.key)) {
        continue;
      }
      if (file.dayMs !== null && (file.dayMs < includeFrom || file.dayMs > endDayMs)) {
        continue;
      }
      if (seen.has(file.key)) {
        continue;
      }
      seen.add(file.key);
      const materializedPath = this.materializedPathByKey.get(file.key);
      sources.push({
        key: file.key,
        path: materializedPath ?? file.url,
        remoteUrl: file.url,
        label: file.label,
      });
    }
    return sources;
  }

  async init(
    parquetGlobUrl: string,
    onProgress?: (progress: DuckDBInitProgress) => void,
  ): Promise<TimeBounds> {
    if (this.ready && parquetGlobUrl === this.parquetGlob && this.conn) {
      return this.getTimeBounds();
    }

    if (!this.db) {
      const logger = new duckdb.ConsoleLogger(duckdb.LogLevel.WARNING);
      const bundle = await duckdb.selectBundle(BUNDLES);
      if (!bundle.mainWorker) {
        throw new Error("DuckDB bundle does not contain a worker script.");
      }
      const worker = new Worker(bundle.mainWorker);
      this.db = new duckdb.AsyncDuckDB(logger, worker);
      await this.db.instantiate(bundle.mainModule, bundle.pthreadWorker);
    }

    if (this.conn) {
      await this.conn.close();
    }
    this.conn = await this.db.connect();
    await this.conn.query("PRAGMA enable_object_cache=false;");

    this.ready = false;
    this.parquetGlob = parquetGlobUrl;
    this.timeBounds = null;
    this.parquetFiles = [];
    this.unusableFileKeys.clear();
    this.materializedPathByKey.clear();
    const initToken = buildSessionToken();

    const parquetUrls = await resolveParquetUrls(parquetGlobUrl, initToken);
    onProgress?.({
      phase: "manifest",
      totalFiles: parquetUrls.length,
      checkedFiles: 0,
      usableFiles: parquetUrls.length,
      skippedFiles: 0,
      message: `Resolved ${parquetUrls.length} parquet files from manifest.`,
    });

    const validation = await validateParquetUrls(parquetUrls, onProgress);
    if (validation.validUrls.length === 0) {
      throw new Error("No readable parquet files were found after validation.");
    }

    this.parquetFiles = validation.validUrls.map((url) => ({
      key: normalizeUrlForCompare(url),
      url,
      dayMs: extractDayMsFromUrl(url),
      label: fileNameFromUrl(url),
    }));

    onProgress?.({
      phase: "creating-view",
      totalFiles: parquetUrls.length,
      checkedFiles: parquetUrls.length,
      usableFiles: this.parquetFiles.length,
      skippedFiles: validation.skippedUrls.length,
      message: "Computing dataset time bounds from readable parquet files...",
    });
    const bounds = await this.determineTimeBounds();

    const dynamicSkips = this.parquetFiles.filter((file) =>
      this.unusableFileKeys.has(file.key),
    ).length;
    const usableFiles = this.parquetFiles.length - dynamicSkips;
    const skippedFiles = validation.skippedUrls.length + dynamicSkips;
    if (usableFiles <= 0) {
      throw new Error("No usable parquet files remained after initialization.");
    }

    onProgress?.({
      phase: "ready",
      totalFiles: parquetUrls.length,
      checkedFiles: parquetUrls.length,
      usableFiles,
      skippedFiles,
      message:
        skippedFiles > 0
          ? `Playback ready. Using ${usableFiles} files (skipped ${skippedFiles}).`
          : `Playback ready. Using ${usableFiles} files.`,
    });
    this.timeBounds = bounds;
    this.ready = true;
    return bounds;
  }

  async getTimeBounds(): Promise<TimeBounds> {
    if (!this.timeBounds) {
      throw new Error("DuckDB client is not initialized.");
    }
    return this.timeBounds;
  }

  async fetchWindow(
    startMs: number,
    endMs: number,
    maxRows = DEFAULT_MAX_ROWS,
  ): Promise<EncodedTrip[]> {
    if (!this.conn) {
      throw new Error("DuckDB client is not initialized.");
    }

    const initialSources = this.buildWindowSources(startMs, endMs);
    if (initialSources.length === 0) {
      return [];
    }

    const startIso = new Date(startMs).toISOString();
    const endIso = new Date(endMs).toISOString();
    const safeMaxRows = Math.max(1, Math.floor(maxRows));

    const remainingSources = [...initialSources];
    while (remainingSources.length > 0) {
      try {
        const parquetSourceSql = buildParquetSqlSource(
          remainingSources.map((source) => source.path),
        );
        const result = await this.conn.query(`
          SELECT
            trip_id,
            epoch_ms(start_time) AS start_time_ms,
            epoch_ms(end_time) AS end_time_ms,
            route_geometry,
            coalesce(route_source, 'unknown') AS route_source,
            coalesce(route_distance_m, 0.0) AS route_distance_m,
            coalesce(route_duration_s, 0.0) AS route_duration_s
          FROM read_parquet(${parquetSourceSql}, union_by_name=true)
          WHERE start_time < TIMESTAMPTZ '${sqlEscape(endIso)}'
            AND end_time >= TIMESTAMPTZ '${sqlEscape(startIso)}'
          ORDER BY start_time
          LIMIT ${safeMaxRows};
        `);

        return (result.toArray() as Array<Record<string, unknown>>)
          .map((row) => ({
            trip_id: String(row.trip_id ?? ""),
            start_time_ms: parseMs(row.start_time_ms),
            end_time_ms: parseMs(row.end_time_ms),
            route_geometry: String(row.route_geometry ?? ""),
            route_source: String(row.route_source ?? "unknown"),
            route_distance_m: parseNumber(row.route_distance_m),
            route_duration_s: parseNumber(row.route_duration_s),
          }))
          .filter(
            (trip) =>
              trip.trip_id.length > 0 &&
              Number.isFinite(trip.start_time_ms) &&
              Number.isFinite(trip.end_time_ms) &&
              trip.route_geometry.length > 0,
          );
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        const failingPath = extractFailingParquetUrl(message);
        if (!failingPath) {
          throw error;
        }

        const failingIndex = findUrlIndex(
          remainingSources.map((source) => source.path),
          failingPath,
        );
        if (failingIndex < 0) {
          throw error;
        }

        const candidate = remainingSources[failingIndex];
        if (
          candidate.remoteUrl &&
          candidate.path === candidate.remoteUrl &&
          isRemoteHttpUrl(candidate.remoteUrl)
        ) {
          try {
            candidate.path = await this.materializeParquetUrl(
              candidate.remoteUrl,
              candidate.key,
            );
            continue;
          } catch {
            // If buffering fails, mark this file unusable below.
          }
        }

        this.unusableFileKeys.add(candidate.key);
        remainingSources.splice(failingIndex, 1);
      }
    }

    return [];
  }
}
