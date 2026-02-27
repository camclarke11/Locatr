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

function sqlEscape(value: string): string {
  return value.replaceAll("'", "''");
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

export class DuckDBTripClient {
  private db: duckdb.AsyncDuckDB | null = null;

  private conn: duckdb.AsyncDuckDBConnection | null = null;

  private ready = false;

  private parquetGlob = "";

  async init(parquetGlobUrl: string): Promise<TimeBounds> {
    if (this.ready && parquetGlobUrl === this.parquetGlob && this.conn) {
      return this.getTimeBounds();
    }

    if (!this.db) {
      const logger = new duckdb.ConsoleLogger();
      const bundle = await duckdb.selectBundle(BUNDLES);
      const worker = new Worker(bundle.mainWorker);
      this.db = new duckdb.AsyncDuckDB(logger, worker);
      await this.db.instantiate(bundle.mainModule, bundle.pthreadWorker);
    }

    if (this.conn) {
      await this.conn.close();
    }
    this.conn = await this.db.connect();
    this.parquetGlob = parquetGlobUrl;

    const globEscaped = sqlEscape(parquetGlobUrl);
    await this.conn.query("PRAGMA enable_object_cache=true;");
    await this.conn.query("SET threads TO 4;");
    await this.conn.query(`
      CREATE OR REPLACE VIEW trips AS
      SELECT
        CAST(trip_id AS VARCHAR) AS trip_id,
        CAST(start_time AS TIMESTAMPTZ) AS start_time,
        CAST(end_time AS TIMESTAMPTZ) AS end_time,
        CAST(route_geometry AS VARCHAR) AS route_geometry
      FROM read_parquet('${globEscaped}');
    `);

    this.ready = true;
    return this.getTimeBounds();
  }

  async getTimeBounds(): Promise<TimeBounds> {
    if (!this.conn) {
      throw new Error("DuckDB client is not initialized.");
    }

    const result = await this.conn.query(`
      SELECT
        epoch_ms(min(start_time)) AS min_ms,
        epoch_ms(max(end_time)) AS max_ms
      FROM trips;
    `);
    const [row] = result.toArray() as Array<{ min_ms: unknown; max_ms: unknown }>;
    const minMs = parseMs(row?.min_ms);
    const maxMs = parseMs(row?.max_ms);
    if (!Number.isFinite(minMs) || !Number.isFinite(maxMs)) {
      throw new Error("Could not determine time bounds from Parquet data.");
    }
    return { minMs, maxMs };
  }

  async fetchWindow(
    startMs: number,
    endMs: number,
    maxRows = DEFAULT_MAX_ROWS,
  ): Promise<EncodedTrip[]> {
    if (!this.conn) {
      throw new Error("DuckDB client is not initialized.");
    }
    const startIso = new Date(startMs).toISOString();
    const endIso = new Date(endMs).toISOString();
    const safeMaxRows = Math.max(1, Math.floor(maxRows));

    const result = await this.conn.query(`
      SELECT
        trip_id,
        epoch_ms(start_time) AS start_time_ms,
        epoch_ms(end_time) AS end_time_ms,
        route_geometry
      FROM trips
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
      }))
      .filter(
        (trip) =>
          trip.trip_id.length > 0 &&
          Number.isFinite(trip.start_time_ms) &&
          Number.isFinite(trip.end_time_ms) &&
          trip.route_geometry.length > 0,
      );
  }
}
