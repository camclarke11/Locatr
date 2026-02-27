import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import * as chrono from "chrono-node";

import { PARQUET_GLOB_URL, WINDOW_SIZE_MS } from "../config";
import { DuckDBTripClient } from "../lib/duckdbTripClient";
import { clamp } from "../lib/time";
import type { DecodedTrip, EncodedTrip, PlaybackSpeed, TimeBounds } from "../types";

type DecoderWorkerResponse = {
  requestId: number;
  trips: DecodedTrip[];
};

const KEEP_WINDOW_DISTANCE = 3;
const WINDOW_QUERY_LIMIT = 120_000;

export function useTripPlayback() {
  const [status, setStatus] = useState<"loading" | "ready" | "error">("loading");
  const [error, setError] = useState<string | null>(null);
  const [jumpError, setJumpError] = useState<string | null>(null);
  const [bounds, setBounds] = useState<TimeBounds | null>(null);
  const [currentTimeMs, setCurrentTimeMs] = useState<number>(Date.now());
  const [isPlaying, setIsPlaying] = useState(false);
  const [speed, setSpeed] = useState<PlaybackSpeed>(10);
  const [decodedTrips, setDecodedTrips] = useState<DecodedTrip[]>([]);
  const [cacheVersion, setCacheVersion] = useState(0);

  const clientRef = useRef<DuckDBTripClient | null>(null);
  const cacheRef = useRef<Map<number, EncodedTrip[]>>(new Map());
  const inFlightRef = useRef<Set<number>>(new Set());
  const workerRef = useRef<Worker | null>(null);
  const decodeRequestRef = useRef(0);
  const latestRequestRef = useRef(0);

  const currentWindowKey = Math.floor(currentTimeMs / WINDOW_SIZE_MS);

  useEffect(() => {
    let isMounted = true;
    const init = async (): Promise<void> => {
      try {
        setStatus("loading");
        clientRef.current = new DuckDBTripClient();
        const tripBounds = await clientRef.current.init(PARQUET_GLOB_URL);
        if (!isMounted) {
          return;
        }
        setBounds(tripBounds);
        setCurrentTimeMs(tripBounds.minMs);
        setStatus("ready");
      } catch (initError) {
        if (!isMounted) {
          return;
        }
        setError(
          initError instanceof Error
            ? initError.message
            : "Failed to initialize DuckDB WASM.",
        );
        setStatus("error");
      }
    };
    void init();

    return () => {
      isMounted = false;
    };
  }, []);

  const fetchWindow = useCallback(
    async (windowKey: number) => {
      if (!clientRef.current) {
        return;
      }
      if (cacheRef.current.has(windowKey) || inFlightRef.current.has(windowKey)) {
        return;
      }

      inFlightRef.current.add(windowKey);
      const windowStart = windowKey * WINDOW_SIZE_MS;
      const windowEnd = windowStart + WINDOW_SIZE_MS;
      try {
        const rows = await clientRef.current.fetchWindow(
          windowStart,
          windowEnd,
          WINDOW_QUERY_LIMIT,
        );
        cacheRef.current.set(windowKey, rows);
        setCacheVersion((version) => version + 1);
      } catch (windowError) {
        setError(
          windowError instanceof Error
            ? windowError.message
            : "Failed to fetch trips for current window.",
        );
        setStatus("error");
      } finally {
        inFlightRef.current.delete(windowKey);
      }
    },
    [],
  );

  useEffect(() => {
    if (status !== "ready" || !bounds) {
      return;
    }

    const keysToFetch = [currentWindowKey - 1, currentWindowKey, currentWindowKey + 1];
    for (const key of keysToFetch) {
      if (key >= 0) {
        void fetchWindow(key);
      }
    }

    for (const key of cacheRef.current.keys()) {
      if (Math.abs(key - currentWindowKey) > KEEP_WINDOW_DISTANCE) {
        cacheRef.current.delete(key);
      }
    }
  }, [bounds, currentWindowKey, fetchWindow, status]);

  const encodedTrips = useMemo(() => {
    const tripsById = new Map<string, EncodedTrip>();
    const keys = [currentWindowKey - 1, currentWindowKey, currentWindowKey + 1];
    for (const key of keys) {
      const rows = cacheRef.current.get(key);
      if (!rows) {
        continue;
      }
      for (const row of rows) {
        tripsById.set(`${row.trip_id}:${row.start_time_ms}`, row);
      }
    }
    return Array.from(tripsById.values());
  }, [cacheVersion, currentWindowKey]);

  useEffect(() => {
    workerRef.current = new Worker(
      new URL("../workers/polylineDecoder.worker.ts", import.meta.url),
      { type: "module" },
    );
    const worker = workerRef.current;

    const onWorkerMessage = (event: MessageEvent<DecoderWorkerResponse>): void => {
      if (event.data.requestId !== latestRequestRef.current) {
        return;
      }
      setDecodedTrips(event.data.trips);
    };
    worker.addEventListener("message", onWorkerMessage);

    return () => {
      worker.removeEventListener("message", onWorkerMessage);
      worker.terminate();
      workerRef.current = null;
    };
  }, []);

  useEffect(() => {
    if (!workerRef.current) {
      return;
    }
    decodeRequestRef.current += 1;
    latestRequestRef.current = decodeRequestRef.current;
    workerRef.current.postMessage({
      requestId: decodeRequestRef.current,
      trips: encodedTrips,
    });
  }, [encodedTrips]);

  useEffect(() => {
    if (!isPlaying || !bounds) {
      return;
    }

    let animationFrameId = 0;
    let previousTimestamp = performance.now();

    const step = (timestamp: number): void => {
      const deltaMs = timestamp - previousTimestamp;
      previousTimestamp = timestamp;

      setCurrentTimeMs((previous) => {
        const next = previous + deltaMs * speed;
        if (next >= bounds.maxMs) {
          setIsPlaying(false);
          return bounds.maxMs;
        }
        return next;
      });
      animationFrameId = window.requestAnimationFrame(step);
    };

    animationFrameId = window.requestAnimationFrame(step);
    return () => window.cancelAnimationFrame(animationFrameId);
  }, [bounds, isPlaying, speed]);

  useEffect(() => {
    if (!bounds) {
      return;
    }

    const onKeyDown = (event: KeyboardEvent): void => {
      if (event.target instanceof HTMLInputElement) {
        return;
      }

      if (event.code === "Space") {
        event.preventDefault();
        setIsPlaying((playing) => !playing);
        return;
      }
      if (event.code === "ArrowLeft" || event.code === "ArrowRight") {
        const baseStep = event.shiftKey ? 30 * 60 * 1000 : 5 * 60 * 1000;
        const delta = event.code === "ArrowLeft" ? -baseStep : baseStep;
        setCurrentTimeMs((value) => clamp(value + delta, bounds.minMs, bounds.maxMs));
      }
    };

    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [bounds]);

  const setPlaybackTime = useCallback(
    (nextTimeMs: number) => {
      if (!bounds) {
        return;
      }
      setCurrentTimeMs(clamp(nextTimeMs, bounds.minMs, bounds.maxMs));
      setJumpError(null);
    },
    [bounds],
  );

  const jumpToNaturalLanguage = useCallback(
    (query: string) => {
      if (!bounds) {
        return;
      }
      const parsed = chrono.en.GB.parseDate(query, new Date(currentTimeMs));
      if (!parsed) {
        setJumpError("Could not parse that date/time phrase.");
        return;
      }
      setJumpError(null);
      setCurrentTimeMs(clamp(parsed.getTime(), bounds.minMs, bounds.maxMs));
    },
    [bounds, currentTimeMs],
  );

  const togglePlay = useCallback(() => {
    setIsPlaying((playing) => !playing);
  }, []);

  return {
    status,
    error,
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
  };
}
