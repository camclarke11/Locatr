#!/usr/bin/env python3
"""Run the Santander pipeline across a month range with resume support..."""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import logging
import os
import random
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

import london_bike_pipeline as bike_pipeline

LOGGER = logging.getLogger("london-bike-backfill")


def month_floor(value: datetime) -> datetime:
    return value.replace(day=1, hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)


def shift_month(value: datetime, delta: int) -> datetime:
    year = value.year
    month = value.month + delta
    while month > 12:
        year += 1
        month -= 12
    while month < 1:
        year -= 1
        month += 12
    return value.replace(year=year, month=month)


def month_string(value: datetime) -> str:
    return value.strftime("%Y-%m")


def iter_months(start_month: str, end_month: str) -> list[str]:
    current, _ = bike_pipeline.parse_month(start_month)
    end, _ = bike_pipeline.parse_month(end_month)
    if current > end:
        raise ValueError("--start-month must be <= --end-month")
    out: list[str] = []
    while current <= end:
        out.append(month_string(current))
        current = shift_month(current, 1)
    return out


def discover_latest_available_month(base_url: str, start_month: str) -> str:
    session = bike_pipeline.build_http_session()
    lower_bound, _ = bike_pipeline.parse_month(start_month)
    cursor = month_floor(datetime.now(timezone.utc))
    while cursor >= lower_bound:
        month_start = cursor
        month_end = shift_month(month_start, 1)
        try:
            urls = bike_pipeline.discover_month_urls(
                session=session,
                base_url=base_url,
                month_start=month_start,
                month_end=month_end,
            )
            if urls:
                return month_string(month_start)
        except Exception:  # noqa: BLE001
            pass
        cursor = shift_month(cursor, -1)
    raise RuntimeError("Could not discover any available month at or after --start-month.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill Santander cycle data for a month range with resume support."
    )
    parser.add_argument(
        "--start-month",
        required=True,
        help="Inclusive range start in YYYY-MM format (example: 2024-05).",
    )
    parser.add_argument(
        "--end-month",
        default="latest",
        help="Inclusive range end in YYYY-MM, or 'latest' to auto-detect from TfL.",
    )
    parser.add_argument(
        "--resume",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Skip months that already have parquet files in output-dir.",
    )
    parser.add_argument(
        "--continue-on-error",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Continue remaining months if a month fails.",
    )
    parser.add_argument(
        "--tfl-base-url",
        default="https://s3-eu-west-1.amazonaws.com/cycling.data.tfl.gov.uk/?list-type=2&prefix=usage-stats/",
        help="TfL usage stats index URL (S3 listing URL or HTML index page).",
    )
    parser.add_argument(
        "--osrm-url",
        default="https://router.project-osrm.org",
        help="Base URL for OSRM (must expose /route/v1/bicycle).",
    )
    parser.add_argument(
        "--download-dir",
        default="./pipeline/data/raw",
        help="Directory to store downloaded source archives/CSVs.",
    )
    parser.add_argument(
        "--extract-dir",
        default="./pipeline/data/extracted",
        help="Directory to extract ZIP archives.",
    )
    parser.add_argument(
        "--output-dir",
        default="./pipeline/output/parquet",
        help="Directory for daily parquet outputs.",
    )
    parser.add_argument(
        "--route-cache",
        default="./pipeline/output/route_cache.parquet",
        help="Route cache parquet shared across months.",
    )
    parser.add_argument(
        "--osrm-workers",
        type=int,
        default=8,
        help="Number of concurrent OSRM workers.",
    )
    parser.add_argument(
        "--osrm-qps",
        type=float,
        default=10.0,
        help="Global max requests per second to OSRM.",
    )
    parser.add_argument(
        "--request-timeout",
        type=int,
        default=20,
        help="HTTP request timeout in seconds.",
    )
    parser.add_argument(
        "--max-trips",
        type=int,
        default=None,
        help="Optional cap for number of trips processed per month.",
    )
    parser.add_argument(
        "--max-trips-strategy",
        choices=("uniform", "earliest"),
        default="uniform",
        help="How to apply --max-trips: 'uniform' samples across month, 'earliest' keeps oldest rows.",
    )
    parser.add_argument(
        "--max-trips-seed",
        type=int,
        default=42,
        help="Random seed used when --max-trips-strategy=uniform.",
    )
    parser.add_argument(
        "--max-new-routes",
        type=int,
        default=None,
        help="Optional cap on new OSRM lookups in each month run.",
    )
    parser.add_argument(
        "--auto-tune",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Benchmark OSRM quickly and auto-select workers/qps before backfill.",
    )
    parser.add_argument(
        "--auto-tune-requests",
        type=int,
        default=120,
        help="Number of route requests to use in auto-tune benchmark.",
    )
    parser.add_argument(
        "--auto-tune-only",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Run the OSRM auto-tuner and exit without processing months.",
    )
    parser.add_argument(
        "--pause-file",
        default="./pipeline/output/.backfill_pause",
        help="If this file exists, stop cleanly after current month (or skip).",
    )
    parser.add_argument(
        "--state-file",
        default="./pipeline/output/backfill_state.json",
        help="Write structured backfill progress state to this JSON file.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging.",
    )
    return parser.parse_args()


def month_has_output(month: str, output_dir: Path) -> bool:
    return any(output_dir.glob(f"{month}-*.parquet"))


def load_probe_pairs(route_cache_path: Path, limit: int) -> list[tuple[float, float, float, float]]:
    column_order = ["start_lon", "start_lat", "end_lon", "end_lat"]
    if route_cache_path.exists():
        frame = pd.read_parquet(route_cache_path, columns=column_order)
        if set(column_order).issubset(frame.columns):
            sample = frame[column_order].drop_duplicates(ignore_index=True)
            if len(sample) > 0:
                sample = sample.sample(
                    n=min(limit, len(sample)),
                    random_state=42,
                )
                return [tuple(row) for row in sample.itertuples(index=False, name=None)]

    fallback_points = [
        (-0.1276, 51.5074),
        (-0.1410, 51.5010),
        (-0.0990, 51.5140),
        (-0.0760, 51.5210),
        (-0.1890, 51.4930),
        (-0.1100, 51.5300),
        (-0.0840, 51.5000),
        (-0.1500, 51.5150),
    ]
    pairs: list[tuple[float, float, float, float]] = []
    for start_lon, start_lat in fallback_points:
        for end_lon, end_lat in fallback_points:
            if (start_lon, start_lat) != (end_lon, end_lat):
                pairs.append((start_lon, start_lat, end_lon, end_lat))
    return pairs[:limit]


def expand_probe_pairs(
    pairs: list[tuple[float, float, float, float]], target: int
) -> list[tuple[float, float, float, float]]:
    if not pairs:
        raise RuntimeError("Auto-tune probe pair list is empty.")
    if len(pairs) >= target:
        return pairs[:target]
    out: list[tuple[float, float, float, float]] = []
    index = 0
    while len(out) < target:
        out.append(pairs[index % len(pairs)])
        index += 1
    return out


def log_overall_progress(
    completed: int,
    total: int,
    overall_started_at: float,
    current_month: str,
) -> None:
    elapsed = max(time.monotonic() - overall_started_at, 1e-6)
    avg_per_month = elapsed / max(completed, 1)
    remaining = max(total - completed, 0)
    eta_seconds = remaining * avg_per_month
    percent = (completed / total * 100.0) if total else 100.0
    LOGGER.info(
        "Overall progress: %d/%d months (%.1f%%) | elapsed %.1fm | ETA %.1fm | last=%s",
        completed,
        total,
        percent,
        elapsed / 60.0,
        eta_seconds / 60.0,
        current_month,
    )


def write_backfill_state(
    state_file: Path,
    *,
    status: str,
    start_month: str,
    end_month: str,
    total_months: int,
    completed_months: int,
    current_month: str | None,
    next_month: str | None,
    elapsed_seconds: float,
) -> None:
    state_file.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "status": status,
        "updated_at_utc": datetime.now(timezone.utc).isoformat(),
        "start_month": start_month,
        "end_month": end_month,
        "total_months": total_months,
        "completed_months": completed_months,
        "percent_complete": round((completed_months / total_months * 100.0), 2)
        if total_months
        else 100.0,
        "current_month": current_month,
        "next_month": next_month,
        "elapsed_seconds": round(elapsed_seconds, 2),
    }
    state_file.write_text(f"{json.dumps(payload, indent=2)}\n", encoding="utf-8")


def benchmark_config(
    pairs: list[tuple[float, float, float, float]],
    osrm_url: str,
    workers: int,
    qps: float,
    timeout: int,
) -> dict[str, float]:
    limiter = bike_pipeline.RateLimiter(qps)
    session_local = threading.local()

    def get_session():
        session = getattr(session_local, "session", None)
        if session is None:
            session = bike_pipeline.build_http_session()
            session_local.session = session
        return session

    started = time.monotonic()
    success = 0
    failure = 0

    def run_pair(pair: tuple[float, float, float, float]) -> bool:
        start_lon, start_lat, end_lon, end_lat = pair
        limiter.wait()
        try:
            bike_pipeline.fetch_osrm_route(
                session=get_session(),
                osrm_base_url=osrm_url,
                start_lon=start_lon,
                start_lat=start_lat,
                end_lon=end_lon,
                end_lat=end_lat,
                timeout=timeout,
            )
            return True
        except Exception:  # noqa: BLE001
            return False

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
        for ok in pool.map(run_pair, pairs):
            if ok:
                success += 1
            else:
                failure += 1

    elapsed = max(time.monotonic() - started, 1e-6)
    total = success + failure
    success_rate = success / total if total else 0.0
    throughput = success / elapsed
    return {
        "workers": float(workers),
        "qps": float(qps),
        "success": float(success),
        "failure": float(failure),
        "elapsed_s": float(elapsed),
        "success_rate": float(success_rate),
        "throughput_success_rps": float(throughput),
    }


def auto_tune_osrm(args: argparse.Namespace) -> tuple[int, float]:
    cpu_count = os.cpu_count() or 8
    worker_candidates = sorted(
        {
            max(4, cpu_count // 2),
            max(8, cpu_count),
            max(12, int(cpu_count * 1.5)),
            max(16, cpu_count * 2),
        }
    )
    qps_candidates = sorted(
        {
            max(20.0, args.osrm_qps),
            80.0,
            120.0,
            180.0,
            260.0,
            320.0,
        }
    )

    probe_target = max(40, args.auto_tune_requests)
    pairs = load_probe_pairs(Path(args.route_cache), limit=probe_target)
    random.Random(42).shuffle(pairs)
    pairs = expand_probe_pairs(pairs, args.auto_tune_requests)

    LOGGER.info(
        "Auto-tune: benchmarking %d requests across %d worker levels x %d qps levels.",
        len(pairs),
        len(worker_candidates),
        len(qps_candidates),
    )

    results: list[dict[str, float]] = []
    for workers in worker_candidates:
        for qps in qps_candidates:
            metrics = benchmark_config(
                pairs=pairs,
                osrm_url=args.osrm_url,
                workers=workers,
                qps=qps,
                timeout=args.request_timeout,
            )
            results.append(metrics)
            LOGGER.info(
                "Auto-tune probe workers=%d qps=%.1f -> success %.1f%%, throughput %.1f rps",
                workers,
                qps,
                metrics["success_rate"] * 100.0,
                metrics["throughput_success_rps"],
            )

    acceptable = [row for row in results if row["success_rate"] >= 0.995]
    chosen_pool = acceptable if acceptable else results
    chosen = max(chosen_pool, key=lambda row: row["throughput_success_rps"])
    chosen_workers = int(chosen["workers"])
    chosen_qps = float(chosen["qps"])

    LOGGER.warning(
        "Auto-tune selected workers=%d qps=%.1f (success %.1f%%, throughput %.1f rps).",
        chosen_workers,
        chosen_qps,
        chosen["success_rate"] * 100.0,
        chosen["throughput_success_rps"],
    )
    if not acceptable:
        LOGGER.warning(
            "No probe reached >=99.5%% success rate; selected best-throughput fallback profile."
        )
    return chosen_workers, chosen_qps


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    resolved_end_month = (
        discover_latest_available_month(args.tfl_base_url, args.start_month)
        if args.end_month == "latest"
        else args.end_month
    )
    if args.auto_tune or args.auto_tune_only:
        tuned_workers, tuned_qps = auto_tune_osrm(args)
        args.osrm_workers = tuned_workers
        args.osrm_qps = tuned_qps
        if args.auto_tune_only:
            LOGGER.warning(
                "Auto-tune-only mode complete. Suggested flags: --osrm-workers %d --osrm-qps %.1f",
                tuned_workers,
                tuned_qps,
            )
            return

    months = iter_months(args.start_month, resolved_end_month)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    pause_file = Path(args.pause_file)
    state_file = Path(args.state_file)
    overall_started_at = time.monotonic()
    completed_months = 0

    LOGGER.info(
        "Backfill plan: %s -> %s (%d month(s))",
        args.start_month,
        resolved_end_month,
        len(months),
    )
    write_backfill_state(
        state_file=state_file,
        status="running",
        start_month=args.start_month,
        end_month=resolved_end_month,
        total_months=len(months),
        completed_months=completed_months,
        current_month=None,
        next_month=months[0] if months else None,
        elapsed_seconds=0.0,
    )

    paused = False
    for index, month in enumerate(months, start=1):
        if args.resume and month_has_output(month, output_dir):
            LOGGER.info("[%d/%d] Skipping %s (output already exists).", index, len(months), month)
            completed_months += 1
            log_overall_progress(
                completed=completed_months,
                total=len(months),
                overall_started_at=overall_started_at,
                current_month=month,
            )
            write_backfill_state(
                state_file=state_file,
                status="running",
                start_month=args.start_month,
                end_month=resolved_end_month,
                total_months=len(months),
                completed_months=completed_months,
                current_month=month,
                next_month=months[index] if index < len(months) else None,
                elapsed_seconds=time.monotonic() - overall_started_at,
            )
            if pause_file.exists():
                paused = True
                LOGGER.warning(
                    "Pause file detected (%s). Stopping cleanly after month %s.",
                    pause_file,
                    month,
                )
            continue

        if paused:
            break

        LOGGER.info("[%d/%d] Processing %s", index, len(months), month)
        month_started_at = time.monotonic()
        month_args = argparse.Namespace(
            month=month,
            tfl_base_url=args.tfl_base_url,
            osrm_url=args.osrm_url,
            download_dir=args.download_dir,
            extract_dir=args.extract_dir,
            output_dir=args.output_dir,
            route_cache=args.route_cache,
            osrm_workers=args.osrm_workers,
            osrm_qps=args.osrm_qps,
            request_timeout=args.request_timeout,
            max_trips=args.max_trips,
            max_trips_strategy=args.max_trips_strategy,
            max_trips_seed=args.max_trips_seed,
            max_new_routes=args.max_new_routes,
            verbose=args.verbose,
        )

        try:
            bike_pipeline.process_month(month_args)
            month_elapsed = time.monotonic() - month_started_at
            LOGGER.info("Month %s completed in %.1f minutes.", month, month_elapsed / 60.0)
            completed_months += 1
            log_overall_progress(
                completed=completed_months,
                total=len(months),
                overall_started_at=overall_started_at,
                current_month=month,
            )
            write_backfill_state(
                state_file=state_file,
                status="running",
                start_month=args.start_month,
                end_month=resolved_end_month,
                total_months=len(months),
                completed_months=completed_months,
                current_month=month,
                next_month=months[index] if index < len(months) else None,
                elapsed_seconds=time.monotonic() - overall_started_at,
            )
            if pause_file.exists():
                paused = True
                LOGGER.warning(
                    "Pause file detected (%s). Stopping cleanly after month %s.",
                    pause_file,
                    month,
                )
                break
        except Exception as exc:  # noqa: BLE001
            LOGGER.exception("Month %s failed: %s", month, exc)
            if not args.continue_on_error:
                raise

    if paused:
        write_backfill_state(
            state_file=state_file,
            status="paused",
            start_month=args.start_month,
            end_month=resolved_end_month,
            total_months=len(months),
            completed_months=completed_months,
            current_month=None,
            next_month=months[completed_months] if completed_months < len(months) else None,
            elapsed_seconds=time.monotonic() - overall_started_at,
        )
        LOGGER.warning(
            "Backfill paused. Remove %s and rerun same command with --resume to continue.",
            pause_file,
        )
        return

    dataset_files = bike_pipeline.list_dataset_parquet_files(output_dir)
    write_backfill_state(
        state_file=state_file,
        status="completed",
        start_month=args.start_month,
        end_month=resolved_end_month,
        total_months=len(months),
        completed_months=completed_months,
        current_month=None,
        next_month=None,
        elapsed_seconds=time.monotonic() - overall_started_at,
    )
    LOGGER.info(
        "Backfill finished. Dataset now has %d parquet day file(s) in %s.",
        len(dataset_files),
        output_dir,
    )


if __name__ == "__main__":
    main()
