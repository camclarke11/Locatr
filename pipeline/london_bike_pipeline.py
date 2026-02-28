#!/usr/bin/env python3
"""Build a London Santander cycle trip dataset for DuckDB WASM clients.

Pipeline steps:
1. Discover and download historical CSV/ZIP files from TfL usage stats.
2. Standardize station names and station coordinates.
3. Generate bike routes with OSRM and compress them as Polyline6 strings.
4. Export one Parquet file per day optimized for browser-side DuckDB queries.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import logging
import re
import threading
import time
import warnings
import zipfile
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, Sequence
from urllib.parse import parse_qsl, quote, urlencode, urljoin, urlparse, urlsplit, urlunsplit

import pandas as pd
import polyline
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

LOGGER = logging.getLogger("london-bike-pipeline")
LONDON_LAT_MIN = 51.20
LONDON_LAT_MAX = 51.75
LONDON_LON_MIN = -0.60
LONDON_LON_MAX = 0.35

COLUMN_ALIASES = {
    "trip_id": [
        "rental id",
        "rentalid",
        "journey id",
        "journeyid",
        "trip id",
        "tripid",
    ],
    "start_time": [
        "start date",
        "startdate",
        "start datetime",
        "startdatetime",
        "started at",
        "startedat",
    ],
    "end_time": [
        "end date",
        "enddate",
        "end datetime",
        "enddatetime",
        "ended at",
        "endedat",
    ],
    "start_station": [
        "start station name",
        "startstationname",
        "start station",
        "startstation",
    ],
    "start_station_number": [
        "start station number",
        "startstationnumber",
        "start station id",
        "startstationid",
    ],
    "end_station": [
        "end station name",
        "endstationname",
        "end station",
        "endstation",
    ],
    "end_station_number": [
        "end station number",
        "endstationnumber",
        "end station id",
        "endstationid",
    ],
    "start_lat": [
        "start station latitude",
        "startstationlatitude",
        "start latitude",
        "startlat",
    ],
    "start_lon": [
        "start station longitude",
        "startstationlongitude",
        "start longitude",
        "startlon",
    ],
    "end_lat": [
        "end station latitude",
        "endstationlatitude",
        "end latitude",
        "endlat",
    ],
    "end_lon": [
        "end station longitude",
        "endstationlongitude",
        "end longitude",
        "endlon",
    ],
}


@dataclass(frozen=True)
class RouteResult:
    start_lon: float
    start_lat: float
    end_lon: float
    end_lat: float
    route_geometry: str
    route_distance_m: float
    route_duration_s: float
    route_source: str


class RateLimiter:
    """Token-like limiter that enforces max requests per second globally."""

    def __init__(self, qps: float) -> None:
        if qps <= 0:
            raise ValueError("qps must be greater than 0")
        self.interval = 1.0 / qps
        self._lock = threading.Lock()
        self._next_ts = 0.0

    def wait(self) -> None:
        with self._lock:
            now = time.monotonic()
            if now < self._next_ts:
                time.sleep(self._next_ts - now)
                now = time.monotonic()
            self._next_ts = max(self._next_ts, now) + self.interval


def normalize_text(value: str) -> str:
    return re.sub(r"[^a-z0-9]", "", value.lower())


def normalize_station_name(name: object) -> str:
    if pd.isna(name):
        return "Unknown"
    clean = str(name).strip()
    clean = clean.replace("’", "'").replace("&amp;", "&")
    clean = re.sub(r"\s+", " ", clean)
    if not clean:
        return "Unknown"
    titled = clean.title()
    return titled.replace("'S", "'s")


def parse_month(month_str: str) -> tuple[datetime, datetime]:
    month_start = datetime.strptime(month_str, "%Y-%m").replace(tzinfo=timezone.utc)
    if month_start.month == 12:
        month_end = month_start.replace(year=month_start.year + 1, month=1)
    else:
        month_end = month_start.replace(month=month_start.month + 1)
    return month_start, month_end


def cap_trips(
    trips: pd.DataFrame, max_trips: int, strategy: str, seed: int
) -> pd.DataFrame:
    if max_trips <= 0:
        raise ValueError("--max-trips must be greater than 0")
    if len(trips) <= max_trips:
        return trips

    if strategy == "earliest":
        capped = trips.head(max_trips).copy()
    else:
        capped = trips.sample(n=max_trips, random_state=seed).sort_values("start_time").copy()

    day_count = capped["start_time"].dt.date.nunique()
    LOGGER.warning(
        "Trimmed dataset to %d trips via --max-trips using '%s' strategy (%d unique days retained).",
        len(capped),
        strategy,
        day_count,
    )
    return capped


def list_dataset_parquet_files(parquet_dir: Path) -> list[Path]:
    return sorted(
        [path for path in parquet_dir.glob("*.parquet") if re.match(r"\d{4}-\d{2}-\d{2}\.parquet$", path.name)]
    )


def write_dataset_manifest(
    parquet_dir: Path,
    month: str,
    run_trip_count: int,
    run_start_time: datetime,
    run_end_time: datetime,
    route_cache_size: int,
) -> Path:
    dataset_files = list_dataset_parquet_files(parquet_dir)
    metadata = {
        "month": month,
        "trip_count": int(run_trip_count),
        "date_range_utc": {
            "min_start_time": run_start_time.isoformat(),
            "max_end_time": run_end_time.isoformat(),
        },
        "parquet_files": [path.name for path in dataset_files],
        "run_parquet_file_count": len([path for path in dataset_files if path.name.startswith(f"{month}-")]),
        "dataset_parquet_file_count": len(dataset_files),
        "routes_cached": int(route_cache_size),
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    metadata_path = parquet_dir / "manifest.json"
    metadata_path.write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    return metadata_path


def build_http_session() -> requests.Session:
    retry = Retry(
        total=4,
        connect=4,
        read=4,
        backoff_factor=0.4,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
    )
    adapter = HTTPAdapter(max_retries=retry)
    session = requests.Session()
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update({"User-Agent": "london-bike-pipeline/1.0"})
    return session


def extract_date_ranges(text: str) -> list[tuple[datetime, datetime]]:
    patterns = [
        (r"(\d{1,2}[A-Za-z]{3}\d{4})-(\d{1,2}[A-Za-z]{3}\d{4})", "%d%b%Y"),
        (r"(\d{1,2}[A-Za-z]{3}\d{2})-(\d{1,2}[A-Za-z]{3}\d{2})", "%d%b%y"),
        (r"(\d{8})-(\d{8})", "%Y%m%d"),
    ]
    ranges: list[tuple[datetime, datetime]] = []
    for pattern, fmt in patterns:
        for start_text, end_text in re.findall(pattern, text):
            try:
                start = datetime.strptime(start_text, fmt).replace(tzinfo=timezone.utc)
                end = datetime.strptime(end_text, fmt).replace(tzinfo=timezone.utc)
            except ValueError:
                continue
            if end < start:
                start, end = end, start
            ranges.append((start, end + timedelta(days=1)))
    return ranges


def overlaps(
    range_start: datetime, range_end: datetime, month_start: datetime, month_end: datetime
) -> bool:
    return range_start < month_end and range_end > month_start


def discover_month_urls(
    session: requests.Session, base_url: str, month_start: datetime, month_end: datetime
) -> list[str]:
    def parse_s3_listing(listing_url: str) -> tuple[str, set[str]]:
        """Read all S3 listing pages and return bucket host + file URLs."""

        def build_page_url(next_token: str | None) -> str:
            split = urlsplit(listing_url)
            query = dict(parse_qsl(split.query, keep_blank_values=True))
            if next_token:
                query["continuation-token"] = next_token
            else:
                query.pop("continuation-token", None)
            return urlunsplit(
                (split.scheme, split.netloc, split.path, urlencode(query), split.fragment)
            )

        candidates: set[str] = set()
        bucket_name = ""
        next_token: str | None = None
        while True:
            response = session.get(build_page_url(next_token), timeout=45)
            response.raise_for_status()
            root = ET.fromstring(response.text)
            ns = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}

            bucket_name = (
                root.findtext("s3:Name", default="", namespaces=ns) or bucket_name
            ).strip()
            for key in root.findall("s3:Contents/s3:Key", ns):
                raw_key = (key.text or "").strip()
                lower = raw_key.lower()
                if lower.endswith(".csv") or lower.endswith(".zip"):
                    candidates.add(f"https://{bucket_name}/{quote(raw_key, safe='/')}")

            is_truncated = (
                root.findtext("s3:IsTruncated", default="false", namespaces=ns).lower()
                == "true"
            )
            if not is_truncated:
                break
            next_token = root.findtext(
                "s3:NextContinuationToken", default="", namespaces=ns
            ).strip()
            if not next_token:
                break

        if not bucket_name:
            raise RuntimeError("Unable to parse S3 listing bucket name from --tfl-base-url.")
        return bucket_name, candidates

    LOGGER.info("Discovering TfL files from %s", base_url)
    response = session.get(base_url, timeout=45)
    if (
        response.status_code == 404
        and base_url.rstrip("/") == "https://cycling.data.tfl.gov.uk/usage-stats"
    ):
        fallback_url = (
            "https://s3-eu-west-1.amazonaws.com/"
            "cycling.data.tfl.gov.uk/?list-type=2&prefix=usage-stats/"
        )
        LOGGER.warning(
            "TfL usage-stats path returned 404; falling back to S3 listing endpoint: %s",
            fallback_url,
        )
        return discover_month_urls(
            session=session,
            base_url=fallback_url,
            month_start=month_start,
            month_end=month_end,
        )
    response.raise_for_status()

    all_candidates: set[str] = set()
    if "<ListBucketResult" in response.text:
        _, all_candidates = parse_s3_listing(base_url)
    else:
        hrefs = re.findall(r"""href=["']([^"']+)["']""", response.text, flags=re.IGNORECASE)
        for href in hrefs:
            lower = href.lower()
            if not lower.endswith(".csv") and not lower.endswith(".zip"):
                continue
            all_candidates.add(urljoin(base_url, href))

    month_tokens = {
        month_start.strftime("%Y%m"),
        month_start.strftime("%Y-%m"),
        month_start.strftime("%b%Y").lower(),
        month_start.strftime("%B%Y").lower(),
        month_start.strftime("%b%y").lower(),
    }

    selected: list[str] = []
    for url in sorted(all_candidates):
        filename = Path(urlparse(url).path).name
        name_lower = filename.lower()
        ranges = extract_date_ranges(filename)
        if ranges:
            if any(overlaps(rs, re_, month_start, month_end) for rs, re_ in ranges):
                selected.append(url)
            continue

        if any(token in name_lower for token in month_tokens):
            selected.append(url)

    if not selected:
        raise RuntimeError(
            "No TfL CSV/ZIP files discovered for the requested month. "
            "Check --month and --tfl-base-url."
        )

    LOGGER.info("Discovered %d source files", len(selected))
    return selected


def download_files(
    session: requests.Session, urls: Sequence[str], download_dir: Path
) -> list[Path]:
    download_dir.mkdir(parents=True, exist_ok=True)
    artifacts: list[Path] = []
    for url in urls:
        filename = Path(urlparse(url).path).name
        target = download_dir / filename
        if target.exists() and target.stat().st_size > 0:
            LOGGER.info("Reusing download: %s", target.name)
            artifacts.append(target)
            continue

        LOGGER.info("Downloading: %s", url)
        with session.get(url, timeout=90, stream=True) as response:
            response.raise_for_status()
            with target.open("wb") as output:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        output.write(chunk)
        artifacts.append(target)
    return artifacts


def extract_csv_paths(artifacts: Sequence[Path], extract_dir: Path) -> list[Path]:
    extract_dir.mkdir(parents=True, exist_ok=True)
    csv_paths: list[Path] = []
    for artifact in artifacts:
        suffix = artifact.suffix.lower()
        if suffix == ".csv":
            csv_paths.append(artifact)
            continue
        if suffix != ".zip":
            continue

        target_dir = extract_dir / artifact.stem
        target_dir.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(artifact) as archive:
            members = [name for name in archive.namelist() if name.lower().endswith(".csv")]
            for member in members:
                archive.extract(member, path=target_dir)
                csv_paths.append(target_dir / member)
    if not csv_paths:
        raise RuntimeError("No CSV files found in downloaded artifacts.")
    LOGGER.info("Prepared %d CSV files for ingestion", len(csv_paths))
    return csv_paths


def find_column(columns: Iterable[str], aliases: Sequence[str], required: bool) -> str | None:
    normalized_to_original = {normalize_text(col): col for col in columns}
    alias_keys = [normalize_text(alias) for alias in aliases]

    for alias_key in alias_keys:
        if alias_key in normalized_to_original:
            return normalized_to_original[alias_key]

    for col_norm, original in normalized_to_original.items():
        if any(alias_key in col_norm for alias_key in alias_keys):
            return original

    if required:
        raise KeyError(f"Could not resolve required column aliases: {aliases}")
    return None


def parse_datetime_pair(
    start_values: pd.Series, end_values: pd.Series
) -> tuple[pd.Series, pd.Series, str]:
    """Parse start/end datetimes using the strategy that preserves sane durations."""

    def parse(values: pd.Series, *, dayfirst: bool) -> pd.Series:
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore",
                message="Parsing dates in %Y-%m-%d %H:%M format when dayfirst=True was specified.*",
                category=UserWarning,
            )
            return pd.to_datetime(values, errors="coerce", dayfirst=dayfirst, utc=True)

    start_dayfirst_false = parse(start_values, dayfirst=False)
    end_dayfirst_false = parse(end_values, dayfirst=False)
    score_dayfirst_false = (
        start_dayfirst_false.notna()
        & end_dayfirst_false.notna()
        & (end_dayfirst_false >= start_dayfirst_false)
    ).sum()

    start_dayfirst_true = parse(start_values, dayfirst=True)
    end_dayfirst_true = parse(end_values, dayfirst=True)
    score_dayfirst_true = (
        start_dayfirst_true.notna()
        & end_dayfirst_true.notna()
        & (end_dayfirst_true >= start_dayfirst_true)
    ).sum()

    if score_dayfirst_true > score_dayfirst_false:
        return start_dayfirst_true, end_dayfirst_true, "dayfirst=True"
    return start_dayfirst_false, end_dayfirst_false, "dayfirst=False"


def normalize_trip_frame(raw: pd.DataFrame, source_name: str) -> pd.DataFrame:
    columns = list(raw.columns)
    resolved = {
        "trip_id": find_column(columns, COLUMN_ALIASES["trip_id"], required=False),
        "start_time": find_column(columns, COLUMN_ALIASES["start_time"], required=True),
        "end_time": find_column(columns, COLUMN_ALIASES["end_time"], required=True),
        "start_station": find_column(columns, COLUMN_ALIASES["start_station"], required=True),
        "start_station_number": find_column(
            columns, COLUMN_ALIASES["start_station_number"], required=False
        ),
        "end_station": find_column(columns, COLUMN_ALIASES["end_station"], required=True),
        "end_station_number": find_column(
            columns, COLUMN_ALIASES["end_station_number"], required=False
        ),
        "start_lat": find_column(columns, COLUMN_ALIASES["start_lat"], required=False),
        "start_lon": find_column(columns, COLUMN_ALIASES["start_lon"], required=False),
        "end_lat": find_column(columns, COLUMN_ALIASES["end_lat"], required=False),
        "end_lon": find_column(columns, COLUMN_ALIASES["end_lon"], required=False),
    }

    frame = pd.DataFrame()
    if resolved["trip_id"] is not None:
        frame["trip_id"] = raw[resolved["trip_id"]].astype("string")
    else:
        frame["trip_id"] = pd.Series(index=raw.index, dtype="string")

    frame["start_time"], frame["end_time"], datetime_strategy = parse_datetime_pair(
        raw[resolved["start_time"]],
        raw[resolved["end_time"]],
    )
    LOGGER.debug("Datetime parse strategy for %s: %s", source_name, datetime_strategy)

    frame["start_station"] = raw[resolved["start_station"]].map(normalize_station_name)
    frame["end_station"] = raw[resolved["end_station"]].map(normalize_station_name)
    if resolved["start_station_number"] is not None:
        frame["start_station_number"] = pd.to_numeric(
            raw[resolved["start_station_number"]], errors="coerce"
        ).astype("Int64")
    else:
        frame["start_station_number"] = pd.Series(index=raw.index, dtype="Int64")
    if resolved["end_station_number"] is not None:
        frame["end_station_number"] = pd.to_numeric(
            raw[resolved["end_station_number"]], errors="coerce"
        ).astype("Int64")
    else:
        frame["end_station_number"] = pd.Series(index=raw.index, dtype="Int64")

    if resolved["start_lat"] is not None:
        frame["start_lat"] = pd.to_numeric(raw[resolved["start_lat"]], errors="coerce")
    else:
        frame["start_lat"] = pd.Series(index=raw.index, dtype="float64")
    if resolved["start_lon"] is not None:
        frame["start_lon"] = pd.to_numeric(raw[resolved["start_lon"]], errors="coerce")
    else:
        frame["start_lon"] = pd.Series(index=raw.index, dtype="float64")
    if resolved["end_lat"] is not None:
        frame["end_lat"] = pd.to_numeric(raw[resolved["end_lat"]], errors="coerce")
    else:
        frame["end_lat"] = pd.Series(index=raw.index, dtype="float64")
    if resolved["end_lon"] is not None:
        frame["end_lon"] = pd.to_numeric(raw[resolved["end_lon"]], errors="coerce")
    else:
        frame["end_lon"] = pd.Series(index=raw.index, dtype="float64")
    frame["source_file"] = source_name

    required = ["start_time", "end_time", "start_station", "end_station"]
    frame = frame.dropna(subset=required).copy()
    frame = frame[frame["end_time"] >= frame["start_time"]].copy()

    frame["trip_id"] = frame["trip_id"].fillna("").str.strip()
    missing_trip_id = frame["trip_id"] == ""
    if missing_trip_id.any():
        seed = (
            frame.loc[missing_trip_id, "start_time"].astype("int64").astype("string")
            + "|"
            + frame.loc[missing_trip_id, "end_time"].astype("int64").astype("string")
            + "|"
            + frame.loc[missing_trip_id, "start_station"]
            + "|"
            + frame.loc[missing_trip_id, "end_station"]
        )
        frame.loc[missing_trip_id, "trip_id"] = "auto_" + pd.util.hash_pandas_object(
            seed, index=False
        ).astype("string")

    return frame


def load_bikepoint_station_reference(
    session: requests.Session, timeout: int
) -> pd.DataFrame:
    """Load current station coordinates from TfL BikePoint API."""
    response = session.get("https://api.tfl.gov.uk/BikePoint", timeout=timeout)
    response.raise_for_status()
    points = response.json()

    records: list[dict[str, object]] = []
    for item in points:
        if not isinstance(item, dict):
            continue
        station_id = str(item.get("id", ""))
        match = re.search(r"(\d+)$", station_id)
        station_number = int(match.group(1)) if match else None
        records.append(
            {
                "station_number": station_number,
                "station_name": normalize_station_name(item.get("commonName", "")),
                "lat": pd.to_numeric(item.get("lat"), errors="coerce"),
                "lon": pd.to_numeric(item.get("lon"), errors="coerce"),
            }
        )

    reference = pd.DataFrame.from_records(records).dropna(subset=["lat", "lon"])
    reference["station_number"] = pd.to_numeric(
        reference["station_number"], errors="coerce"
    ).astype("Int64")
    return reference


def backfill_station_coordinates(
    trips: pd.DataFrame, station_reference: pd.DataFrame
) -> pd.DataFrame:
    """Backfill missing start/end coordinates using station number or station name."""
    out = trips.copy()

    by_number = station_reference.dropna(subset=["station_number"]).drop_duplicates(
        subset=["station_number"], keep="first"
    )
    number_to_lat = by_number.set_index("station_number")["lat"].to_dict()
    number_to_lon = by_number.set_index("station_number")["lon"].to_dict()

    by_name = (
        station_reference.groupby("station_name", as_index=False)
        .agg({"lat": "median", "lon": "median"})
        .set_index("station_name")
    )
    name_to_lat = by_name["lat"].to_dict()
    name_to_lon = by_name["lon"].to_dict()

    out["start_lat"] = out["start_lat"].fillna(out["start_station_number"].map(number_to_lat))
    out["start_lon"] = out["start_lon"].fillna(out["start_station_number"].map(number_to_lon))
    out["end_lat"] = out["end_lat"].fillna(out["end_station_number"].map(number_to_lat))
    out["end_lon"] = out["end_lon"].fillna(out["end_station_number"].map(number_to_lon))

    out["start_lat"] = out["start_lat"].fillna(out["start_station"].map(name_to_lat))
    out["start_lon"] = out["start_lon"].fillna(out["start_station"].map(name_to_lon))
    out["end_lat"] = out["end_lat"].fillna(out["end_station"].map(name_to_lat))
    out["end_lon"] = out["end_lon"].fillna(out["end_station"].map(name_to_lon))
    return out


def standardize_station_coordinates(trips: pd.DataFrame) -> pd.DataFrame:
    station_samples = pd.concat(
        [
            trips[["start_station", "start_lat", "start_lon"]].rename(
                columns={
                    "start_station": "station",
                    "start_lat": "lat",
                    "start_lon": "lon",
                }
            ),
            trips[["end_station", "end_lat", "end_lon"]].rename(
                columns={
                    "end_station": "station",
                    "end_lat": "lat",
                    "end_lon": "lon",
                }
            ),
        ],
        ignore_index=True,
    )

    station_reference = (
        station_samples.groupby("station", as_index=False)
        .agg({"lat": "median", "lon": "median"})
        .set_index("station")
    )
    station_reference["lat"] = station_reference["lat"].round(6)
    station_reference["lon"] = station_reference["lon"].round(6)

    out = trips.copy()
    out = out.join(station_reference.rename(columns={"lat": "start_lat_ref", "lon": "start_lon_ref"}), on="start_station")
    out = out.join(station_reference.rename(columns={"lat": "end_lat_ref", "lon": "end_lon_ref"}), on="end_station")
    out["start_lat"] = out["start_lat_ref"].fillna(out["start_lat"])
    out["start_lon"] = out["start_lon_ref"].fillna(out["start_lon"])
    out["end_lat"] = out["end_lat_ref"].fillna(out["end_lat"])
    out["end_lon"] = out["end_lon_ref"].fillna(out["end_lon"])
    out = out.drop(columns=["start_lat_ref", "start_lon_ref", "end_lat_ref", "end_lon_ref"])

    mask = (
        out["start_lat"].between(LONDON_LAT_MIN, LONDON_LAT_MAX)
        & out["end_lat"].between(LONDON_LAT_MIN, LONDON_LAT_MAX)
        & out["start_lon"].between(LONDON_LON_MIN, LONDON_LON_MAX)
        & out["end_lon"].between(LONDON_LON_MIN, LONDON_LON_MAX)
    )
    filtered = out[mask].copy()
    LOGGER.info("Rows after London bbox filter: %d", len(filtered))
    return filtered


def route_key(start_lon: float, start_lat: float, end_lon: float, end_lat: float) -> str:
    return f"{start_lon:.6f}|{start_lat:.6f}|{end_lon:.6f}|{end_lat:.6f}"


def straight_line_polyline6(
    start_lon: float, start_lat: float, end_lon: float, end_lat: float
) -> str:
    points = [(start_lat, start_lon)]
    if start_lon != end_lon or start_lat != end_lat:
        points.append((end_lat, end_lon))
    return polyline.encode(points, precision=6)


def fetch_osrm_route(
    session: requests.Session,
    osrm_base_url: str,
    start_lon: float,
    start_lat: float,
    end_lon: float,
    end_lat: float,
    timeout: int,
) -> RouteResult:
    if start_lon == end_lon and start_lat == end_lat:
        return RouteResult(
            start_lon=start_lon,
            start_lat=start_lat,
            end_lon=end_lon,
            end_lat=end_lat,
            route_geometry=straight_line_polyline6(start_lon, start_lat, end_lon, end_lat),
            route_distance_m=0.0,
            route_duration_s=0.0,
            route_source="stationary",
        )

    route_url = (
        f"{osrm_base_url.rstrip('/')}/route/v1/bicycle/"
        f"{start_lon:.6f},{start_lat:.6f};{end_lon:.6f},{end_lat:.6f}"
    )
    response = session.get(
        route_url,
        params={
            "overview": "full",
            "geometries": "polyline6",
            "steps": "false",
            "alternatives": "false",
        },
        timeout=timeout,
    )
    response.raise_for_status()
    payload = response.json()
    if payload.get("code") != "Ok" or not payload.get("routes"):
        raise RuntimeError(f"OSRM returned no routes: {payload.get('code')}")

    best = payload["routes"][0]
    return RouteResult(
        start_lon=start_lon,
        start_lat=start_lat,
        end_lon=end_lon,
        end_lat=end_lat,
        route_geometry=best["geometry"],
        route_distance_m=float(best.get("distance", 0.0)),
        route_duration_s=float(best.get("duration", 0.0)),
        route_source="osrm",
    )


def load_route_cache(cache_path: Path) -> dict[str, RouteResult]:
    if not cache_path.exists():
        return {}

    frame = pd.read_parquet(cache_path)
    cache: dict[str, RouteResult] = {}
    for row in frame.itertuples(index=False):
        result = RouteResult(
            start_lon=float(row.start_lon),
            start_lat=float(row.start_lat),
            end_lon=float(row.end_lon),
            end_lat=float(row.end_lat),
            route_geometry=str(row.route_geometry),
            route_distance_m=float(row.route_distance_m),
            route_duration_s=float(row.route_duration_s),
            route_source=str(row.route_source),
        )
        cache[route_key(result.start_lon, result.start_lat, result.end_lon, result.end_lat)] = result
    LOGGER.info("Loaded %d cached routes", len(cache))
    return cache


def save_route_cache(cache_path: Path, cache: dict[str, RouteResult]) -> None:
    if not cache:
        return
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    rows = [
        {
            "start_lon": result.start_lon,
            "start_lat": result.start_lat,
            "end_lon": result.end_lon,
            "end_lat": result.end_lat,
            "route_geometry": result.route_geometry,
            "route_distance_m": result.route_distance_m,
            "route_duration_s": result.route_duration_s,
            "route_source": result.route_source,
        }
        for result in cache.values()
    ]
    frame = pd.DataFrame(rows).sort_values(
        by=["start_lon", "start_lat", "end_lon", "end_lat"], ignore_index=True
    )
    frame.to_parquet(cache_path, compression="zstd", index=False)
    LOGGER.info("Saved route cache with %d entries -> %s", len(frame), cache_path)


def hydrate_routes(
    trips: pd.DataFrame,
    cache: dict[str, RouteResult],
    osrm_base_url: str,
    timeout: int,
    osrm_workers: int,
    osrm_qps: float,
    max_new_routes: int | None,
) -> dict[str, RouteResult]:
    unique_pairs = (
        trips[["start_lon", "start_lat", "end_lon", "end_lat"]]
        .round(6)
        .drop_duplicates(ignore_index=True)
    )
    missing: list[tuple[float, float, float, float]] = []
    for row in unique_pairs.itertuples(index=False):
        key = route_key(row.start_lon, row.start_lat, row.end_lon, row.end_lat)
        if key not in cache:
            missing.append((row.start_lon, row.start_lat, row.end_lon, row.end_lat))

    if not missing:
        LOGGER.info("All route pairs already cached.")
        return cache

    limiter = RateLimiter(osrm_qps)
    session_local = threading.local()

    def get_session() -> requests.Session:
        session = getattr(session_local, "session", None)
        if session is None:
            session = build_http_session()
            session_local.session = session
        return session

    fetch_count = len(missing)
    if max_new_routes is not None and fetch_count > max_new_routes:
        LOGGER.warning(
            "Capping OSRM fetches at %d (requested %d unique pairs).",
            max_new_routes,
            fetch_count,
        )
        to_fetch = missing[:max_new_routes]
        to_fallback = missing[max_new_routes:]
    else:
        to_fetch = missing
        to_fallback = []

    total_to_fetch = len(to_fetch)
    LOGGER.info(
        "Fetching %d OSRM routes with %d workers (qps=%.2f, timeout=%ss)",
        total_to_fetch,
        osrm_workers,
        osrm_qps,
        timeout,
    )

    def fetch_pair(pair: tuple[float, float, float, float]) -> RouteResult:
        start_lon, start_lat, end_lon, end_lat = pair
        limiter.wait()
        try:
            return fetch_osrm_route(
                session=get_session(),
                osrm_base_url=osrm_base_url,
                start_lon=start_lon,
                start_lat=start_lat,
                end_lon=end_lon,
                end_lat=end_lat,
                timeout=timeout,
            )
        except Exception as exc:  # noqa: BLE001
            LOGGER.debug("OSRM fallback for pair %s due to %s", pair, exc)
            return RouteResult(
                start_lon=start_lon,
                start_lat=start_lat,
                end_lon=end_lon,
                end_lat=end_lat,
                route_geometry=straight_line_polyline6(start_lon, start_lat, end_lon, end_lat),
                route_distance_m=0.0,
                route_duration_s=0.0,
                route_source="fallback_straight_line",
            )

    with concurrent.futures.ThreadPoolExecutor(max_workers=osrm_workers) as pool:
        futures = {pool.submit(fetch_pair, pair): pair for pair in to_fetch}
        completed = 0
        started_at = time.monotonic()
        last_progress_log_at = started_at
        progress_interval_count = max(50, total_to_fetch // 100 if total_to_fetch else 1)
        for future in concurrent.futures.as_completed(futures):
            completed += 1
            result = future.result()
            key = route_key(result.start_lon, result.start_lat, result.end_lon, result.end_lat)
            cache[key] = result
            now = time.monotonic()
            should_log_by_count = completed % progress_interval_count == 0
            should_log_by_time = now - last_progress_log_at >= 10.0
            if should_log_by_count or should_log_by_time or completed == total_to_fetch:
                elapsed_s = max(now - started_at, 1e-6)
                rate = completed / elapsed_s
                remaining = max(total_to_fetch - completed, 0)
                eta_s = int(remaining / rate) if rate > 0 else -1
                LOGGER.info(
                    "OSRM progress: %d/%d (%.1f%%) | %.1f pairs/s | elapsed %ds | eta %ss",
                    completed,
                    total_to_fetch,
                    (completed / total_to_fetch) * 100 if total_to_fetch else 100.0,
                    rate,
                    int(elapsed_s),
                    eta_s if eta_s >= 0 else "?",
                )
                last_progress_log_at = now

    for start_lon, start_lat, end_lon, end_lat in to_fallback:
        result = RouteResult(
            start_lon=start_lon,
            start_lat=start_lat,
            end_lon=end_lon,
            end_lat=end_lat,
            route_geometry=straight_line_polyline6(start_lon, start_lat, end_lon, end_lat),
            route_distance_m=0.0,
            route_duration_s=0.0,
            route_source="fallback_max_new_routes",
        )
        cache[route_key(start_lon, start_lat, end_lon, end_lat)] = result

    return cache


def write_daily_parquet(trips: pd.DataFrame, out_dir: Path) -> list[Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    file_paths: list[Path] = []
    for day, day_frame in trips.groupby(trips["start_time"].dt.date, sort=True):
        ordered = day_frame.sort_values("start_time").reset_index(drop=True)
        target = out_dir / f"{day.isoformat()}.parquet"
        table = pa.Table.from_pandas(
            ordered[
                [
                    "trip_id",
                    "start_time",
                    "end_time",
                    "route_geometry",
                    "route_source",
                    "route_distance_m",
                    "route_duration_s",
                ]
            ],
            preserve_index=False,
        )
        pq.write_table(
            table,
            target,
            compression="zstd",
            use_dictionary=["route_geometry"],
            row_group_size=50_000,
        )
        file_paths.append(target)
    return file_paths


def process_month(args: argparse.Namespace) -> dict[str, object]:
    month_start, month_end = parse_month(args.month)
    session = build_http_session()

    discovered_urls = discover_month_urls(
        session=session,
        base_url=args.tfl_base_url,
        month_start=month_start,
        month_end=month_end,
    )
    artifacts = download_files(session, discovered_urls, Path(args.download_dir))
    csv_paths = extract_csv_paths(artifacts, Path(args.extract_dir))

    normalized_frames: list[pd.DataFrame] = []
    for csv_path in csv_paths:
        LOGGER.info("Reading CSV: %s", csv_path)
        raw = pd.read_csv(csv_path, low_memory=False)
        normalized = normalize_trip_frame(raw, source_name=csv_path.name)
        normalized = normalized[
            (normalized["start_time"] >= month_start) & (normalized["start_time"] < month_end)
        ].copy()
        LOGGER.info(
            "Prepared %d rows from %s after normalization + month filter",
            len(normalized),
            csv_path.name,
        )
        normalized_frames.append(normalized)

    if not normalized_frames:
        raise RuntimeError("No trips found in source files.")

    trips = pd.concat(normalized_frames, ignore_index=True)
    trips = trips.drop_duplicates(
        subset=["trip_id", "start_time", "end_time", "start_station", "end_station"]
    )
    station_reference = load_bikepoint_station_reference(
        session=session, timeout=args.request_timeout
    )
    trips = backfill_station_coordinates(trips, station_reference)
    trips = standardize_station_coordinates(trips)
    trips = trips.sort_values("start_time").reset_index(drop=True)

    if args.max_trips is not None:
        trips = cap_trips(
            trips=trips,
            max_trips=args.max_trips,
            strategy=args.max_trips_strategy,
            seed=args.max_trips_seed,
        )

    trips = trips.round({"start_lon": 6, "start_lat": 6, "end_lon": 6, "end_lat": 6})
    trips["route_key"] = trips.apply(
        lambda row: route_key(row.start_lon, row.start_lat, row.end_lon, row.end_lat), axis=1
    )

    cache_path = Path(args.route_cache)
    route_cache = load_route_cache(cache_path)
    route_cache = hydrate_routes(
        trips=trips,
        cache=route_cache,
        osrm_base_url=args.osrm_url,
        timeout=args.request_timeout,
        osrm_workers=args.osrm_workers,
        osrm_qps=args.osrm_qps,
        max_new_routes=args.max_new_routes,
    )
    save_route_cache(cache_path, route_cache)

    trips["route_geometry"] = trips["route_key"].map(
        lambda key: route_cache[key].route_geometry
        if key in route_cache
        else straight_line_polyline6(0.0, 0.0, 0.0, 0.0)
    )
    trips["route_source"] = trips["route_key"].map(
        lambda key: route_cache[key].route_source if key in route_cache else "fallback_missing"
    )
    trips["route_distance_m"] = trips["route_key"].map(
        lambda key: route_cache[key].route_distance_m if key in route_cache else 0.0
    )
    trips["route_duration_s"] = trips["route_key"].map(
        lambda key: route_cache[key].route_duration_s if key in route_cache else 0.0
    )
    trips = trips.drop(columns=["route_key", "source_file"])

    parquet_dir = Path(args.output_dir)
    parquet_paths = write_daily_parquet(trips, parquet_dir)
    metadata_path = write_dataset_manifest(
        parquet_dir=parquet_dir,
        month=args.month,
        run_trip_count=len(trips),
        run_start_time=trips["start_time"].min(),
        run_end_time=trips["end_time"].max(),
        route_cache_size=len(route_cache),
    )

    LOGGER.info("Pipeline complete.")
    LOGGER.info("Trips exported: %d", len(trips))
    LOGGER.info("Parquet files: %d -> %s", len(parquet_paths), parquet_dir)
    LOGGER.info("Manifest: %s", metadata_path)
    return {
        "month": args.month,
        "trip_count": int(len(trips)),
        "manifest_path": str(metadata_path),
        "parquet_paths": [str(path) for path in parquet_paths],
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build daily Polyline6 route parquet files for Santander bike trips."
    )
    parser.add_argument(
        "--month",
        required=True,
        help="Month to process in YYYY-MM format (example: 2024-05).",
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
        help="Route cache parquet to avoid repeated OSRM calls across runs.",
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
        help="Optional cap for number of trips processed (for smoke testing).",
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
        help="Optional cap on new OSRM lookups in this run. Overflow uses straight-line fallback.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    process_month(args)


if __name__ == "__main__":
    main()
