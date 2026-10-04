"""Download MSC GeoMet ``climate-hourly`` responses for a selection.

:func:`fetch_collection` requests the collection one Climate ID and one
local-standard-time calendar year at a time, first as a ``resulttype=hits``
count and then as ``f=csv`` pages. Every response body is cached under
``downloads/`` by content digest, and a selection manifest in the format that
:mod:`research_store.ingestion.geomet_climate_hourly` declares names each one
with its request URL, retrieval time and SHA-256. Nothing here reads or writes
the catalogue: `research-store ingest` of the manifest is the offline step.
"""

from __future__ import annotations

import csv
import hashlib
import http.client
import json
import time
import urllib.error
import urllib.request
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any

from research_store.foundation.models import Registry
from research_store.ingestion.geomet_climate_hourly import (
    MANIFEST_TIME_FORMAT,
    RETRIEVED_AT_FORMAT,
    ROLE_HITS,
    ROLE_PAGE,
    Window,
    api_options,
    atomic_write,
    hits_count,
    hits_url,
    page_row_count,
    page_url,
    parse_retrieved_at,
    window_settled,
    windows,
    write_manifest,
)

# Politeness: strictly sequential requests at least this far apart, and a
# bounded, growing back-off when the server says it is busy.
MIN_REQUEST_INTERVAL_SECONDS = 1.0
TIMEOUT_SECONDS = 120.0
MAX_ATTEMPTS = 4
BACKOFF_SECONDS = (5.0, 10.0, 20.0)
MAX_RETRY_AFTER_SECONDS = 600.0
# A window whose pages do not hold its count is fetched once more, all of it,
# before the fetch stops: the data may have changed between two requests.
WINDOW_ATTEMPTS = 2
CACHE_INDEX_COLUMNS = (
    "request_url",
    "filename",
    "sha256",
    "size_bytes",
    "retrieved_at",
    "http_date",
    "content_type",
    "server",
)


def _user_agent() -> str:
    try:
        installed = version("research-data-store")
    except PackageNotFoundError:
        installed = "unknown"
    return f"research-store/{installed} (MSC GeoMet climate-hourly acquisition)"


def _header(headers: Any, name: str) -> str | None:
    if headers is None:
        return None
    value = headers.get(name)
    if value is None and hasattr(headers, "items"):
        for key, item in headers.items():
            if str(key).lower() == name.lower():
                value = item
                break
    return None if value is None else str(value)


def _utc_now() -> datetime:
    return datetime.now(UTC)


@dataclass(frozen=True, slots=True)
class _Response:
    url: str
    body: bytes
    retrieved_at: str
    http_date: str
    content_type: str
    server: str


class _Http:
    """Sequential, paced, retrying GETs with an append-only request log."""

    def __init__(
        self,
        *,
        opener: Callable[..., Any],
        sleep: Callable[[float], None],
        clock: Callable[[], float],
        now: Callable[[], datetime],
        min_interval_seconds: float,
        log: Callable[[dict[str, Any]], None],
    ):
        if min_interval_seconds < MIN_REQUEST_INTERVAL_SECONDS:
            raise ValueError(
                f"Requests to a public API must be at least "
                f"{MIN_REQUEST_INTERVAL_SECONDS:g} s apart, not {min_interval_seconds:g}"
            )
        self._opener = opener
        self._sleep = sleep
        self._clock = clock
        self._now = now
        self._min_interval = min_interval_seconds
        self._log = log
        self._last_finished: float | None = None
        self._user_agent = _user_agent()
        self.requests = 0

    def _pace(self) -> None:
        if self._last_finished is None:
            return
        wait = self._min_interval - (self._clock() - self._last_finished)
        if wait > 0:
            self._sleep(wait)

    def _back_off(self, attempt: int, headers: Any, url: str) -> None:
        delay = BACKOFF_SECONDS[min(attempt - 1, len(BACKOFF_SECONDS) - 1)]
        retry_after = _header(headers, "Retry-After")
        if retry_after and retry_after.strip().isdigit():
            requested = float(retry_after.strip())
            if requested > MAX_RETRY_AFTER_SECONDS:
                raise RuntimeError(
                    f"GeoMet asked to retry {url} after {requested:g} s; try later"
                )
            delay = max(delay, requested)
        self._sleep(delay)

    def get(self, url: str, *, expected_types: tuple[str, ...]) -> _Response:
        for attempt in range(1, MAX_ATTEMPTS + 1):
            self._pace()
            requested_at = self._now()
            request = urllib.request.Request(
                url, headers={"User-Agent": self._user_agent}, method="GET"
            )
            record: dict[str, Any] = {
                "requested_at": requested_at.strftime(RETRIEVED_AT_FORMAT),
                "url": url,
                "attempt": attempt,
            }
            headers: Any = None
            retryable = True
            try:
                self.requests += 1
                with self._opener(request, timeout=TIMEOUT_SECONDS) as response:
                    status = int(getattr(response, "status", 200))
                    headers = response.headers
                    body = response.read()
            except urllib.error.HTTPError as error:
                headers = error.headers
                retryable = error.code == 429 or 500 <= error.code < 600
                record.update(status=error.code, outcome="http_error", error=str(error))
                error.close()
                failure: str = f"HTTP {error.code} from GeoMet for {url}"
            except (urllib.error.URLError, http.client.HTTPException, OSError) as error:
                record.update(status=None, outcome="network_error", error=repr(error))
                failure = f"GeoMet request failed for {url}: {error!r}"
            else:
                failure = ""
                length = _header(headers, "Content-Length")
                encoding = (_header(headers, "Content-Encoding") or "identity").lower()
                content_type = _header(headers, "Content-Type") or ""
                media_type = content_type.split(";")[0].strip().lower()
                if status != 200:
                    retryable = status == 429 or 500 <= status < 600
                    failure = f"HTTP {status} from GeoMet for {url}"
                elif encoding != "identity":
                    retryable = False
                    failure = f"GeoMet sent Content-Encoding {encoding!r} for {url}"
                elif length is not None and int(length) != len(body):
                    failure = (
                        f"GeoMet transfer truncated for {url}: Content-Length "
                        f"{length} but {len(body)} bytes received"
                    )
                elif media_type not in expected_types:
                    retryable = False
                    failure = (
                        f"GeoMet returned {content_type!r} for {url}; expected "
                        f"{' or '.join(expected_types)}"
                    )
                record.update(
                    status=status,
                    bytes=len(body),
                    sha256=hashlib.sha256(body).hexdigest(),
                    outcome="ok" if not failure else "rejected",
                )
                if failure:
                    record["error"] = failure
            finally:
                self._last_finished = self._clock()
            self._log(record)
            if not failure:
                return _Response(
                    url=url,
                    body=body,
                    retrieved_at=requested_at.strftime(RETRIEVED_AT_FORMAT),
                    http_date=_header(headers, "Date") or "",
                    content_type=content_type,
                    server=(
                        _header(headers, "X-Powered-By")
                        or _header(headers, "Server")
                        or ""
                    ),
                )
            if not retryable or attempt == MAX_ATTEMPTS:
                raise RuntimeError(failure)
            self._back_off(attempt, headers, url)
        raise AssertionError("unreachable")


@dataclass(frozen=True, slots=True)
class _CacheEntry:
    request_url: str
    filename: str
    sha256: str
    size_bytes: int
    retrieved_at: str
    http_date: str
    content_type: str
    server: str


class _Cache:
    """Content-addressed responses plus an index from request URL to file."""

    def __init__(self, root: Path):
        self.root = root
        self.index_path = root / "cache-index.tsv"
        self.entries: dict[str, _CacheEntry] = {}
        if self.index_path.is_file():
            with self.index_path.open("r", encoding="utf-8", newline="") as stream:
                reader = csv.reader(stream, delimiter="\t", quoting=csv.QUOTE_NONE)
                header = next(reader, None)
                if tuple(header or ()) != CACHE_INDEX_COLUMNS:
                    raise ValueError(f"Unexpected cache index header in {self.index_path}")
                for row in reader:
                    item = dict(zip(CACHE_INDEX_COLUMNS, row, strict=True))
                    self.entries[item["request_url"]] = _CacheEntry(
                        **{**item, "size_bytes": int(item["size_bytes"])}
                    )

    def lookup(self, url: str) -> tuple[_CacheEntry, bytes] | None:
        entry = self.entries.get(url)
        if entry is None:
            return None
        path = self.root / entry.filename
        if not path.is_file():
            return None
        data = path.read_bytes()
        if hashlib.sha256(data).hexdigest() != entry.sha256:
            return None
        return entry, data

    def store(self, filename: str, response: _Response) -> _CacheEntry:
        digest = hashlib.sha256(response.body).hexdigest()
        path = self.root / filename
        if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != digest:
            atomic_write(path, response.body)
        entry = _CacheEntry(
            request_url=response.url,
            filename=filename,
            sha256=digest,
            size_bytes=len(response.body),
            retrieved_at=response.retrieved_at,
            http_date=response.http_date,
            content_type=response.content_type,
            server=response.server,
        )
        self.entries[response.url] = entry
        self._write_index()
        return entry

    def _write_index(self) -> None:
        lines = ["\t".join(CACHE_INDEX_COLUMNS)]
        for url in sorted(self.entries):
            entry = self.entries[url]
            lines.append(
                "\t".join(
                    str(getattr(entry, name)) for name in CACHE_INDEX_COLUMNS
                )
            )
        atomic_write(self.index_path, ("\n".join(lines) + "\n").encode("utf-8"))


@dataclass(frozen=True, slots=True)
class FetchResult:
    manifest: Path
    stations: int
    windows: int
    open_windows: int
    pages: int
    number_matched: int
    requests: int
    reused_responses: int

    def summary(self) -> dict[str, Any]:
        return {
            "manifest": str(self.manifest),
            "stations": self.stations,
            "windows": self.windows,
            "open_windows": self.open_windows,
            "pages": self.pages,
            "number_matched": self.number_matched,
            "http_requests": self.requests,
            "reused_responses": self.reused_responses,
        }


def fetch_collection(
    dataset_id: str,
    *,
    climate_ids: Iterable[str],
    ranges: Sequence[tuple[date | datetime | str, date | datetime | str]],
    registry: Registry,
    downloads: str | Path,
    refresh: bool = False,
    min_interval_seconds: float = MIN_REQUEST_INTERVAL_SECONDS,
    opener: Callable[..., Any] | None = None,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
    now: Callable[[], datetime] = _utc_now,
) -> FetchResult:
    """Download the responses for a selection and write its manifest.

    A cached response is reused without a request only when its window had
    settled by the time it was retrieved (see ``window_settled``): a window
    that could still gain hours, such as the current year, is asked for again
    on every fetch. `refresh` asks for every response again. A response whose
    bytes (for a count, whose ``numberMatched``) are unchanged keeps its
    original file and retrieval time, so an unchanged refresh reproduces the
    same manifest and the later ingest is a no-op; the one exception is a
    window that has settled since, whose unchanged response is recorded again
    with the retrieval time that makes it final.
    """

    spec = registry.get(dataset_id)
    spec.require_ready()
    if spec.producer != "geomet_climate_hourly":
        raise ValueError(f"{dataset_id!r} is not fetched from MSC GeoMet")
    api = api_options(spec)
    selected = windows(climate_ids, ranges)
    root = Path(downloads).expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    cache = _Cache(root)
    log_path = root / "fetch-log.jsonl"

    def log(record: dict[str, Any]) -> None:
        with log_path.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(record, sort_keys=True) + "\n")

    client = _Http(
        opener=opener or urllib.request.urlopen,
        sleep=sleep,
        clock=clock,
        now=now,
        min_interval_seconds=min_interval_seconds,
        log=log,
    )
    reused = 0

    def obtain(
        url: str, *, window: Window, role: str, offset: int, force: bool
    ) -> tuple[_CacheEntry, bytes]:
        nonlocal reused
        cached = cache.lookup(url)
        settled = cached is not None and window_settled(
            api, window, parse_retrieved_at(cached[0].retrieved_at)
        )
        if cached is not None and settled and not force:
            reused += 1
            return cached
        expected = ("text/csv",) if role == ROLE_PAGE else (
            "application/json",
            "application/geo+json",
        )
        response = client.get(url, expected_types=expected)
        if role == ROLE_PAGE:
            page_row_count(response.body, spec)
        else:
            hits_count(response.body)
        if cached is not None:
            entry, data = cached
            unchanged = data == response.body or (
                role == ROLE_HITS and hits_count(data) == hits_count(response.body)
            )
            now_settled = window_settled(
                api, window, parse_retrieved_at(response.retrieved_at)
            )
            if unchanged and (settled or not now_settled):
                log({"url": url, "outcome": "unchanged", "kept": entry.filename})
                reused += 1
                return cached
        digest = hashlib.sha256(response.body).hexdigest()
        suffix = "csv" if role == ROLE_PAGE else "json"
        name = f"o{offset}" if role == ROLE_PAGE else ROLE_HITS
        filename = (
            f"pages/{window.climate_id}/{window.climate_id}_{window.label}_"
            f"{name}_{digest[:12]}.{suffix}"
        )
        return cache.store(filename, response), response.body

    rows: list[dict[str, str]] = []
    totals: dict[str, int] = {}
    page_total = 0
    open_windows = 0
    for window in selected:
        for attempt in range(1, WINDOW_ATTEMPTS + 1):
            force = refresh or attempt > 1
            hits_entry, hits_body = obtain(
                hits_url(api, window), window=window, role=ROLE_HITS, offset=0, force=force
            )
            matched = hits_count(hits_body)
            pages: list[tuple[int, _CacheEntry]] = []
            received = 0
            for offset in range(0, matched, api.limit):
                entry, body = obtain(
                    page_url(api, window, offset),
                    window=window,
                    role=ROLE_PAGE,
                    offset=offset,
                    force=force,
                )
                count = page_row_count(body, spec)
                expected_rows = min(api.limit, matched - offset)
                received += count
                pages.append((offset, entry))
                if count != expected_rows:
                    break
            if received == matched and len(pages) == len(range(0, matched, api.limit)):
                break
            log(
                {
                    "outcome": "count_mismatch",
                    "climate_id": window.climate_id,
                    "window": window.label,
                    "number_matched": matched,
                    "rows_received": received,
                    "attempt": attempt,
                }
            )
            if attempt == WINDOW_ATTEMPTS:
                raise RuntimeError(
                    f"GeoMet pages for {window.climate_id} {window.label} hold "
                    f"{received} rows but numberMatched is {matched}, "
                    f"{WINDOW_ATTEMPTS} times; the window may be changing, try again later"
                )
        totals[window.climate_id] = totals.get(window.climate_id, 0) + matched
        open_windows += not window_settled(
            api, window, parse_retrieved_at(hits_entry.retrieved_at)
        )
        common = {
            "climate_id": window.climate_id,
            "window_start_lst": f"{window.start:{MANIFEST_TIME_FORMAT}}",
            "window_end_lst": f"{window.end:{MANIFEST_TIME_FORMAT}}",
            "number_matched": str(matched),
        }
        for role, offset, entry in [
            (ROLE_HITS, None, hits_entry),
            *[(ROLE_PAGE, offset, entry) for offset, entry in pages],
        ]:
            rows.append(
                {
                    **common,
                    "role": role,
                    "page_offset": "" if offset is None else str(offset),
                    "filename": entry.filename,
                    "size_bytes": str(entry.size_bytes),
                    "sha256": entry.sha256,
                    "request_url": entry.request_url,
                    "retrieved_at": entry.retrieved_at,
                    "http_date": entry.http_date,
                    "content_type": entry.content_type,
                    "server": entry.server,
                }
            )
        page_total += len(pages)

    unserved = sorted(station for station, total in totals.items() if total == 0)
    if unserved:
        raise ValueError(
            f"Climate ID(s) {unserved} have no records in the requested range; the "
            "station is not served by the climate-hourly collection or has no "
            "hourly data then"
        )
    manifest = write_manifest(root, spec, rows)
    return FetchResult(
        manifest=manifest,
        stations=len(totals),
        windows=len(selected),
        open_windows=open_windows,
        pages=page_total,
        number_matched=sum(totals.values()),
        requests=client.requests,
        reused_responses=reused,
    )
