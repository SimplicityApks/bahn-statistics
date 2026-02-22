"""
HTTP client for the public DB HAFAS REST API (v6.db.transport.rest).

Key design decisions:
- Use /stops/{id}/departures + /stops/{id}/arrivals joined by tripId.
  Do NOT use /journeys (omits S6) or /trips/{id} (returns HTTP 500).
- Delay values are in seconds (integer or null).
- Rate limit: comfortable margin below 100 req/min hard limit.
"""
import asyncio
import logging
import re
import time
import urllib.parse
from datetime import datetime
from typing import Optional

import httpx

from config import (
    API_BASE_URL,
    API_MIN_INTERVAL_S,
    API_TIMEOUT_S,
    ALLOWED_PRODUCTS,
    PRODUCT_PARAMS,
)

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Rate limiter
# ---------------------------------------------------------------------------
class RateLimiter:
    """
    Simple token-bucket enforcing a minimum interval between requests.

    The asyncio.Lock is created lazily and tied to the running event loop so
    this works correctly across multiple asyncio.run() calls (Python 3.9+).
    """

    def __init__(self, min_interval: float):
        self._min_interval = min_interval
        self._last_request: float = 0.0
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._lock: Optional[asyncio.Lock] = None

    def _get_lock(self) -> asyncio.Lock:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None
        if self._lock is None or self._loop is not loop:
            self._loop = loop
            self._lock = asyncio.Lock()
        return self._lock

    async def acquire(self) -> None:
        async with self._get_lock():
            now = time.monotonic()
            wait = self._min_interval - (now - self._last_request)
            if wait > 0:
                await asyncio.sleep(wait)
            self._last_request = time.monotonic()


_rate_limiter = RateLimiter(API_MIN_INTERVAL_S)


# ---------------------------------------------------------------------------
# Low-level HTTP helper
# ---------------------------------------------------------------------------
async def _get(
    client: httpx.AsyncClient,
    path: str,
    params: dict,
) -> dict:
    """Rate-limited GET with automatic retry on transient errors."""
    await _rate_limiter.acquire()
    attempt = 0
    backoff = 2.0
    while True:
        try:
            resp = await client.get(path, params=params, timeout=API_TIMEOUT_S)
            if resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", 60))
                log.warning("Rate-limited by API; sleeping %ds", retry_after)
                await asyncio.sleep(retry_after)
                continue
            resp.raise_for_status()
            return resp.json()
        except (httpx.TimeoutException, httpx.ConnectError, httpx.RemoteProtocolError) as exc:
            attempt += 1
            if attempt >= 4:
                log.error("GET %s failed after %d retries: %s", path, attempt, exc)
                raise
            log.warning("GET %s failed (attempt %d): %s — retrying in %.0fs", path, attempt, exc, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)


# ---------------------------------------------------------------------------
# Trip key extraction
# ---------------------------------------------------------------------------
_ZI_RE = re.compile(r"#ZI#([^#]+)#")
_DA_RE = re.compile(r"#DA#(\d{6})#")


def make_trip_key(trip_id: str) -> str:
    """
    Extract a stable dedup key from a HAFAS tripId.

    HAFAS tripIds encode the run number (#ZI#...#) and date (#DA#DDMMYY#).
    Combining these two yields a key that uniquely identifies a physical train
    run and is stable across multiple API queries.

    Falls back to the URL-decoded full tripId if parsing fails.
    """
    zi_match = _ZI_RE.search(trip_id)
    da_match = _DA_RE.search(trip_id)
    if zi_match and da_match:
        return f"ZI-{zi_match.group(1)}-DA-{da_match.group(1)}"
    # Fallback: use the raw tripId (URL-decoded for readability)
    try:
        decoded = urllib.parse.unquote(trip_id)
    except Exception:
        decoded = trip_id
    return decoded


# ---------------------------------------------------------------------------
# Delay extraction helpers
# ---------------------------------------------------------------------------
def extract_delay_seconds(entry: dict, field: str = "delay") -> Optional[int]:
    """
    Safely extract an integer delay value (in seconds) from a departure/arrival dict.

    The v6.db.transport.rest API uses the field name 'delay' for both departures
    and arrivals (NOT 'departureDelay'/'arrivalDelay'). The actual scheduled/real
    times are in 'plannedWhen' and 'when' respectively.

    Returns None if the field is absent or null.
    """
    value = entry.get(field)
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# Public API functions
# ---------------------------------------------------------------------------
async def verify_station(
    client: httpx.AsyncClient,
    station_id: str,
    expected_name: str,
) -> bool:
    """
    Query /locations to verify a station ID resolves to the expected name.
    Raises RuntimeError if the station cannot be confirmed.
    """
    try:
        data = await _get(client, "/locations", {"query": expected_name, "results": 10})
        for loc in data:
            if loc.get("id") == station_id:
                log.info(
                    "Station verified: %s (%s)", loc.get("name"), station_id
                )
                return True
        # Warn but don't crash — the API may have changed its ID format
        log.warning(
            "Station ID %s not found in /locations results for '%s'. "
            "Results: %s",
            station_id,
            expected_name,
            [loc.get("id") for loc in data[:5]],
        )
        return False
    except Exception as exc:
        log.error("Station verification failed for %s: %s", station_id, exc)
        return False


async def fetch_departures(
    client: httpx.AsyncClient,
    stop_id: str,
    when: datetime,
    duration_min: int,
    results: int = 200,
) -> list[dict]:
    """
    GET /stops/{stop_id}/departures

    Returns a list of raw departure dicts from the API,
    pre-filtered to ALLOWED_PRODUCTS.
    """
    params = {
        "when": when.isoformat(),
        "duration": duration_min,
        "results": results,
        **PRODUCT_PARAMS,
    }
    try:
        data = await _get(client, f"/stops/{stop_id}/departures", params)
        deps = data if isinstance(data, list) else data.get("departures", [])
        return [d for d in deps if _is_allowed(d)]
    except Exception as exc:
        log.error("fetch_departures(%s) failed: %s", stop_id, exc)
        return []


async def fetch_arrivals(
    client: httpx.AsyncClient,
    stop_id: str,
    when: datetime,
    duration_min: int,
    results: int = 200,
) -> list[dict]:
    """
    GET /stops/{stop_id}/arrivals

    Returns a list of raw arrival dicts from the API,
    pre-filtered to ALLOWED_PRODUCTS.
    """
    params = {
        "when": when.isoformat(),
        "duration": duration_min,
        "results": results,
        **PRODUCT_PARAMS,
    }
    try:
        data = await _get(client, f"/stops/{stop_id}/arrivals", params)
        arrs = data if isinstance(data, list) else data.get("arrivals", [])
        return [a for a in arrs if _is_allowed(a)]
    except Exception as exc:
        log.error("fetch_arrivals(%s) failed: %s", stop_id, exc)
        return []


async def fetch_journeys(
    client: httpx.AsyncClient,
    from_id: str,
    to_id: str,
    departure: datetime,
    results: int = 15,
) -> list[dict]:
    """
    GET /journeys?from=...&to=...&departure=...

    Used for routes where departure-board + arrival-board tripIds don't match
    across HAFAS split points (e.g. Aachen → Düsseldorf).

    Returns raw journey dicts (each containing a 'legs' list).
    Only direct connections (single-leg journeys) are returned.
    """
    params = {
        "from":      from_id,
        "to":        to_id,
        "departure": departure.isoformat(),
        "results":   results,
        **PRODUCT_PARAMS,
    }
    try:
        data = await _get(client, "/journeys", params)
        journeys = data.get("journeys", []) if isinstance(data, dict) else []
        # Keep only direct (single-leg) journeys
        return [j for j in journeys if len(j.get("legs", [])) == 1]
    except Exception as exc:
        log.error("fetch_journeys(%s→%s) failed: %s", from_id, to_id, exc)
        return []


def parse_journey_leg(journey: dict, route: str) -> Optional[dict]:
    """
    Convert a single-leg /journeys entry into a normalised journey dict.

    The /journeys endpoint uses 'departureDelay'/'arrivalDelay' field names
    in leg objects (unlike the departure/arrival boards which use 'delay').

    Returns None if essential fields are missing or the product is not allowed.
    """
    legs = journey.get("legs", [])
    if not legs:
        return None
    leg = legs[0]

    trip_id = leg.get("tripId") or leg.get("trip") or ""
    if not trip_id:
        return None

    planned_dep_str = leg.get("plannedDeparture") or leg.get("departure")
    planned_arr_str = leg.get("plannedArrival")   or leg.get("arrival")
    if not planned_dep_str:
        return None

    try:
        planned_dep_dt = datetime.fromisoformat(planned_dep_str)
        local_date = planned_dep_dt.strftime("%Y-%m-%d")
    except (ValueError, TypeError):
        return None

    line = leg.get("line") or {}
    product  = line.get("product") or ""
    line_name = (line.get("name") or "").replace(" ", "")

    if product not in ALLOWED_PRODUCTS:
        return None

    # /journeys legs use departureDelay / arrivalDelay (may also have 'delay')
    dep_delay = leg.get("departureDelay")
    if dep_delay is None:
        dep_delay = leg.get("delay")
    arr_delay = leg.get("arrivalDelay")

    cancelled = bool(journey.get("cancelled") or leg.get("cancelled"))

    return {
        "trip_key":   make_trip_key(trip_id),
        "trip_id":    trip_id,
        "date":       local_date,
        "route":      route,
        "line_name":  line_name,
        "product":    product,
        "planned_dep": planned_dep_str,
        "planned_arr": planned_arr_str,
        "dep_delay_s": int(dep_delay) if dep_delay is not None else None,
        "arr_delay_s": int(arr_delay) if arr_delay is not None else None,
        "cancelled":   cancelled,
        "data_stage":  "initial",
    }


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------
def _get_product(entry: dict) -> str:
    """Safely extract line.product from a departure/arrival dict."""
    try:
        return entry["line"]["product"]
    except (KeyError, TypeError):
        return ""


def _is_allowed(entry: dict) -> bool:
    """
    Return True if this departure/arrival should be recorded.
    - RE/RB trains (regional, regionalExp): all allowed.
    - Suburban (S-Bahn): ONLY S6 — not S1, S2, S3, S9, S11, etc.
    - ICE/IC (national, nationalExpress): never allowed.
    """
    product = _get_product(entry)
    if product not in ALLOWED_PRODUCTS:
        return False
    if product == "suburban":
        name = _get_line_name(entry)  # normalised: "S6", "S1", etc.
        return name == "S6"
    return True


def _get_line_name(entry: dict) -> str:
    """Safely extract line.name (e.g. 'S 6', 'RE 2')."""
    try:
        name = entry["line"]["name"] or ""
        # Normalise 'S 6' -> 'S6', 'RE 2' -> 'RE2' etc.
        return name.replace(" ", "")
    except (KeyError, TypeError):
        return "?"


def parse_departure(dep: dict, route: str, direction_hint: str = "") -> Optional[dict]:
    """
    Convert a raw /departures entry into a normalised journey dict ready for upsert.

    The direction_hint is used as a SOFT hint only: if we have arrival-side data
    the join is the authoritative filter. direction_hint is checked only when we
    want to pre-filter obvious wrong-direction trains (e.g. southbound from Essen).

    Returns None only if essential fields (tripId, plannedWhen) are missing.
    """

    trip_id = dep.get("tripId") or dep.get("trip") or ""
    if not trip_id:
        log.debug("Skipping departure with no tripId: %s", dep.get("line"))
        return None

    trip_key = make_trip_key(trip_id)
    planned_dep_str = dep.get("plannedWhen") or dep.get("when")
    if not planned_dep_str:
        return None

    # Parse local date from planned departure
    try:
        planned_dep_dt = datetime.fromisoformat(planned_dep_str)
        local_date = planned_dep_dt.strftime("%Y-%m-%d")
    except (ValueError, TypeError):
        return None

    return {
        "trip_key": trip_key,
        "trip_id": trip_id,
        "date": local_date,
        "route": route,
        "line_name": _get_line_name(dep),
        "product": _get_product(dep),
        "planned_dep": planned_dep_str,
        "planned_arr": None,  # filled in when matched with arrival
        "dep_delay_s": extract_delay_seconds(dep, "delay"),
        "arr_delay_s": None,  # filled in when matched with arrival
        "cancelled": bool(dep.get("cancelled")),
        "data_stage": "initial",
    }


def build_arrival_index(arrivals: list[dict]) -> dict[str, dict]:
    """
    Build a {trip_key: arrival_dict} index from raw arrival entries.
    If multiple arrivals share a trip_key, keep the most informative one.
    """
    index: dict[str, dict] = {}
    for arr in arrivals:
        trip_id = arr.get("tripId") or arr.get("trip") or ""
        if not trip_id:
            continue
        key = make_trip_key(trip_id)
        existing = index.get(key)
        if existing is None:
            index[key] = arr
        else:
            # Prefer entry with delay data
            if arr.get("arrivalDelay") is not None and existing.get("arrivalDelay") is None:
                index[key] = arr
    return index


def enrich_with_arrival(journey: dict, arrival: dict) -> bool:
    """
    Merge arrival data into a journey dict (in-place).

    Returns True if the arrival is valid (arr time > dep time).
    Returns False if the arrival is chronologically impossible — which indicates
    the arrival is from the SAME train on a DIFFERENT segment of its journey
    (e.g. a northbound RE6 arrives Düsseldorf at 19:47 and later departs
    Essen at 20:29 going northbound; the tripId collides with a southbound leg).
    """
    planned_arr_str = arrival.get("plannedWhen") or arrival.get("when")
    if not planned_arr_str:
        return False

    # Sanity check: arrival must be AFTER departure
    try:
        dep_dt = datetime.fromisoformat(journey["planned_dep"])
        arr_dt = datetime.fromisoformat(planned_arr_str)
        if arr_dt <= dep_dt:
            log.debug(
                "Rejected reversed arrival match for %s: dep=%s arr=%s",
                journey.get("trip_key"), journey["planned_dep"], planned_arr_str,
            )
            return False
        # Also reject if travel time is unreasonably long (> 90 min for any dep+arr route)
        if (arr_dt - dep_dt).total_seconds() > 90 * 60:
            log.debug("Rejected implausible travel time for %s", journey.get("trip_key"))
            return False
    except (ValueError, TypeError, KeyError):
        return False

    journey["planned_arr"] = planned_arr_str
    journey["arr_delay_s"] = extract_delay_seconds(arrival, "delay")
    return True
