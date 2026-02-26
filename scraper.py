"""
Scraping daemon for the DB Bahn statistics scraper.

Polling strategy:
  - During active windows (morning 06:45–09:30, evening 16:45–20:30 LOCAL time):
    poll every 5 minutes.
  - Outside windows: sleep until the next window starts.

Two-stage delay capture:
  Stage 1 (initial): Fetch all departures/arrivals for the full window.
    Captures schedule info; delay fields may be null for future trains.
  Stage 2 (refresh): For trains departing within REFRESH_LOOKAHEAD_MIN,
    re-query a narrow departure window to capture live delay data.

Timezone note:
  All window times (config.py) are LOCAL Germany time. Scheduling uses
  datetime.now() (local naive). API calls receive UTC-aware timestamps.
"""
import asyncio
import logging
import signal
import sqlite3
import time as time_mod
from datetime import datetime, time, timedelta, timezone

import httpx

from api import (
    build_arrival_index,
    enrich_with_arrival,
    fetch_arrivals,
    fetch_departures,
    fetch_journeys,
    make_trip_key,
    parse_departure,
    parse_journey_leg,
    verify_station,
)
from config import (
    ACTIVE_BUFFER_AFTER_MIN,
    ACTIVE_BUFFER_BEFORE_MIN,
    API_BASE_URL,
    DEPARTURES_DURATION_MIN,
    POLL_INTERVAL_ACTIVE_S,
    POLL_INTERVAL_IDLE_MAX_S,
    REFRESH_LOOKAHEAD_MIN,
    ROUTES,
)
from database import get_pending_refresh, log_scrape_run, upsert_journey

log = logging.getLogger(__name__)

# Global shutdown flag set by SIGTERM/SIGINT
_shutdown = False


def _install_signal_handlers() -> None:
    def _handle(signum, frame):  # noqa: ARG001
        global _shutdown
        log.info("Signal %d received — shutting down after current cycle", signum)
        _shutdown = True

    signal.signal(signal.SIGTERM, _handle)
    signal.signal(signal.SIGINT, _handle)


# ---------------------------------------------------------------------------
# Window helpers  (all times are LOCAL naive datetimes)
# ---------------------------------------------------------------------------
def _window_bounds_local(route_cfg: dict) -> tuple[time, time]:
    """Return the active polling window (with buffer) as local time objects."""
    ws: time = route_cfg["window_start"]
    we: time = route_cfg["window_end"]
    ws_s = ws.hour * 3600 + ws.minute * 60 - ACTIVE_BUFFER_BEFORE_MIN * 60
    we_s = we.hour * 3600 + we.minute * 60 + ACTIVE_BUFFER_AFTER_MIN * 60
    # Clamp to 0..86399 seconds
    ws_s = max(0, ws_s)
    we_s = min(86399, we_s)
    return time(ws_s // 3600, (ws_s % 3600) // 60), time(we_s // 3600, (we_s % 3600) // 60)


def is_active_window(now_local: datetime, route_cfg: dict) -> bool:
    """Return True if now_local (naive local time) is in the active window."""
    t = now_local.time().replace(second=0, microsecond=0)
    ws, we = _window_bounds_local(route_cfg)
    return ws <= t <= we


def next_wake_seconds(now_local: datetime) -> float:
    """
    Seconds to sleep before the next action.
    Returns POLL_INTERVAL_ACTIVE_S if any window is currently active.
    Otherwise returns seconds until the nearest window start, capped at
    POLL_INTERVAL_IDLE_MAX_S.
    """
    for route_cfg in ROUTES.values():
        if is_active_window(now_local, route_cfg):
            return float(POLL_INTERVAL_ACTIVE_S)

    t_now = now_local.time()
    min_wait_s = float(POLL_INTERVAL_IDLE_MAX_S)

    for route_cfg in ROUTES.values():
        ws_buffered, _ = _window_bounds_local(route_cfg)
        t_ws = ws_buffered.hour * 3600 + ws_buffered.minute * 60
        t_cur = t_now.hour * 3600 + t_now.minute * 60 + t_now.second
        if t_ws > t_cur:
            wait_s = float(t_ws - t_cur)
            min_wait_s = min(min_wait_s, wait_s)

    return max(30.0, min_wait_s)


def _local_window_start_utc(route_cfg: dict) -> datetime:
    """
    Return the (naive UTC) datetime for the window start today,
    using the current local timezone offset.
    """
    now_local = datetime.now()
    now_utc = datetime.now(timezone.utc).replace(tzinfo=None)
    utc_offset_s = (now_local - now_utc).total_seconds()

    ws: time = route_cfg["window_start"]
    # Build local datetime for today's window start
    local_ws = now_local.replace(
        hour=ws.hour, minute=ws.minute, second=0, microsecond=0
    )
    # Convert to UTC naive
    return local_ws - timedelta(seconds=utc_offset_s)


# ---------------------------------------------------------------------------
# Single-route scrape cycle
# ---------------------------------------------------------------------------
async def _scrape_route_stage1(
    client: httpx.AsyncClient,
    conn: sqlite3.Connection,
    route_name: str,
    route_cfg: dict,
) -> int:
    """
    Stage 1: Fetch departures + arrivals for the full window and upsert.

    Filter logic:
    - Any departure with a matching arrival (same tripId) at the destination
      is definitively included — the join IS the route filter.
    - A departure without an arrival match is included only if its direction
      field suggests it passes through the destination (soft fallback).
    - This correctly handles RE trains whose final destination is beyond Essen
      (e.g. RE2 to Dortmund): they appear in Essen arrivals board, so the join
      picks them up even though direction says "Dortmund".

    Returns the number of journey rows upserted.
    """
    now_utc = datetime.now(timezone.utc)
    now_local = datetime.now()

    ws: time = route_cfg["window_start"]
    we: time = route_cfg["window_end"]
    direction_hint: str = route_cfg["direction_hint"]
    from_id = route_cfg["from_id"]
    to_id = route_cfg["to_id"]

    # Determine query start: beginning of window (with buffer), or "now - 5 min"
    # if we are already past the window start.
    local_ws_today = now_local.replace(
        hour=ws.hour, minute=ws.minute, second=0, microsecond=0
    )
    if now_local > local_ws_today + timedelta(minutes=ACTIVE_BUFFER_BEFORE_MIN):
        # We are inside or past the window — start from a few minutes ago
        query_start_local = now_local - timedelta(minutes=5)
    else:
        query_start_local = local_ws_today - timedelta(minutes=ACTIVE_BUFFER_BEFORE_MIN)

    # Convert to UTC-aware for API
    utc_offset = now_utc.replace(tzinfo=None) - now_local
    query_start_utc = (query_start_local + utc_offset).replace(
        tzinfo=timezone.utc
    )

    log.debug(
        "[%s] Stage-1: query from %s (local %s), duration %d min",
        route_name,
        query_start_utc.isoformat(),
        query_start_local.strftime("%H:%M"),
        DEPARTURES_DURATION_MIN,
    )

    # Fetch departures and arrivals concurrently
    deps, arrs = await asyncio.gather(
        fetch_departures(client, from_id, query_start_utc, DEPARTURES_DURATION_MIN),
        fetch_arrivals(client, to_id, query_start_utc, DEPARTURES_DURATION_MIN),
    )

    arrival_index = build_arrival_index(arrs)
    upserted = 0
    skipped_no_match = 0

    for dep in deps:
        journey = parse_departure(dep, route_name)
        if journey is None:
            continue

        trip_key = journey["trip_key"]
        arr = arrival_index.get(trip_key)

        if arr:
            # Confirmed via arrival join: this train passes through the destination
            valid = enrich_with_arrival(journey, arr)
            if not valid:
                # Reversed/impossible arrival time means wrong-direction train
                # Fall through to the direction_hint soft-check below
                arr = None

        if not arr:
            # No arrival match yet — use direction_hint as soft fallback
            direction = dep.get("direction") or ""
            if direction_hint and direction_hint.lower() not in direction.lower():
                skipped_no_match += 1
                continue
            # Direction matches, include without arrival data (will be refreshed later)

        upsert_journey(conn, journey)
        upserted += 1

    log.info(
        "[%s] Stage-1: %d departures, %d arrivals → %d upserted "
        "(%d arrival-joined, %d skipped no-match)",
        route_name,
        len(deps),
        len(arrs),
        upserted,
        sum(1 for d in deps
            if (j := parse_departure(d, route_name)) and arrival_index.get(j["trip_key"])),
        skipped_no_match,
    )
    return upserted


async def _scrape_route_stage1_journeys(
    client: httpx.AsyncClient,
    conn: sqlite3.Connection,
    route_name: str,
    route_cfg: dict,
) -> int:
    """
    Stage 1 for routes using the /journeys planner (Aachen ↔ Düsseldorf).

    Queries /journeys for direct connections across the full window.
    The planner resolves through-journeys correctly even when HAFAS changes
    the tripId at intermediate split-point stations.
    """
    now_local = datetime.now()
    ws: time = route_cfg["window_start"]
    we: time = route_cfg["window_end"]
    from_id = route_cfg["from_id"]
    to_id   = route_cfg["to_id"]
    max_legs        = route_cfg.get("max_legs", 1)
    max_travel_min  = route_cfg.get("max_travel_min", 90)
    extra_api_params = route_cfg.get("extra_api_params") or {}

    local_ws_today = now_local.replace(
        hour=ws.hour, minute=ws.minute, second=0, microsecond=0
    )
    if now_local > local_ws_today + timedelta(minutes=ACTIVE_BUFFER_BEFORE_MIN):
        query_start_local = now_local - timedelta(minutes=5)
    else:
        query_start_local = local_ws_today - timedelta(minutes=ACTIVE_BUFFER_BEFORE_MIN)

    # Convert to UTC-aware
    now_utc = datetime.now(timezone.utc)
    utc_offset = now_utc.replace(tzinfo=None) - now_local
    query_start_utc = (query_start_local + utc_offset).replace(tzinfo=timezone.utc)

    log.debug(
        "[%s] Stage-1 (journeys): query from %s (local %s)",
        route_name, query_start_utc.isoformat(), query_start_local.strftime("%H:%M"),
    )

    # The window covers ~2h 30min; fetch in two overlapping passes to cover it all
    # (the /journeys endpoint returns ~15 results per call, ~hourly frequency = 2-3 trains)
    journeys_pass1 = await fetch_journeys(
        client, from_id, to_id, query_start_utc, results=20,
        max_legs=max_legs, extra_params=extra_api_params or None,
    )

    # Second pass: offset by half the window to catch later trains
    half_window = timedelta(minutes=(we.hour * 60 + we.minute - ws.hour * 60 - ws.minute) // 2)
    query_start_pass2 = query_start_utc + half_window
    journeys_pass2 = await fetch_journeys(
        client, from_id, to_id, query_start_pass2, results=20,
        max_legs=max_legs, extra_params=extra_api_params or None,
    )

    upserted = 0
    seen_keys: set[str] = set()

    for j_raw in journeys_pass1 + journeys_pass2:
        journey = parse_journey_leg(j_raw, route_name, max_travel_min=max_travel_min)
        if journey is None:
            continue
        # Deduplicate within this cycle
        key = (journey["trip_key"], journey["date"])
        if key in seen_keys:
            continue
        seen_keys.add(key)
        # Only keep trains within the window (with buffer)
        try:
            dep_dt = datetime.fromisoformat(journey["planned_dep"])
            # planned_dep is already in local timezone (CET +01:00 / CEST +02:00);
            # strip tz-info to get a naive local time for comparison.
            # Do NOT subtract utc_offset here — that would add an extra hour.
            dep_local = dep_dt.replace(tzinfo=None)
            ws_buffered = local_ws_today.replace(
                hour=ws.hour, minute=ws.minute
            ) - timedelta(minutes=ACTIVE_BUFFER_BEFORE_MIN)
            we_buffered = local_ws_today.replace(
                hour=we.hour, minute=we.minute
            ) + timedelta(minutes=ACTIVE_BUFFER_AFTER_MIN)
            if not (ws_buffered <= dep_local <= we_buffered):
                continue
        except (ValueError, TypeError):
            pass

        upsert_journey(conn, journey)
        upserted += 1

    log.info(
        "[%s] Stage-1 (journeys): %d+%d raw → %d upserted",
        route_name, len(journeys_pass1), len(journeys_pass2), upserted,
    )
    return upserted


async def _scrape_route_stage2_journeys(
    client: httpx.AsyncClient,
    conn: sqlite3.Connection,
    route_name: str,
    route_cfg: dict,
) -> int:
    """Stage 2 refresh for journeys-based routes: re-query /journeys for imminent trains."""
    pending = get_pending_refresh(conn, route_name, within_minutes=REFRESH_LOOKAHEAD_MIN)
    if not pending:
        return 0

    from_id = route_cfg["from_id"]
    to_id   = route_cfg["to_id"]
    max_legs        = route_cfg.get("max_legs", 1)
    max_travel_min  = route_cfg.get("max_travel_min", 90)
    extra_api_params = route_cfg.get("extra_api_params") or {}
    refreshed = 0

    for row in pending:
        try:
            planned_dep_dt = datetime.fromisoformat(row["planned_dep"])
            if planned_dep_dt.tzinfo is None:
                planned_dep_dt = planned_dep_dt.replace(tzinfo=timezone.utc)
        except (ValueError, KeyError):
            continue

        query_start = planned_dep_dt - timedelta(minutes=5)
        journeys = await fetch_journeys(
            client, from_id, to_id, query_start, results=5,
            max_legs=max_legs, extra_params=extra_api_params or None,
        )

        for j_raw in journeys:
            journey = parse_journey_leg(j_raw, route_name, max_travel_min=max_travel_min)
            if journey is None:
                continue
            if journey["trip_key"] != row["trip_key"]:
                continue
            journey["data_stage"] = "refreshed"
            upsert_journey(conn, journey)
            refreshed += 1
            break

    log.info("[%s] Stage-2 (journeys): refreshed %d/%d rows", route_name, refreshed, len(pending))
    return refreshed


async def _scrape_route_stage2(
    client: httpx.AsyncClient,
    conn: sqlite3.Connection,
    route_name: str,
    route_cfg: dict,
) -> int:
    """
    Stage 2: Re-query trains departing soon to capture live delay data.
    Returns the number of rows refreshed.
    """
    pending = get_pending_refresh(conn, route_name, within_minutes=REFRESH_LOOKAHEAD_MIN)
    if not pending:
        return 0

    log.debug("[%s] Stage-2: %d rows pending refresh", route_name, len(pending))

    from_id = route_cfg["from_id"]
    to_id = route_cfg["to_id"]
    refreshed = 0

    for row in pending:
        try:
            planned_dep_dt = datetime.fromisoformat(row["planned_dep"])
            # Ensure UTC-aware
            if planned_dep_dt.tzinfo is None:
                planned_dep_dt = planned_dep_dt.replace(tzinfo=timezone.utc)
        except (ValueError, KeyError):
            continue

        # Narrow query: ±8-minute window around the planned departure
        query_start = planned_dep_dt - timedelta(minutes=8)

        deps, arrs = await asyncio.gather(
            fetch_departures(client, from_id, query_start, duration_min=20, results=30),
            fetch_arrivals(client, to_id, query_start, duration_min=20, results=30),
        )

        arrival_index = build_arrival_index(arrs)
        target_key = row["trip_key"]

        for dep in deps:
            journey = parse_departure(dep, route_name)
            if journey is None:
                continue
            if journey["trip_key"] != target_key:
                continue  # not the train we're looking for
            journey["data_stage"] = "refreshed"
            arr = arrival_index.get(target_key)
            if arr:
                enrich_with_arrival(journey, arr)  # OK even if returns False; delay data still useful
            upsert_journey(conn, journey)
            refreshed += 1
            break

    log.info("[%s] Stage-2: refreshed %d/%d rows", route_name, refreshed, len(pending))
    return refreshed


async def scrape_route(
    client: httpx.AsyncClient,
    conn: sqlite3.Connection,
    route_name: str,
    route_cfg: dict,
) -> dict:
    """Run a full scrape cycle (stage 1 + stage 2) for a single route."""
    t0 = time_mod.monotonic()
    errors = 0
    upserted = 0

    use_journeys = route_cfg.get("use_journeys", False)

    try:
        if use_journeys:
            upserted = await _scrape_route_stage1_journeys(client, conn, route_name, route_cfg)
        else:
            upserted = await _scrape_route_stage1(client, conn, route_name, route_cfg)
    except Exception as exc:
        log.error("[%s] Stage-1 failed: %s", route_name, exc, exc_info=True)
        errors += 1

    try:
        if use_journeys:
            await _scrape_route_stage2_journeys(client, conn, route_name, route_cfg)
        else:
            await _scrape_route_stage2(client, conn, route_name, route_cfg)
    except Exception as exc:
        log.error("[%s] Stage-2 failed: %s", route_name, exc, exc_info=True)
        errors += 1

    duration = time_mod.monotonic() - t0
    run = {
        "started_at": datetime.now(timezone.utc).isoformat(),
        "route": route_name,
        "journeys_upserted": upserted,
        "errors": errors,
        "duration_s": round(duration, 2),
    }
    log_scrape_run(conn, run)
    return run


# ---------------------------------------------------------------------------
# Main daemon and single-shot entry points
# ---------------------------------------------------------------------------
async def _verify_stations(client: httpx.AsyncClient) -> None:
    """Verify every unique station ID referenced by any route."""
    seen: dict[str, str] = {}
    for cfg in ROUTES.values():
        seen[cfg["from_id"]] = cfg["from_name"]
        seen[cfg["to_id"]]   = cfg["to_name"]
    await asyncio.gather(
        *(verify_station(client, sid, name) for sid, name in seen.items())
    )


async def scrape_once(conn: sqlite3.Connection) -> None:
    """Run one scrape cycle for ALL routes regardless of time window, then exit."""
    async with httpx.AsyncClient(base_url=API_BASE_URL) as client:
        await _verify_stations(client)
        for route_name, route_cfg in ROUTES.items():
            log.info("Single-shot scrape: route=%s", route_name)
            await scrape_route(client, conn, route_name, route_cfg)


async def run_daemon(conn: sqlite3.Connection) -> None:
    """
    Continuous polling daemon.
    - Polls every 5 min during active windows (local time).
    - Sleeps until the next window otherwise.
    - Exits cleanly on SIGTERM / SIGINT.
    """
    _install_signal_handlers()

    async with httpx.AsyncClient(base_url=API_BASE_URL) as client:
        await _verify_stations(client)
        log.info("Daemon started. Monitoring routes: %s", list(ROUTES.keys()))

        while not _shutdown:
            now_local = datetime.now()
            any_active = False

            for route_name, route_cfg in ROUTES.items():
                if is_active_window(now_local, route_cfg):
                    any_active = True
                    log.info(
                        "Active window open: route=%s — starting scrape cycle", route_name
                    )
                    await scrape_route(client, conn, route_name, route_cfg)

            sleep_s = next_wake_seconds(datetime.now())
            if not any_active:
                log.info("No active window. Sleeping %.0fs until next window.", sleep_s)

            # Sleep in small chunks to react to SIGTERM promptly
            deadline = time_mod.monotonic() + sleep_s
            while not _shutdown and time_mod.monotonic() < deadline:
                await asyncio.sleep(min(10.0, deadline - time_mod.monotonic()))

    log.info("Daemon stopped.")
