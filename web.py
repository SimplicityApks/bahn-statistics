"""FastAPI web dashboard for DB Bahn commute statistics."""
import sqlite3
import statistics
from collections import defaultdict
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse

_HTML_FILE = Path(__file__).parent / "dashboard.html"

from config import DB_PATH
from database import get_journeys_for_stats
from stats import (
    ROUTE_LABELS,
    ROUTE_ORDER,
    _ROUTE_SHORT,
    compute_slot_stats,
    compute_stats,
    get_date_range,
    rank_recommendations,
)

# ---------------------------------------------------------------------------
# Map configuration
# ---------------------------------------------------------------------------
STATION_COORDS: dict = {
    "DDF": {"lat": 51.2195, "lon": 6.7942, "name": "Düsseldorf Hbf"},
    "ESS": {"lat": 51.4508, "lon": 7.0131, "name": "Essen Hbf"},
    "AAH": {"lat": 50.7681, "lon": 6.0910, "name": "Aachen Hbf"},
    "WUP": {"lat": 51.2543, "lon": 7.1498, "name": "Wuppertal Hbf"},
    "BON": {"lat": 50.7320, "lon": 7.0990, "name": "Bonn Hbf"},
}

# (origin_id, destination_id) for each route
ROUTE_STATIONS: dict = {
    "morning":           ("DDF", "ESS"),
    "evening":           ("ESS", "DDF"),
    "aachen_morning":    ("AAH", "DDF"),
    "aachen_evening":    ("DDF", "AAH"),
    "wuppertal_morning": ("WUP", "DDF"),
    "wuppertal_evening": ("DDF", "WUP"),
    "bonn_morning":      ("BON", "DDF"),
    "bonn_evening":      ("DDF", "BON"),
}


# ---------------------------------------------------------------------------
# Lifespan — open/close DB connection
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    app.state.conn = conn
    yield
    conn.close()


app = FastAPI(title="Bahn Statistics", lifespan=lifespan)


# ---------------------------------------------------------------------------
# Histogram config
# ---------------------------------------------------------------------------
HIST_BREAKPOINTS = [0, 60, 120, 180, 240, 300, 360, 420, 480, 600, 900, 1800, float("inf")]
HIST_LABELS = ["<1", "1-2", "2-3", "3-4", "4-5", "5-6", "6-7", "7-8", "8-10", "10-15", "15-30", "30+"]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
_MIN_COUNT = 5  # mirror stats.py rank_recommendations threshold

def _strip_stats(stats: dict) -> dict:
    """Remove bulk list fields and low-count lines from compute_stats output."""
    result: dict = {}
    for route, lines in stats.items():
        filtered = {
            line: {k: v for k, v in data.items()
                   if k not in ("dep_delays", "arr_delays", "journey_deltas", "travel_pairs")}
            for line, data in lines.items()
            if data["count"] >= _MIN_COUNT
        }
        if filtered:
            result[route] = filtered
    return result


def _build_trend(journeys: list[dict], route: str) -> dict:
    """Group by (date, line), produce aligned mean-arr-delay arrays for Chart.js."""
    by_date_line: dict = defaultdict(lambda: defaultdict(list))
    for j in journeys:
        if j.get("route") != route:
            continue
        if j.get("cancelled") or j.get("arr_delay_s") is None:
            continue
        date = j.get("date", "")
        line = j.get("line_name", "")
        by_date_line[date][line].append(float(j["arr_delay_s"]))

    all_dates = sorted(by_date_line.keys())
    all_lines: set = set()
    line_counts: dict = defaultdict(int)
    for date_data in by_date_line.values():
        for line, vals in date_data.items():
            all_lines.add(line)
            line_counts[line] += len(vals)
    sorted_lines = sorted(l for l in all_lines if line_counts[l] >= _MIN_COUNT)

    series: dict = {}
    for line in sorted_lines:
        vals = []
        for date in all_dates:
            delays = by_date_line[date].get(line)
            if delays:
                vals.append(round(statistics.mean(delays) / 60, 2))
            else:
                vals.append(None)
        series[line] = vals

    return {"dates": all_dates, "series": series}


def _build_histogram(
    journeys: list[dict],
    route: Optional[str] = None,
    line: Optional[str] = None,
) -> dict:
    """Bucket arr_delay_s into HIST_BREAKPOINTS."""
    filtered = [
        j for j in journeys
        if not j.get("cancelled")
        and j.get("arr_delay_s") is not None
        and (route is None or j.get("route") == route)
        and (line is None or j.get("line_name") == line)
    ]

    counts = [0] * len(HIST_LABELS)
    for j in filtered:
        delay_s = float(j["arr_delay_s"])
        for i in range(len(HIST_BREAKPOINTS) - 1):
            lo = HIST_BREAKPOINTS[i]
            hi = HIST_BREAKPOINTS[i + 1]
            if lo <= delay_s < hi:
                counts[i] += 1
                break

    return {"labels": HIST_LABELS, "counts": counts, "total": len(filtered)}


# ---------------------------------------------------------------------------
# API endpoints
# ---------------------------------------------------------------------------
@app.get("/api/summary")
async def api_summary(
    request: Request,
    since: Optional[str] = Query(None),
    route: Optional[str] = Query(None),
):
    conn: sqlite3.Connection = request.app.state.conn
    journeys = get_journeys_for_stats(conn, route=route, since_date=since)

    stats = compute_stats(journeys)
    recommendations = rank_recommendations(stats)
    date_range = get_date_range(journeys)

    return {
        "stats": _strip_stats(stats),
        "recommendations": recommendations,
        "date_range": list(date_range) if date_range else None,
        "total": len(journeys),
    }


@app.get("/api/slots")
async def api_slots(
    request: Request,
    since: Optional[str] = Query(None),
    route: Optional[str] = Query(None),
):
    conn: sqlite3.Connection = request.app.state.conn
    journeys = get_journeys_for_stats(conn, route=route, since_date=since)
    slot_stats = compute_slot_stats(journeys)

    # Count qualifying journeys per (route, line) — same filter compute_slot_stats uses
    counts: dict = defaultdict(lambda: defaultdict(int))
    for j in journeys:
        if not j.get("cancelled") and j.get("arr_delay_s") is not None:
            counts[j["route"]][j["line_name"]] += 1

    # Drop lines below the threshold; drop routes that become empty
    return {
        r: {line: slots for line, slots in lines.items() if counts[r][line] >= _MIN_COUNT}
        for r, lines in slot_stats.items()
        if any(counts[r][line] >= _MIN_COUNT for line in lines)
    }


@app.get("/api/trend")
async def api_trend(
    request: Request,
    route: str = Query("morning"),
    since: Optional[str] = Query(None),
):
    conn: sqlite3.Connection = request.app.state.conn
    journeys = get_journeys_for_stats(conn, since_date=since)
    return _build_trend(journeys, route)


@app.get("/api/histogram")
async def api_histogram(
    request: Request,
    route: Optional[str] = Query(None),
    line: Optional[str] = Query(None),
    since: Optional[str] = Query(None),
):
    conn: sqlite3.Connection = request.app.state.conn
    journeys = get_journeys_for_stats(conn, since_date=since)
    return _build_histogram(journeys, route=route, line=line)


@app.get("/api/recent")
async def api_recent(
    request: Request,
    route: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=500),
):
    conn: sqlite3.Connection = request.app.state.conn
    journeys = get_journeys_for_stats(conn, route=route)
    # Sort descending by date + planned_dep, return display fields only
    journeys.sort(key=lambda j: (j.get("date", ""), j.get("planned_dep", "")), reverse=True)
    display_fields = (
        "id", "date", "route", "line_name", "product",
        "planned_dep", "planned_arr", "dep_delay_s", "arr_delay_s",
        "cancelled", "data_stage",
    )
    return [
        {k: j.get(k) for k in display_fields}
        for j in journeys[:limit]
    ]


@app.get("/api/map")
async def api_map(
    request: Request,
    since: Optional[str] = Query(None),
):
    conn: sqlite3.Connection = request.app.state.conn
    journeys = get_journeys_for_stats(conn, since_date=since)

    # Per-(route, line) accumulators
    rl_dep: dict = defaultdict(list)   # (route, line) -> [dep_delay_s, ...]
    rl_arr: dict = defaultdict(list)   # (route, line) -> [arr_delay_s, ...]

    # Per-station accumulators (as origin / as destination)
    station_dep: dict = defaultdict(list)
    station_arr: dict = defaultdict(list)

    for j in journeys:
        if j.get("cancelled"):
            continue
        route = j.get("route")
        if route not in ROUTE_STATIONS:
            continue

        from_id, to_id = ROUTE_STATIONS[route]
        line = j.get("line_name", "?")
        key = (route, line)

        dep_delay = j.get("dep_delay_s")
        arr_delay = j.get("arr_delay_s")

        if dep_delay is not None:
            rl_dep[key].append(float(dep_delay))
            station_dep[from_id].append(float(dep_delay))
        if arr_delay is not None:
            rl_arr[key].append(float(arr_delay))
            station_arr[to_id].append(float(arr_delay))

    # Build routes payload — nested per train line, filtered to _MIN_COUNT
    routes_out: dict = {}
    for route, (from_id, to_id) in ROUTE_STATIONS.items():
        all_keys = {k for k in list(rl_dep) + list(rl_arr) if k[0] == route}
        lines_out: dict = {}
        for (_, line) in sorted(all_keys):
            dd = rl_dep.get((route, line), [])
            ad = rl_arr.get((route, line), [])
            count = max(len(dd), len(ad))
            if count < _MIN_COUNT:
                continue
            lines_out[line] = {
                "count": count,
                "mean_dep_delay_s": round(statistics.mean(dd), 1) if dd else None,
                "mean_arr_delay_s": round(statistics.mean(ad), 1) if ad else None,
            }
        if not lines_out:
            continue
        routes_out[route] = {
            "from_id": from_id,
            "to_id": to_id,
            "label": ROUTE_LABELS.get(route, route),
            "lines": lines_out,
        }

    # Build stations payload
    stations_out: dict = {}
    for sid, coords in STATION_COORDS.items():
        dd = station_dep.get(sid, [])
        ad = station_arr.get(sid, [])
        all_d = ad + dd  # prefer arr for overall colour; it's what hurts the rider
        stations_out[sid] = {
            **coords,
            "mean_dep_delay_s": round(statistics.mean(dd), 1) if dd else None,
            "dep_count": len(dd),
            "mean_arr_delay_s": round(statistics.mean(ad), 1) if ad else None,
            "arr_count": len(ad),
            "overall_mean_s": round(statistics.mean(all_d), 1) if all_d else None,
        }

    return {"stations": stations_out, "routes": routes_out}


@app.get("/", response_class=HTMLResponse)
async def index():
    return _HTML_FILE.read_text(encoding="utf-8")
