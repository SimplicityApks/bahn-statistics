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
    ROUTE_ORDER,
    _ROUTE_SHORT,
    compute_slot_stats,
    compute_stats,
    get_date_range,
    rank_recommendations,
)


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
def _strip_stats(stats: dict) -> dict:
    """Remove bulk list fields from compute_stats output."""
    result: dict = {}
    for route, lines in stats.items():
        result[route] = {}
        for line, data in lines.items():
            result[route][line] = {
                k: v for k, v in data.items()
                if k not in ("dep_delays", "arr_delays", "journey_deltas", "travel_pairs")
            }
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
    for date_data in by_date_line.values():
        all_lines.update(date_data.keys())
    sorted_lines = sorted(all_lines)

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
    return compute_slot_stats(journeys)


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


@app.get("/", response_class=HTMLResponse)
async def index():
    return _HTML_FILE.read_text(encoding="utf-8")
