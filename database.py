"""
SQLite database layer for the DB Bahn statistics scraper.
"""
import sqlite3
import logging
from datetime import datetime, timezone
from typing import Optional

from config import DB_PATH

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------
SCHEMA = """
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS journeys (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    trip_key     TEXT    NOT NULL,
    date         TEXT    NOT NULL,   -- "YYYY-MM-DD" (local date)
    route        TEXT    NOT NULL,   -- "morning" | "evening"
    line_name    TEXT    NOT NULL,   -- "S6", "RE2", …
    product      TEXT    NOT NULL,   -- "suburban" | "regional" | "regionalExp"
    planned_dep  TEXT    NOT NULL,   -- ISO 8601 with timezone
    planned_arr  TEXT,               -- ISO 8601, null until arrival matched
    dep_delay_s  INTEGER,            -- seconds; null = not yet known
    arr_delay_s  INTEGER,            -- seconds at destination; null = not yet known
    cancelled    INTEGER NOT NULL DEFAULT 0,
    data_stage   TEXT    NOT NULL DEFAULT 'initial',  -- 'initial' | 'refreshed'
    last_updated TEXT    NOT NULL,
    UNIQUE(trip_key, date, route)
);

CREATE TABLE IF NOT EXISTS scrape_runs (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at        TEXT NOT NULL,
    route             TEXT NOT NULL,
    journeys_upserted INTEGER,
    errors            INTEGER DEFAULT 0,
    duration_s        REAL
);
"""


def init_db(db_path: str = DB_PATH) -> sqlite3.Connection:
    """Create (or open) the SQLite database, apply schema, return connection."""
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA)
    conn.commit()
    log.info("Database ready at %s", db_path)
    return conn


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def upsert_journey(conn: sqlite3.Connection, journey: dict) -> None:
    """
    Insert a journey or update an existing one (matched by trip_key+date+route).

    Update rules:
    - dep_delay_s / arr_delay_s: only overwrite if the new value is non-null
    - planned_arr: fill in if previously null
    - cancelled: always take the latest value
    - data_stage: take the latest (so 'refreshed' wins over 'initial')
    """
    sql = """
    INSERT INTO journeys
        (trip_key, date, route, line_name, product,
         planned_dep, planned_arr, dep_delay_s, arr_delay_s,
         cancelled, data_stage, last_updated)
    VALUES
        (:trip_key, :date, :route, :line_name, :product,
         :planned_dep, :planned_arr, :dep_delay_s, :arr_delay_s,
         :cancelled, :data_stage, :last_updated)
    ON CONFLICT(trip_key, date, route) DO UPDATE SET
        cancelled    = excluded.cancelled,
        data_stage   = excluded.data_stage,
        last_updated = excluded.last_updated,
        planned_arr  = COALESCE(excluded.planned_arr, journeys.planned_arr),
        dep_delay_s  = CASE
                           WHEN excluded.dep_delay_s IS NOT NULL THEN excluded.dep_delay_s
                           ELSE journeys.dep_delay_s
                       END,
        arr_delay_s  = CASE
                           WHEN excluded.arr_delay_s IS NOT NULL THEN excluded.arr_delay_s
                           ELSE journeys.arr_delay_s
                       END
    """
    row = {
        "trip_key": journey["trip_key"],
        "date": journey["date"],
        "route": journey["route"],
        "line_name": journey["line_name"],
        "product": journey["product"],
        "planned_dep": journey["planned_dep"],
        "planned_arr": journey.get("planned_arr"),
        "dep_delay_s": journey.get("dep_delay_s"),
        "arr_delay_s": journey.get("arr_delay_s"),
        "cancelled": 1 if journey.get("cancelled") else 0,
        "data_stage": journey.get("data_stage", "initial"),
        "last_updated": _now_iso(),
    }
    try:
        conn.execute(sql, row)
        conn.commit()
    except sqlite3.Error as exc:
        log.error("upsert_journey failed for %s: %s", journey.get("trip_key"), exc)
        conn.rollback()


def get_pending_refresh(
    conn: sqlite3.Connection,
    route: str,
    within_minutes: int,
) -> list[dict]:
    """
    Return journey rows that:
    - belong to today
    - match the given route
    - haven't been refreshed yet (data_stage = 'initial')
    - have a planned_dep within the next `within_minutes` minutes
    - are not cancelled
    """
    sql = """
    SELECT * FROM journeys
    WHERE route = ?
      AND date  = date('now', 'localtime')
      AND cancelled = 0
      AND data_stage = 'initial'
      AND datetime(planned_dep) <= datetime('now', ? || ' minutes', 'utc')
      AND datetime(planned_dep) >= datetime('now', '-15 minutes', 'utc')
    ORDER BY planned_dep
    """
    cursor = conn.execute(sql, (route, f"+{within_minutes}"))
    return [dict(row) for row in cursor.fetchall()]


def get_journeys_for_stats(
    conn: sqlite3.Connection,
    route: Optional[str] = None,
    since_date: Optional[str] = None,
) -> list[dict]:
    """
    Return journeys suitable for statistics:
    - dep_delay_s IS NOT NULL (we have real delay data)
    Optionally filter by route and/or since_date.
    Includes cancelled journeys (caller decides how to handle them).
    """
    conditions = ["1=1"]
    params: list = []

    if route:
        conditions.append("route = ?")
        params.append(route)
    if since_date:
        conditions.append("date >= ?")
        params.append(since_date)

    where = " AND ".join(conditions)
    sql = f"""
    SELECT * FROM journeys
    WHERE {where}
    ORDER BY date, planned_dep
    """
    cursor = conn.execute(sql, params)
    return [dict(row) for row in cursor.fetchall()]


def log_scrape_run(conn: sqlite3.Connection, run: dict) -> None:
    """Record metadata about a completed scrape cycle."""
    sql = """
    INSERT INTO scrape_runs (started_at, route, journeys_upserted, errors, duration_s)
    VALUES (:started_at, :route, :journeys_upserted, :errors, :duration_s)
    """
    try:
        conn.execute(sql, run)
        conn.commit()
    except sqlite3.Error as exc:
        log.error("log_scrape_run failed: %s", exc)


def count_journeys(conn: sqlite3.Connection) -> int:
    """Return total journey count (for startup info messages)."""
    row = conn.execute("SELECT COUNT(*) FROM journeys").fetchone()
    return row[0] if row else 0
