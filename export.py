#!/usr/bin/env python3
"""Generate static JSON + HTML for GitHub Pages deployment.

Creates:
  docs/index.html          — dashboard with API URLs rewritten for static serving
  docs/api/summary.json
  docs/api/slots.json
  docs/api/trend_<route>.json  (one per route)
  docs/api/histogram.json
  docs/api/map.json

Run:
  python3 export.py [--out docs]
"""
import argparse
import json
import sqlite3
import statistics
from collections import defaultdict
from pathlib import Path

from config import DB_PATH
from database import get_journeys_for_stats
from stats import compute_stats, compute_slot_stats, rank_recommendations, get_date_range
from web import _strip_stats, _build_trend, _build_histogram, STATION_COORDS, ROUTE_STATIONS, ROUTE_LABELS, _MIN_COUNT

ROUTES = [
    "morning", "evening",
    "aachen_morning", "aachen_evening",
    "wuppertal_morning", "wuppertal_evening",
    "bonn_morning", "bonn_evening",
]


def _build_map(journeys: list[dict]) -> dict:
    """Synchronous version of api_map — same logic as web.py."""
    rl_dep: dict = defaultdict(list)
    rl_arr: dict = defaultdict(list)
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
        if lines_out:
            routes_out[route] = {
                "from_id": from_id,
                "to_id": to_id,
                "label": ROUTE_LABELS.get(route, route),
                "lines": lines_out,
            }

    stations_out: dict = {}
    for sid, coords in STATION_COORDS.items():
        dd = station_dep.get(sid, [])
        ad = station_arr.get(sid, [])
        all_d = ad + dd
        stations_out[sid] = {
            **coords,
            "mean_dep_delay_s": round(statistics.mean(dd), 1) if dd else None,
            "dep_count": len(dd),
            "mean_arr_delay_s": round(statistics.mean(ad), 1) if ad else None,
            "arr_count": len(ad),
            "overall_mean_s": round(statistics.mean(all_d), 1) if all_d else None,
        }

    return {"stations": stations_out, "routes": routes_out}


def _build_slots_response(journeys: list[dict]) -> dict:
    """Replicate api_slots filtering — returns all routes (no route filter)."""
    slot_stats = compute_slot_stats(journeys)
    counts: dict = defaultdict(lambda: defaultdict(int))
    for j in journeys:
        if not j.get("cancelled") and j.get("arr_delay_s") is not None:
            counts[j["route"]][j["line_name"]] += 1
    return {
        r: {line: slots for line, slots in lines.items() if counts[r][line] >= _MIN_COUNT}
        for r, lines in slot_stats.items()
        if any(counts[r][line] >= _MIN_COUNT for line in lines)
    }


def _rewrite_html(src: Path) -> str:
    """Copy dashboard.html and rewrite /api/* fetch calls for static JSON files."""
    html = src.read_text(encoding="utf-8")

    # Trend is parameterised by route — pre-generated per-route JSON files
    html = html.replace(
        "const res = await fetch('/api/trend?' + p);",
        "const res = await fetch(`api/trend_${route}.json`);",
    )
    # All other endpoints: drop query params, point to static JSON
    html = html.replace("fetch('/api/summary?' + p)", "fetch('api/summary.json')")
    html = html.replace("fetch('/api/slots?' + p)", "fetch('api/slots.json')")
    html = html.replace("fetch('/api/histogram?' + p)", "fetch('api/histogram.json')")
    html = html.replace("fetch('/api/map?' + p)", "fetch('api/map.json')")

    return html


def export(out_dir: Path) -> None:
    api_dir = out_dir / "api"
    api_dir.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    journeys = get_journeys_for_stats(conn)
    conn.close()

    print(f"Loaded {len(journeys)} journeys from {DB_PATH}")

    # summary.json
    stats = compute_stats(journeys)
    date_range = get_date_range(journeys)
    summary = {
        "stats": _strip_stats(stats),
        "recommendations": rank_recommendations(stats),
        "date_range": list(date_range) if date_range else None,
        "total": len(journeys),
    }
    (api_dir / "summary.json").write_text(json.dumps(summary))
    print("  summary.json")

    # slots.json
    (api_dir / "slots.json").write_text(json.dumps(_build_slots_response(journeys)))
    print("  slots.json")

    # trend_<route>.json — one file per route so the route selector still works
    for route in ROUTES:
        (api_dir / f"trend_{route}.json").write_text(json.dumps(_build_trend(journeys, route)))
    print(f"  trend_<route>.json  ({len(ROUTES)} routes)")

    # histogram.json
    (api_dir / "histogram.json").write_text(json.dumps(_build_histogram(journeys)))
    print("  histogram.json")

    # map.json
    (api_dir / "map.json").write_text(json.dumps(_build_map(journeys)))
    print("  map.json")

    # index.html — dashboard.html with static API paths
    src = Path(__file__).parent / "dashboard.html"
    (out_dir / "index.html").write_text(_rewrite_html(src), encoding="utf-8")
    print("  index.html")

    print(f"Done → {out_dir}/")


def main() -> None:
    parser = argparse.ArgumentParser(description="Export static GitHub Pages site")
    parser.add_argument("--out", default="docs", help="Output directory (default: docs)")
    args = parser.parse_args()
    export(Path(args.out))


if __name__ == "__main__":
    main()
