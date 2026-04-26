#!/usr/bin/env python3
"""Generate static JSON + HTML for GitHub Pages deployment.

Creates:
  docs/index.html        — dashboard.html with STATIC_MODE = true
  docs/api/journeys.json — raw journey rows; the dashboard recomputes all
                           stats client-side so the Since / Route filters work
                           without a server.

Run:
  python3 export.py [--out docs]
"""
import argparse
import json
import sqlite3
from pathlib import Path

from config import DB_PATH
from database import get_journeys_for_stats

# Fields the JS aggregation needs. Everything else (id, trip_key, product,
# data_stage, last_updated) is omitted to keep the payload small.
_JOURNEY_FIELDS = (
    "date", "route", "line_name",
    "planned_dep", "planned_arr",
    "dep_delay_s", "arr_delay_s",
    "cancelled",
)


def _rewrite_html(src: Path) -> str:
    """Flip STATIC_MODE so dashboard.html pulls api/journeys.json instead of /api/*."""
    html = src.read_text(encoding="utf-8")
    marker = "let STATIC_MODE = false;"
    if marker not in html:
        raise RuntimeError(f"dashboard.html no longer contains {marker!r}")
    return html.replace(marker, "let STATIC_MODE = true;")


def export(out_dir: Path) -> None:
    api_dir = out_dir / "api"
    api_dir.mkdir(parents=True, exist_ok=True)

    # Drop any stale JSON files (e.g. summary.json / trend_*.json from the
    # previous pre-baked export) so the deployed site only contains journeys.json.
    for stale in api_dir.glob("*.json"):
        stale.unlink()

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    journeys = get_journeys_for_stats(conn)
    conn.close()

    print(f"Loaded {len(journeys)} journeys from {DB_PATH}")

    rows = [{k: j.get(k) for k in _JOURNEY_FIELDS} for j in journeys]
    payload = (api_dir / "journeys.json")
    payload.write_text(json.dumps(rows, separators=(",", ":")))
    print(f"  journeys.json  ({payload.stat().st_size:,} bytes)")

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
