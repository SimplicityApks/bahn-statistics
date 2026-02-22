#!/usr/bin/env python3
"""
DB Bahn Statistics Scraper — CLI entry point.

Commands:
  scrape          Run the continuous polling daemon (blocking)
  scrape --once   Run a single scrape cycle and exit
  stats           Print statistics to the terminal
  stats --route morning|evening
  stats --since YYYY-MM-DD
  verify          Verify station IDs against the live API and exit
"""
import argparse
import asyncio
import logging
import sys

from config import DB_PATH, LOG_FILE, LOG_DATE_FORMAT, LOG_FORMAT
from database import count_journeys, init_db, get_journeys_for_stats
from stats import (
    compute_slot_stats,
    compute_stats,
    get_date_range,
    print_stats,
    rank_recommendations,
)


# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------
def setup_logging(verbose: bool = False) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    handlers: list[logging.Handler] = [
        logging.StreamHandler(sys.stderr),
    ]
    try:
        fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
        fh.setLevel(logging.DEBUG)
        handlers.append(fh)
    except OSError as exc:
        print(f"Warning: could not open log file {LOG_FILE}: {exc}", file=sys.stderr)

    logging.basicConfig(
        level=level,
        format=LOG_FORMAT,
        datefmt=LOG_DATE_FORMAT,
        handlers=handlers,
    )


# ---------------------------------------------------------------------------
# Subcommand handlers
# ---------------------------------------------------------------------------
def cmd_verify(args) -> int:  # noqa: ARG001
    """Verify all station IDs (from all configured routes) against the live API."""
    import httpx
    from api import verify_station
    from config import API_BASE_URL, ROUTES

    async def _run():
        # Collect unique station IDs across all routes
        stations: dict[str, str] = {}
        for cfg in ROUTES.values():
            stations[cfg["from_id"]] = cfg["from_name"]
            stations[cfg["to_id"]]   = cfg["to_name"]

        async with httpx.AsyncClient(base_url=API_BASE_URL) as client:
            results = await asyncio.gather(
                *(verify_station(client, sid, name) for sid, name in stations.items())
            )
        if all(results):
            print(f"All {len(stations)} station IDs verified successfully.")
            return 0
        else:
            print("One or more station IDs could not be verified. Check the log.")
            return 1

    return asyncio.run(_run())


def cmd_scrape(args) -> int:
    """Run the scraper (daemon or single-shot)."""
    from scraper import run_daemon, scrape_once

    conn = init_db(DB_PATH)
    n = count_journeys(conn)
    logging.getLogger(__name__).info(
        "Database has %d journey records. Starting scraper…", n
    )

    try:
        if args.once:
            asyncio.run(scrape_once(conn))
        else:
            asyncio.run(run_daemon(conn))
    except KeyboardInterrupt:
        pass
    finally:
        conn.close()
    return 0


def cmd_stats(args) -> int:
    """Compute and display statistics."""
    conn = init_db(DB_PATH)

    journeys = get_journeys_for_stats(
        conn,
        route=getattr(args, "route", None),
        since_date=getattr(args, "since", None),
    )
    conn.close()

    if not journeys:
        print(
            "No data available yet.\n"
            "Start collecting data with:  python main.py scrape\n"
            "Or run a quick test with:   python main.py scrape --once"
        )
        return 0

    stats = compute_stats(journeys)
    slot_stats = compute_slot_stats(journeys)
    recommendations = rank_recommendations(stats)
    date_range = get_date_range(journeys)
    total = len(journeys)

    print_stats(stats, slot_stats, recommendations, date_range=date_range, total_count=total)
    return 0


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------
def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="bahn-stats",
        description="Scrape and analyse Deutsche Bahn commute delays.",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable DEBUG-level logging to stderr.",
    )

    sub = parser.add_subparsers(dest="cmd", required=True, metavar="COMMAND")

    # scrape
    p_scrape = sub.add_parser(
        "scrape",
        help="Run the scraper daemon (use --once for a single cycle).",
    )
    p_scrape.add_argument(
        "--once",
        action="store_true",
        help="Run one scrape cycle and exit instead of looping.",
    )

    # stats
    p_stats = sub.add_parser(
        "stats",
        help="Print delay statistics and recommendations.",
    )
    p_stats.add_argument(
        "--route",
        choices=["morning", "evening", "aachen_morning", "aachen_evening",
                 "wuppertal_morning", "wuppertal_evening"],
        help="Restrict output to one route.",
    )
    p_stats.add_argument(
        "--since",
        metavar="YYYY-MM-DD",
        help="Only include journeys on or after this date.",
    )

    # verify
    sub.add_parser(
        "verify",
        help="Verify station IDs against the live API and exit.",
    )

    return parser


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    setup_logging(verbose=getattr(args, "verbose", False))

    dispatch = {
        "scrape": cmd_scrape,
        "stats": cmd_stats,
        "verify": cmd_verify,
    }

    handler = dispatch.get(args.cmd)
    if handler is None:
        parser.print_help()
        sys.exit(1)

    sys.exit(handler(args))


if __name__ == "__main__":
    main()
