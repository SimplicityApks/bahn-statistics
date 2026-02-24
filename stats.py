"""
Statistics computation and terminal output for the DB Bahn scraper.

Focus: arrival punctuality and "surprise" delay added after the 15-minute mark.

Key metrics:
  - Arrival delay at destination (primary — this is what actually hurts)
  - Journey delay delta = arr_delay - dep_delay (surprise added en route after T-15)
  - Door-to-door time from T-15 = planned_travel + 15 min + arr_delay
    (useful for comparing slower S6 vs faster RE)
  - Departure delay shown as secondary "known at T-15" context

Output sections:
  1. Per-route × per-line: arrival punctuality table (primary)
  2. Per-route × per-line: journey dynamics — dep delay (known), surprise delta, door-to-door
  3. Departure slot analysis: mean arrival delay per 10-min departure slot
  4. Recommendations: ranked by arrival reliability + surprise score
"""
import math
import statistics
from collections import defaultdict
from datetime import datetime
from typing import Optional

from rich import box
from rich.console import Console
from rich.rule import Rule
from rich.table import Table
from rich.text import Text

# Use a fixed width wide enough to accommodate all columns without truncation.
_term_cols = Console().width
console = Console(width=max(120, _term_cols))


# ---------------------------------------------------------------------------
# Statistics computation
# ---------------------------------------------------------------------------
def _filter_travel_pairs(
    pairs: list[tuple[float, Optional[float]]],
    factor: float = 2.5,
) -> list[tuple[float, Optional[float]]]:
    """
    Filter (travel_min, arr_delay_or_none) pairs where planned travel time is
    an outlier vs the median for this line.

    WHY travel_min only:
      Bad arrival matches have a wrong planned_arr (e.g. 19:38 instead of 17:38),
      inflating travel_min to 149 min on a 29-min route.  The signal is always in
      the PLANNED duration — never in arr_delay_s, which can legitimately be very
      large (a 2-hour delay still leaves travel_min at ~29 min).

      Filtering on door_to_door would incorrectly discard legitimate high-delay
      journeys (door = 29 + 15 + 120 ≈ 164 min looks like the bad match at 166 min).

    With fewer than 4 pairs the sample is too small to estimate a reliable median,
    so no filtering is applied.
    """
    if len(pairs) < 4:
        return pairs
    travel_mins = [t for t, _ in pairs]
    med = statistics.median(travel_mins)
    if med <= 0:
        return pairs
    return [(t, d) for t, d in pairs if t <= med * factor]


def _percentile(data: list[float], p: float) -> float:
    """Compute the p-th percentile of a sorted list (0 <= p <= 100)."""
    if not data:
        return 0.0
    sorted_data = sorted(data)
    n = len(sorted_data)
    idx = (p / 100) * (n - 1)
    lo = int(idx)
    hi = lo + 1
    if hi >= n:
        return sorted_data[-1]
    frac = idx - lo
    return sorted_data[lo] + frac * (sorted_data[hi] - sorted_data[lo])


def compute_stats(journeys: list[dict]) -> dict:
    """
    Build per-route × per-line statistics.

    Returns:
        {
          "morning": {
            "S6": {
              "count": int,
              "cancelled_count": int,
              "dep_delays": [float, ...],       # seconds — known at T-15
              "arr_delays": [float, ...],        # seconds — primary metric
              "journey_deltas": [float, ...],    # arr_delay - dep_delay, surprise en route
              "door_to_door_mins": [float, ...], # planned_travel + 15min + arr_delay/60
              "travel_times": [float, ...],      # planned duration in minutes
              "s": { <computed stats dict> }
            }, ...
          }, ...
        }
    """
    result: dict = {}

    for j in journeys:
        route = j["route"]
        line = j["line_name"]

        if route not in result:
            result[route] = {}
        if line not in result[route]:
            result[route][line] = {
                "count": 0,
                "cancelled_count": 0,
                "dep_delays": [],
                "arr_delays": [],
                "journey_deltas": [],
                # (travel_min, arr_delay_s_or_none) pairs — filtered jointly below
                "travel_pairs": [],
            }

        entry = result[route][line]
        entry["count"] += 1

        if j.get("cancelled"):
            entry["cancelled_count"] += 1
            continue

        dep_delay = j.get("dep_delay_s")
        arr_delay = j.get("arr_delay_s")

        if dep_delay is not None:
            entry["dep_delays"].append(float(dep_delay))
        if arr_delay is not None:
            entry["arr_delays"].append(float(arr_delay))

        # Journey delta: surprise added between departure and arrival
        if dep_delay is not None and arr_delay is not None:
            entry["journey_deltas"].append(float(arr_delay - dep_delay))

        # Collect (travel_min, arr_delay) pair for joint outlier filtering below
        planned_dep_str = j.get("planned_dep")
        planned_arr_str = j.get("planned_arr")
        if planned_dep_str and planned_arr_str:
            try:
                dep_dt = datetime.fromisoformat(planned_dep_str)
                arr_dt = datetime.fromisoformat(planned_arr_str)
                travel_min = (arr_dt - dep_dt).total_seconds() / 60
                if 0 < travel_min < 300:
                    entry["travel_pairs"].append(
                        (travel_min, float(arr_delay) if arr_delay is not None else None)
                    )
            except (ValueError, TypeError):
                pass

    # 30-minute penalty applied to all delay metrics for cancelled trains.
    # Rationale: a cancellation detected at T-15 or later forces you onto the next
    # service (~30 min wait), indistinguishable from a 30-min delay from the rider's
    # perspective.  We do NOT add to journey_deltas because a cancellation is not an
    # en-route surprise — it is a full service failure equally hitting departure and
    # arrival, with no additional delay added during a journey that never ran.
    _CANCEL_PENALTY_S = 1800.0  # 30 minutes

    # Compute derived statistics for each line
    for route_data in result.values():
        for line_data in route_data.values():
            dd = line_data["dep_delays"][:]   # copy; we'll append penalty entries
            ad = line_data["arr_delays"][:]
            jd = line_data["journey_deltas"]
            total = line_data["count"]
            cancelled = line_data["cancelled_count"]

            # Filter out bad arrival matches using planned travel time only.
            # arr_delay is never filtered: a real 2-hour delay keeps travel_min ~29 min
            # and belongs in the data. Only a wrong planned_arr inflates travel_min.
            clean_pairs = _filter_travel_pairs(line_data["travel_pairs"])
            tt = [t for t, _ in clean_pairs]
            dt = [t + 15.0 + d / 60.0 for t, d in clean_pairs if d is not None]

            # Inject cancellation penalty entries into departure/arrival delay lists
            # and door-to-door.  Travel time uses the line's own median so the
            # door-to-door penalty is realistic for that specific route segment.
            travel_med_min = statistics.median(tt) if tt else 30.0
            for _ in range(cancelled):
                dd.append(_CANCEL_PENALTY_S)
                ad.append(_CANCEL_PENALTY_S)
                dt.append(travel_med_min + 15.0 + _CANCEL_PENALTY_S / 60.0)

            line_data["s"] = {
                "cancel_rate_pct": (cancelled / total * 100) if total else 0.0,
                # On-time at destination: arrived within 1 min of schedule.
                # Cancellations are counted as not on time (1800 > 60).
                "on_time_arr_pct": (
                    sum(1 for d in ad if d <= 60) / len(ad) * 100
                ) if ad else 0.0,
                # Departure delay — known at T-15, shown as secondary context
                "dep_mean_s": statistics.mean(dd) if dd else None,
                "dep_p90_s":  _percentile(dd, 90) if dd else None,
                # Arrival delay at destination — PRIMARY metric
                "arr_mean_s": statistics.mean(ad) if ad else None,
                "arr_p50_s":  _percentile(ad, 50) if ad else None,
                "arr_p75_s":  _percentile(ad, 75) if ad else None,
                "arr_p90_s":  _percentile(ad, 90) if ad else None,
                "arr_p95_s":  _percentile(ad, 95) if ad else None,
                # Journey delta: surprise delay added en route (arr - dep)
                "delta_mean_s": statistics.mean(jd) if jd else None,
                "delta_p50_s":  _percentile(jd, 50) if jd else None,
                "delta_p90_s":  _percentile(jd, 90) if jd else None,
                # Door-to-door from T-15 in minutes
                "door_mean_min": statistics.mean(dt) if dt else None,
                "door_p50_min":  _percentile(dt, 50) if dt else None,
                "door_p90_min":  _percentile(dt, 90) if dt else None,
                # Planned travel time
                "travel_mean_min": statistics.mean(tt) if tt else None,
            }

    return result


def compute_slot_stats(journeys: list[dict]) -> dict:
    """
    Group journeys into 10-minute departure-time buckets and compute mean
    arrival delay per (route, line, slot) triple.

    The slot key is the DEPARTURE time (what the user picks when planning),
    but the delay shown is the ARRIVAL delay (what actually matters).

    Returns:
        { "morning": { "S6": { "07:10": mean_arr_delay_s, ... }, ... }, ... }
    """
    buckets: dict = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))

    for j in journeys:
        if j.get("cancelled") or j.get("arr_delay_s") is None:
            continue
        route = j["route"]
        line = j["line_name"]
        try:
            dep_dt = datetime.fromisoformat(j["planned_dep"])
            minute_bucket = (dep_dt.minute // 10) * 10
            slot = f"{dep_dt.hour:02d}:{minute_bucket:02d}"
            buckets[route][line][slot].append(float(j["arr_delay_s"]))
        except (ValueError, TypeError, KeyError):
            continue

    result: dict = {}
    for route, lines in buckets.items():
        result[route] = {}
        for line, slots in lines.items():
            result[route][line] = {
                slot: statistics.mean(vals)
                for slot, vals in sorted(slots.items())
            }
    return result


def rank_recommendations(stats: dict) -> list[dict]:
    """
    Rank lines per route by composite reliability score:
        50% arrival p90 delay          (primary — does it arrive on time?)
        30% journey delta p90          (surprise after T-15 — what can't be adjusted for)
        20% cancellation rate

    Clamp score to [0, 1]. Returns list sorted by score descending.
    """
    recs = []
    for route, lines in stats.items():
        for line, data in lines.items():
            s = data["s"]
            arr_p90   = s.get("arr_p90_s")   or 0.0
            delta_p90 = s.get("delta_p90_s") or 0.0
            cancel    = s.get("cancel_rate_pct") or 0.0

            score = (
                0.5 * max(0.0, 1.0 - arr_p90 / 600.0)
                + 0.3 * max(0.0, 1.0 - delta_p90 / 600.0)
                + 0.2 * max(0.0, 1.0 - cancel / 100.0)
            )
            score = max(0.0, min(1.0, score))

            arr_mean_min  = (s.get("arr_mean_s")  or 0) / 60
            arr_p90_min   = arr_p90 / 60
            delta_p90_min = delta_p90 / 60
            door_p90      = s.get("door_p90_min")
            cancel_str    = f"{cancel:.1f}%"

            if score >= 0.85:
                verdict = "Excellent — arrives on time, minimal surprises en route."
            elif score >= 0.70:
                door_str = f", door-to-door p90 {door_p90:.0f} min" if door_p90 else ""
                verdict = f"Good — mean arr delay {arr_mean_min:.1f} min, p90 {arr_p90_min:.1f} min{door_str}."
            elif score >= 0.50:
                verdict = (
                    f"Moderate — arr p90 {arr_p90_min:.1f} min, "
                    f"surprise p90 {delta_p90_min:.1f} min, cancel {cancel_str}."
                )
            else:
                verdict = (
                    f"Poor — high arr p90 ({arr_p90_min:.1f} min) "
                    f"or cancellation rate {cancel_str}."
                )

            recs.append({
                "route": route,
                "line": line,
                "score": score,
                "verdict": verdict,
                "count": data["count"],
                "arr_p90_min": arr_p90_min,
                "delta_p90_min": delta_p90_min,
                "door_p90_min": door_p90,
                "cancel_pct": cancel,
            })

    recs.sort(key=lambda r: (-r["score"], r["route"], r["line"]))
    return recs


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------
def _fmt_delay(seconds: Optional[float]) -> str:
    """Format a delay value in minutes, or '--' if missing."""
    if seconds is None:
        return "[dim]--[/dim]"
    return f"{seconds / 60:.1f}"


def _fmt_min(minutes: Optional[float]) -> str:
    """Format a minutes value, or '--' if missing."""
    if minutes is None:
        return "[dim]--[/dim]"
    return f"{minutes:.0f}"


def _pct(v: Optional[float]) -> str:
    if v is None:
        return "--"
    return f"{v:.1f}%"


def _color_delay(seconds: Optional[float]) -> str:
    """Return a rich color tag based on arrival delay severity."""
    if seconds is None:
        return "dim"
    m = seconds / 60
    if m <= 1:
        return "green"
    if m <= 5:
        return "yellow"
    return "red"


def _color_min(minutes: Optional[float]) -> str:
    """Color for minute values (door-to-door, travel time)."""
    return "dim" if minutes is None else "white"


# ---------------------------------------------------------------------------
# Print functions
# ---------------------------------------------------------------------------
ROUTE_LABELS = {
    "morning":           "MORNING  Düsseldorf Hbf → Essen Hbf",
    "evening":           "EVENING  Essen Hbf → Düsseldorf Hbf",
    "aachen_morning":    "MORNING  Aachen Hbf → Düsseldorf Hbf",
    "aachen_evening":    "EVENING  Düsseldorf Hbf → Aachen Hbf",
    "wuppertal_morning": "MORNING  Wuppertal Hbf → Düsseldorf Hbf",
    "wuppertal_evening": "EVENING  Düsseldorf Hbf → Wuppertal Hbf",
    "bonn_morning":      "MORNING  Bonn Hbf → Düsseldorf Hbf",
    "bonn_evening":      "EVENING  Düsseldorf Hbf → Bonn Hbf",
}

ROUTE_ORDER = (
    "morning", "evening",
    "aachen_morning", "aachen_evening",
    "wuppertal_morning", "wuppertal_evening",
    "bonn_morning", "bonn_evening",
)


def _make_arrival_table(line_data: dict) -> Table:
    """Primary table: arrival punctuality at destination."""
    t = Table(
        box=box.SIMPLE_HEAD,
        show_header=True,
        header_style="bold cyan",
        show_footer=False,
        expand=False,
    )
    t.add_column("Line",       style="bold", no_wrap=True)
    t.add_column("N",          justify="right", no_wrap=True)
    t.add_column("Cancel%",    justify="right", no_wrap=True)
    t.add_column("On-time%",   justify="right", no_wrap=True, style="dim")
    t.add_column("Arr mean",   justify="right", no_wrap=True)
    t.add_column("Arr p50",    justify="right", no_wrap=True)
    t.add_column("Arr p75",    justify="right", no_wrap=True)
    t.add_column("Arr p90",    justify="right", no_wrap=True)
    t.add_column("Arr p95",    justify="right", no_wrap=True)

    for line in sorted(line_data.keys()):
        data = line_data[line]
        s = data["s"]
        c = _color_delay(s.get("arr_p90_s"))
        t.add_row(
            line,
            str(data["count"]),
            _pct(s.get("cancel_rate_pct")),
            _pct(s.get("on_time_arr_pct")),
            f"[{c}]{_fmt_delay(s.get('arr_mean_s'))}[/{c}]",
            f"[{c}]{_fmt_delay(s.get('arr_p50_s'))}[/{c}]",
            f"[{c}]{_fmt_delay(s.get('arr_p75_s'))}[/{c}]",
            f"[{c}]{_fmt_delay(s.get('arr_p90_s'))}[/{c}]",
            f"[{c}]{_fmt_delay(s.get('arr_p95_s'))}[/{c}]",
        )
    return t


def _make_dynamics_table(line_data: dict) -> Table:
    """
    Secondary table: journey dynamics.

    Shows departure delay (known at T-15, adjustable), surprise delay added
    en route (delta = arr - dep), and total door-to-door time from T-15.
    """
    t = Table(
        box=box.SIMPLE_HEAD,
        show_header=True,
        header_style="bold cyan",
        show_footer=False,
        expand=False,
    )
    t.add_column("Line",      style="bold", no_wrap=True)
    t.add_column("Dep mean",  justify="right", no_wrap=True, style="dim")
    t.add_column("Dep p90",   justify="right", no_wrap=True, style="dim")
    t.add_column("Δ mean",    justify="right", no_wrap=True)
    t.add_column("Δ p50",     justify="right", no_wrap=True)
    t.add_column("Δ p90",     justify="right", no_wrap=True)
    t.add_column("Door mean", justify="right", no_wrap=True)
    t.add_column("Door p50",  justify="right", no_wrap=True)
    t.add_column("Door p90",  justify="right", no_wrap=True)
    t.add_column("Plan",      justify="right", no_wrap=True, style="dim")

    for line in sorted(line_data.keys()):
        data = line_data[line]
        s = data["s"]
        dc = _color_delay(s.get("delta_p90_s"))
        t.add_row(
            line,
            _fmt_delay(s.get("dep_mean_s")),
            _fmt_delay(s.get("dep_p90_s")),
            f"[{dc}]{_fmt_delay(s.get('delta_mean_s'))}[/{dc}]",
            f"[{dc}]{_fmt_delay(s.get('delta_p50_s'))}[/{dc}]",
            f"[{dc}]{_fmt_delay(s.get('delta_p90_s'))}[/{dc}]",
            _fmt_min(s.get("door_mean_min")) + " min",
            _fmt_min(s.get("door_p50_min")) + " min",
            _fmt_min(s.get("door_p90_min")) + " min",
            f"{s['travel_mean_min']:.0f} min" if s.get("travel_mean_min") else "--",
        )
    return t


def _print_route_tables(stats: dict, route: str) -> None:
    label = ROUTE_LABELS.get(route, route.upper())
    console.print(Rule(f"[bold]{label}[/bold]"))

    line_data = stats.get(route, {})
    console.print(
        "[bold]Arrival punctuality[/bold] "
        "[dim](minutes late at destination; green ≤1, yellow ≤5, red >5 | "
        "On-time% = arrived within 1 min | "
        "cancellations counted as +30 min)[/dim]"
    )
    console.print(_make_arrival_table(line_data))

    console.print(
        "[bold]Journey dynamics[/bold] "
        "[dim](all in minutes | "
        "Dep@T-15 = departure delay known 15 min before, adjustable | "
        "Surprise = extra delay added en route = arr−dep | "
        "Door = total time from leaving home to arriving)[/dim]"
    )
    console.print(_make_dynamics_table(line_data))


def _print_slot_table(slot_stats: dict, route: str) -> None:
    line_data = slot_stats.get(route)
    if not line_data:
        return

    label = ROUTE_LABELS.get(route, route.upper())
    console.print(Rule(f"[bold]{label} — Slot Analysis[/bold]"))

    all_slots: set[str] = set()
    for slots in line_data.values():
        all_slots.update(slots.keys())
    sorted_slots = sorted(all_slots)
    sorted_lines = sorted(line_data.keys())

    t = Table(box=box.SIMPLE_HEAD, show_header=True, header_style="bold cyan")
    t.add_column("Dep slot")
    for line in sorted_lines:
        t.add_column(line, justify="right")

    for slot in sorted_slots:
        cells = []
        for line in sorted_lines:
            delay_s = line_data[line].get(slot)
            if delay_s is None:
                cells.append("[dim]--[/dim]")
            else:
                color = _color_delay(delay_s)
                cells.append(f"[{color}]{delay_s / 60:.1f}[/{color}]")
        t.add_row(slot, *cells)

    console.print(t)
    console.print("[dim]Mean arrival delay (minutes) by 10-min departure slot.[/dim]")


_ROUTE_SHORT = {
    "morning":           "DDF→ESS morn",
    "evening":           "ESS→DDF eve",
    "aachen_morning":    "AAH→DDF morn",
    "aachen_evening":    "DDF→AAH eve",
    "wuppertal_morning": "WUP→DDF morn",
    "wuppertal_evening": "DDF→WUP eve",
    "bonn_morning":      "BON→DDF morn",
    "bonn_evening":      "DDF→BON eve",
}


def _route_short(route: str) -> str:
    return _ROUTE_SHORT.get(route, route)


def _print_recommendations(recs: list[dict]) -> None:
    console.print(Rule("[bold]RECOMMENDATIONS[/bold]"))
    console.print(
        "[dim]Score = 50% arr p90 + 30% surprise p90 + 20% cancel rate "
        "(higher = more reliable)[/dim]"
    )

    t = Table(box=box.SIMPLE_HEAD, show_header=True, header_style="bold cyan")
    t.add_column("Rank",        justify="right")
    t.add_column("Route")
    t.add_column("Line",        style="bold")
    t.add_column("Score",       justify="right")
    t.add_column("N",           justify="right")
    t.add_column("Arr p90",     justify="right")
    t.add_column("Surprise p90",justify="right")
    t.add_column("Door p90",    justify="right")
    t.add_column("Cancel%",     justify="right")
    t.add_column("Verdict")

    for i, rec in enumerate(recs, 1):
        score = rec["score"]
        score_color = "green" if score >= 0.75 else ("yellow" if score >= 0.5 else "red")
        arr_c = _color_delay((rec["arr_p90_min"] or 0) * 60)
        sur_c = _color_delay((rec["delta_p90_min"] or 0) * 60)
        door  = rec.get("door_p90_min")
        t.add_row(
            str(i),
            _route_short(rec["route"]),
            rec["line"],
            f"[{score_color}]{score:.2f}[/{score_color}]",
            str(rec["count"]),
            f"[{arr_c}]{rec['arr_p90_min']:.1f} min[/{arr_c}]",
            f"[{sur_c}]{rec['delta_p90_min']:.1f} min[/{sur_c}]",
            f"{door:.0f} min" if door is not None else "[dim]--[/dim]",
            f"{rec['cancel_pct']:.1f}%",
            rec["verdict"],
        )

    console.print(t)


def print_stats(
    stats: dict,
    slot_stats: dict,
    recommendations: list[dict],
    date_range: Optional[tuple[str, str]] = None,
    total_count: int = 0,
) -> None:
    """Render all statistics sections to the terminal."""
    console.print()
    header = "[bold white on blue]  DB BAHN COMMUTE STATISTICS  [/bold white on blue]"
    if date_range:
        header += f"  Data: {date_range[0]} → {date_range[1]}  ({total_count} journeys)"
    console.print(header)
    console.print()

    for route in ROUTE_ORDER:
        if route in stats:
            _print_route_tables(stats, route)
            console.print()

    for route in ROUTE_ORDER:
        if route in slot_stats:
            _print_slot_table(slot_stats, route)
            console.print()

    _print_recommendations(recommendations)
    console.print()


def get_date_range(journeys: list[dict]) -> Optional[tuple[str, str]]:
    """Return (min_date, max_date) from a list of journey dicts."""
    dates = [j["date"] for j in journeys if j.get("date")]
    if not dates:
        return None
    return (min(dates), max(dates))
