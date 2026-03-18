"""FastAPI web dashboard for DB Bahn commute statistics."""
import sqlite3
import statistics
from collections import defaultdict
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Optional

from fastapi import FastAPI, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse

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


# ---------------------------------------------------------------------------
# HTML frontend
# ---------------------------------------------------------------------------
HTML_PAGE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>DB Bahn Statistics</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
* { box-sizing: border-box; margin: 0; padding: 0; }
body {
  font-family: 'Courier New', Courier, monospace;
  background: #0f0f1a;
  color: #d0d0e0;
  padding: 16px;
  font-size: 13px;
}
h1 { color: #88b4f7; font-size: 1.4em; }
h2 {
  color: #88b4f7; font-size: 1.05em;
  margin-top: 20px; margin-bottom: 8px;
  border-bottom: 1px solid #2a2a44; padding-bottom: 4px;
}
h3 { color: #9999bb; font-size: 0.9em; margin: 10px 0 4px; }
.header {
  display: flex; align-items: flex-start; gap: 20px; flex-wrap: wrap;
  margin-bottom: 16px; padding: 12px;
  background: #1a1a30; border-radius: 6px;
}
.header-meta { color: #8888aa; font-size: 0.85em; margin-top: 4px; }
.controls { display: flex; gap: 12px; align-items: center; flex-wrap: wrap; }
label { color: #8888aa; }
input[type="date"], select {
  background: #1e1e35; color: #d0d0e0;
  border: 1px solid #444; padding: 4px 8px;
  border-radius: 4px; font-family: inherit; font-size: 0.9em;
}
button {
  background: #2255aa; color: #fff; border: none;
  padding: 4px 12px; border-radius: 4px; cursor: pointer;
  font-family: inherit; font-size: 0.9em;
}
button:hover { background: #3366cc; }
table { border-collapse: collapse; width: 100%; margin-bottom: 8px; }
th {
  background: #1a1a30; color: #88b4f7; padding: 5px 8px;
  text-align: right; border-bottom: 1px solid #333; white-space: nowrap;
}
th:first-child { text-align: left; }
td { padding: 4px 8px; text-align: right; border-bottom: 1px solid #1a1a2e; white-space: nowrap; }
td:first-child { text-align: left; font-weight: bold; }
tr:hover td { background: #1e1e35; }
.green  { color: #4caf50; }
.amber  { color: #ffa000; }
.red    { color: #f44336; }
.dim    { color: #666; }
.sg { color: #4caf50; font-weight: bold; }
.sa { color: #ffa000; font-weight: bold; }
.sr { color: #f44336; font-weight: bold; }
.chart-container {
  position: relative; height: 280px; margin-bottom: 8px;
  background: #1a1a30; border-radius: 6px; padding: 12px;
}
.charts-row { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }
@media (max-width: 900px) { .charts-row { grid-template-columns: 1fr; } }
.section { margin-bottom: 20px; }
.loading { color: #666; font-style: italic; padding: 8px 0; }
.error   { color: #f44336; }
#lastUpdated { color: #8888aa; font-size: 0.85em; }
.verdict-cell { text-align: left !important; max-width: 300px; white-space: normal !important; }
</style>
</head>
<body>

<div class="header">
  <div>
    <h1>DB Bahn Statistics</h1>
    <div class="header-meta" id="headerMeta">Loading…</div>
  </div>
  <div class="controls">
    <label>Since: <input type="date" id="sinceInput"></label>
    <label>Route:
      <select id="routeSelect">
        <option value="">All routes</option>
        <option value="morning">DDF→ESS (morning)</option>
        <option value="evening">ESS→DDF (evening)</option>
        <option value="aachen_morning">AAH→DDF (morning)</option>
        <option value="aachen_evening">DDF→AAH (evening)</option>
        <option value="wuppertal_morning">WUP→DDF (morning)</option>
        <option value="wuppertal_evening">DDF→WUP (evening)</option>
        <option value="bonn_morning">BON→DDF (morning)</option>
        <option value="bonn_evening">DDF→BON (evening)</option>
      </select>
    </label>
    <button onclick="loadAll()">Refresh</button>
    <span id="lastUpdated"></span>
  </div>
</div>

<div class="section">
  <h2>Arrival Punctuality</h2>
  <div id="arrivalTables" class="loading">Loading…</div>
</div>

<div class="section">
  <h2>Journey Dynamics</h2>
  <div id="dynamicsTables" class="loading">Loading…</div>
</div>

<div class="section">
  <h2>Recommendations</h2>
  <div id="recsTable" class="loading">Loading…</div>
</div>

<div class="charts-row">
  <div class="section">
    <h2>Delay Trend — mean arr delay per day (min)</h2>
    <div class="chart-container"><canvas id="trendChart"></canvas></div>
    <div class="dim" style="font-size:0.8em">Selected route (defaults to morning). Gaps = no data that day.</div>
  </div>
  <div class="section">
    <h2>Departure Slot Analysis — mean arr delay (min)</h2>
    <div class="chart-container"><canvas id="slotChart"></canvas></div>
    <div class="dim" style="font-size:0.8em">Mean arrival delay by 10-min departure slot, selected route.</div>
  </div>
</div>

<div class="section">
  <h2>Arrival Delay Distribution</h2>
  <div class="chart-container" style="height:220px"><canvas id="histChart"></canvas></div>
</div>

<script>
const CHART_COLORS = [
  '#4fc3f7','#aed581','#ffb74d','#f06292','#ce93d8',
  '#80cbc4','#fff176','#ff8a65','#a1887f','#90a4ae',
  '#ef9a9a','#80deea','#ffe082','#c5e1a5','#b39ddb',
];

let trendChart = null, slotChart = null, histChart = null;

const ROUTE_ORDER = [
  'morning','evening',
  'aachen_morning','aachen_evening',
  'wuppertal_morning','wuppertal_evening',
  'bonn_morning','bonn_evening',
];

const ROUTE_LABEL = {
  morning: 'DDF→ESS (morning)', evening: 'ESS→DDF (evening)',
  aachen_morning: 'AAH→DDF (morning)', aachen_evening: 'DDF→AAH (evening)',
  wuppertal_morning: 'WUP→DDF (morning)', wuppertal_evening: 'DDF→WUP (evening)',
  bonn_morning: 'BON→DDF (morning)', bonn_evening: 'DDF→BON (evening)',
};

/* ---- formatting helpers ---- */
function fmt(v, decimals=1) {
  if (v === null || v === undefined) return '<span class="dim">--</span>';
  return v.toFixed(decimals);
}
function fmtMin(v) {
  if (v === null || v === undefined) return '<span class="dim">--</span>';
  return Math.round(v) + ' min';
}
function fmtPct(v) {
  if (v === null || v === undefined) return '<span class="dim">--</span>';
  return v.toFixed(1) + '%';
}
function delayClass(seconds) {
  if (seconds === null || seconds === undefined) return 'dim';
  const m = seconds / 60;
  if (m <= 1) return 'green';
  if (m <= 5) return 'amber';
  return 'red';
}
function scoreClass(s) {
  return s >= 0.75 ? 'sg' : (s >= 0.5 ? 'sa' : 'sr');
}
function dc(s) {  /* delay cell wrapping — s in seconds */
  const cls = delayClass(s);
  const val = s !== null && s !== undefined ? (s/60).toFixed(1) : '--';
  return `<span class="${cls}">${val}</span>`;
}
function dcMin(m) {  /* delay cell from minutes */
  return dc(m !== null && m !== undefined ? m * 60 : null);
}

/* ---- URL param helpers ---- */
function getParams() {
  const p = new URLSearchParams();
  const since = document.getElementById('sinceInput').value;
  if (since) p.set('since', since);
  return p;
}
function getRoute() {
  return document.getElementById('routeSelect').value;
}

/* ---- table builders ---- */
function buildArrivalTable(route, lineData) {
  const lines = Object.keys(lineData).sort();
  const rows = lines.map(line => {
    const s = lineData[line].s;
    const c = delayClass(s.arr_p90_s);
    return `<tr>
      <td>${line}</td>
      <td>${lineData[line].count}</td>
      <td>${fmtPct(s.cancel_rate_pct)}</td>
      <td>${fmtPct(s.on_time_arr_pct)}</td>
      <td>${dc(s.arr_mean_s)}</td>
      <td><span class="${c}">${s.arr_p50_s !== null ? (s.arr_p50_s/60).toFixed(1) : '--'}</span></td>
      <td><span class="${c}">${s.arr_p75_s !== null ? (s.arr_p75_s/60).toFixed(1) : '--'}</span></td>
      <td><span class="${c}">${s.arr_p90_s !== null ? (s.arr_p90_s/60).toFixed(1) : '--'}</span></td>
      <td><span class="${c}">${s.arr_p95_s !== null ? (s.arr_p95_s/60).toFixed(1) : '--'}</span></td>
    </tr>`;
  }).join('');
  return `<h3>${ROUTE_LABEL[route] || route}</h3>
  <table>
    <thead><tr>
      <th style="text-align:left">Line</th><th>N</th><th>Cancel%</th><th>On-time%</th>
      <th>Arr mean</th><th>p50</th><th>p75</th><th>p90</th><th>p95</th>
    </tr></thead>
    <tbody>${rows}</tbody>
  </table>`;
}

function buildDynamicsTable(route, lineData) {
  const lines = Object.keys(lineData).sort();
  const rows = lines.map(line => {
    const s = lineData[line].s;
    return `<tr>
      <td>${line}</td>
      <td class="dim">${s.dep_mean_s !== null ? (s.dep_mean_s/60).toFixed(1) : '<span class="dim">--</span>'}</td>
      <td class="dim">${s.dep_p90_s  !== null ? (s.dep_p90_s/60).toFixed(1)  : '<span class="dim">--</span>'}</td>
      <td>${dc(s.delta_mean_s)}</td>
      <td>${dc(s.delta_p50_s)}</td>
      <td>${dc(s.delta_p90_s)}</td>
      <td>${fmtMin(s.door_mean_min)}</td>
      <td>${fmtMin(s.door_p50_min)}</td>
      <td>${fmtMin(s.door_p90_min)}</td>
      <td class="dim">${s.travel_mean_min !== null ? Math.round(s.travel_mean_min) + ' min' : '--'}</td>
    </tr>`;
  }).join('');
  return `<h3>${ROUTE_LABEL[route] || route}</h3>
  <table>
    <thead><tr>
      <th style="text-align:left">Line</th>
      <th>Dep mean</th><th>Dep p90</th>
      <th>Δ mean</th><th>Δ p50</th><th>Δ p90</th>
      <th>Door mean</th><th>Door p50</th><th>Door p90</th><th>Plan</th>
    </tr></thead>
    <tbody>${rows}</tbody>
  </table>`;
}

function buildRecsTable(recs) {
  const rows = recs.map((rec, i) => {
    return `<tr>
      <td style="text-align:right">${i+1}</td>
      <td style="text-align:left">${rec.route}</td>
      <td style="text-align:left">${rec.line}</td>
      <td><span class="${scoreClass(rec.score)}">${rec.score.toFixed(2)}</span></td>
      <td>${rec.count}</td>
      <td>${dcMin(rec.arr_mean_min)}</td>
      <td>${dcMin(rec.arr_p90_min)}</td>
      <td>${dcMin(rec.delta_p90_min)}</td>
      <td>${fmtMin(rec.door_mean_min)}</td>
      <td>${fmtMin(rec.door_p90_min)}</td>
      <td>${fmtPct(rec.cancel_pct)}</td>
      <td class="verdict-cell">${rec.verdict}</td>
    </tr>`;
  }).join('');
  return `<table>
    <thead><tr>
      <th>Rank</th>
      <th style="text-align:left">Route</th>
      <th style="text-align:left">Line</th>
      <th>Score</th><th>N</th>
      <th>Arr mean</th><th>Arr p90</th><th>Surprise p90</th>
      <th>Door mean</th><th>Door p90</th><th>Cancel%</th>
      <th style="text-align:left">Verdict</th>
    </tr></thead>
    <tbody>${rows}</tbody>
  </table>`;
}

/* ---- chart helpers ---- */
const CHART_DEFAULTS = {
  color: '#d0d0e0',
  font: { family: "'Courier New', monospace", size: 11 },
};
const AXIS_STYLE = {
  ticks: { color: '#888', font: { family: "'Courier New', monospace", size: 10 } },
  grid: { color: '#222' },
};

/* ---- data loaders ---- */
async function loadSummary() {
  const p = getParams();
  const route = getRoute();
  if (route) p.set('route', route);
  const res = await fetch('/api/summary?' + p);
  const data = await res.json();

  const meta = data.date_range
    ? `${data.date_range[0]} → ${data.date_range[1]}  ·  ${data.total} journeys`
    : `${data.total} journeys`;
  document.getElementById('headerMeta').textContent = meta;

  let arrHtml = '', dynHtml = '';
  for (const r of ROUTE_ORDER) {
    if (data.stats[r]) {
      arrHtml += buildArrivalTable(r, data.stats[r]);
      dynHtml += buildDynamicsTable(r, data.stats[r]);
    }
  }
  document.getElementById('arrivalTables').innerHTML  = arrHtml || '<span class="dim">No data</span>';
  document.getElementById('dynamicsTables').innerHTML = dynHtml || '<span class="dim">No data</span>';
  document.getElementById('recsTable').innerHTML      = buildRecsTable(data.recommendations);
}

async function loadTrend() {
  const p = getParams();
  const route = getRoute() || 'morning';
  p.set('route', route);
  const res = await fetch('/api/trend?' + p);
  const data = await res.json();

  const lines = Object.keys(data.series).sort();
  const datasets = lines.map((line, i) => ({
    label: line,
    data: data.series[line],
    borderColor: CHART_COLORS[i % CHART_COLORS.length],
    backgroundColor: CHART_COLORS[i % CHART_COLORS.length] + '33',
    tension: 0.3,
    spanGaps: false,
    pointRadius: 3,
    pointHoverRadius: 5,
  }));

  if (trendChart) trendChart.destroy();
  trendChart = new Chart(document.getElementById('trendChart').getContext('2d'), {
    type: 'line',
    data: { labels: data.dates, datasets },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { legend: { labels: { color: '#d0d0e0', font: { family: "'Courier New',monospace", size: 11 } } } },
      scales: {
        x: { ...AXIS_STYLE, ticks: { ...AXIS_STYLE.ticks, maxTicksLimit: 14, maxRotation: 45 } },
        y: { ...AXIS_STYLE, title: { display: true, text: 'min', color: '#888' } },
      },
    },
  });
}

async function loadSlots() {
  const p = getParams();
  const route = getRoute() || 'morning';
  const res = await fetch('/api/slots?' + p);
  const data = await res.json();

  const routeData = data[route];
  if (!routeData) {
    if (slotChart) { slotChart.destroy(); slotChart = null; }
    return;
  }

  const lines = Object.keys(routeData).sort();
  const allSlots = [...new Set(lines.flatMap(l => Object.keys(routeData[l])))].sort();

  const datasets = lines.map((line, i) => ({
    label: line,
    data: allSlots.map(slot => {
      const v = routeData[line][slot];
      return v !== undefined ? Math.round(v / 60 * 10) / 10 : null;
    }),
    backgroundColor: CHART_COLORS[i % CHART_COLORS.length] + 'bb',
    borderColor: CHART_COLORS[i % CHART_COLORS.length],
    borderWidth: 1,
  }));

  if (slotChart) slotChart.destroy();
  slotChart = new Chart(document.getElementById('slotChart').getContext('2d'), {
    type: 'bar',
    data: { labels: allSlots, datasets },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { legend: { labels: { color: '#d0d0e0', font: { family: "'Courier New',monospace", size: 11 } } } },
      scales: {
        x: { ...AXIS_STYLE },
        y: { ...AXIS_STYLE, title: { display: true, text: 'min', color: '#888' } },
      },
    },
  });
}

async function loadHistogram() {
  const p = getParams();
  const route = getRoute();
  if (route) p.set('route', route);
  const res = await fetch('/api/histogram?' + p);
  const data = await res.json();

  // Color by bucket position: early buckets = green, later = red
  const bgColors = data.labels.map((_, i) => {
    const ratio = i / (data.labels.length - 1);
    if (ratio < 0.15) return '#4caf5099';
    if (ratio < 0.40) return '#ffa00099';
    return '#f4433699';
  });
  const borderColors = bgColors.map(c => c.slice(0, 7));

  if (histChart) histChart.destroy();
  histChart = new Chart(document.getElementById('histChart').getContext('2d'), {
    type: 'bar',
    data: {
      labels: data.labels,
      datasets: [{
        label: 'Journeys',
        data: data.counts,
        backgroundColor: bgColors,
        borderColor: borderColors,
        borderWidth: 1,
      }],
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { legend: { display: false } },
      scales: {
        x: { ...AXIS_STYLE, title: { display: true, text: 'Delay bucket (min)', color: '#888' } },
        y: { ...AXIS_STYLE, title: { display: true, text: 'Journeys', color: '#888' } },
      },
    },
  });
}

/* ---- main refresh ---- */
async function loadAll() {
  try {
    await Promise.all([loadSummary(), loadTrend(), loadSlots(), loadHistogram()]);
    document.getElementById('lastUpdated').textContent =
      'Updated: ' + new Date().toLocaleTimeString();
  } catch (e) {
    console.error('loadAll error:', e);
    document.getElementById('lastUpdated').textContent = 'Error: ' + e.message;
  }
}

document.getElementById('sinceInput').addEventListener('change', loadAll);
document.getElementById('routeSelect').addEventListener('change', loadAll);
loadAll();
setInterval(loadAll, 60000);
</script>
</body>
</html>"""


@app.get("/", response_class=HTMLResponse)
async def index():
    return HTML_PAGE
