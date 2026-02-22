# bahn-statistics

Tracks real-time delays on the commutes around Düsseldorf Hbf commute and builds statistics over time to tell you which train to actually take.

## What it does

- Polls the DB HAFAS API every 5 minutes during commute windows
- Captures departure and arrival delays for S6, RE1, RE2, RE6, RE11 etc.
- Stores everything in a local SQLite database
- Outputs delay statistics with percentiles and ranked train recommendations

**Morning:** departures 07:00–09:00
**Evening:** departures 17:00–20:00

## Setup

```bash
pip install -r requirements.txt
```

No API key needed — uses the public [v6.db.transport.rest](https://v6.db.transport.rest) API.

## Usage

**Run the daemon** (keeps running, wakes up for each commute window):
```bash
python3 main.py scrape
```

**Run once** (single scrape cycle, useful for testing):
```bash
python3 main.py scrape --once
```

**Show statistics** (after data has accumulated over several days):
```bash
python3 main.py stats
python3 main.py stats --route morning
python3 main.py stats --since 2026-02-01
```

**Verify station IDs against live API:**
```bash
python3 main.py verify
```

## Running in the background

```bash
nohup python3 main.py scrape &
# logs go to bahn-scraper.log
tail -f bahn-scraper.log
```

Or keep it alive in a tmux session:
```bash
tmux new -s bahn
python3 main.py scrape
# Ctrl+B D to detach
```

## Statistics output

After a week or two of data, `python3 main.py stats` gives you:

- **Departure delays** — mean, p50, p75, p90, p95 per line
- **Arrival delays** — mean, p50, p90 per line
- **Average journey duration** per line
- **Cancellation rate** and **on-time percentage** per line
- **Slot analysis** — mean delay broken down by 10-minute departure slot
- **Recommendations** — lines ranked by a composite reliability score (p90 delay + cancellation rate)

## How delays are captured

The daemon runs two stages on each poll cycle:

1. **Initial scan** — fetches all departures from the origin and arrivals at the destination for the full window. Trains far in the future show no delay yet.
2. **Refresh** — re-queries any train departing within the next 35 minutes to pick up live delay data just before it leaves.

Trains are matched across the departure and arrival boards using their HAFAS `tripId`, ensuring only trains that actually stop at both stations are recorded. Reversed matches (e.g. a northbound RE6 passing through Düsseldorf on its way *to* Essen) are rejected by checking that arrival time > departure time.

> **Note:** Delay data is only available while a train is active. If the daemon is not running when a train departs, that journey's delay is lost.

## Files

| File | Purpose |
|---|---|
| `config.py` | Station IDs, time windows, line filters |
| `api.py` | HTTP client for v6.db.transport.rest |
| `database.py` | SQLite schema and upsert logic |
| `scraper.py` | Polling daemon and two-stage delay capture |
| `stats.py` | Statistics computation and terminal output |
| `main.py` | CLI entry point |
| `bahn.db` | Created at runtime, all captured data |
| `bahn-scraper.log` | Created at runtime, daemon log |
