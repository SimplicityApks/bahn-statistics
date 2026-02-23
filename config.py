"""
Central configuration for the DB Bahn statistics scraper.
"""
from datetime import time

# ---------------------------------------------------------------------------
# Station IDs (EVA numbers)
# ---------------------------------------------------------------------------
DUESSELDORF_HBF_ID = "8000085"
ESSEN_HBF_ID       = "8000098"
AACHEN_HBF_ID      = "8000001"
WUPPERTAL_HBF_ID   = "8000266"
BONN_HBF_ID        = "8000044"

# ---------------------------------------------------------------------------
# Route definitions
# ---------------------------------------------------------------------------
ROUTES = {
    # ---- Düsseldorf ↔ Essen (S6, RE1, RE2, RE6, RE11 …) ----
    "morning": {
        "from_id":        DUESSELDORF_HBF_ID,
        "to_id":          ESSEN_HBF_ID,
        "from_name":      "Düsseldorf Hbf",
        "to_name":        "Essen Hbf",
        "window_start":   time(7, 0),
        "window_end":     time(10, 0),
        "direction_hint": "Essen",
    },
    "evening": {
        "from_id":        ESSEN_HBF_ID,
        "to_id":          DUESSELDORF_HBF_ID,
        "from_name":      "Essen Hbf",
        "to_name":        "Düsseldorf Hbf",
        "window_start":   time(17, 0),
        "window_end":     time(20, 0),
        "direction_hint": "Düsseldorf",
    },

    # ---- Aachen ↔ Düsseldorf (RE1, RE4) ----
    # HAFAS changes the #ZI# run-number at Düsseldorf Hbf, so the departure-board
    # tripId from Aachen never matches the arrival-board tripId at Düsseldorf.
    # Use the /journeys planner endpoint instead — it resolves through-journeys
    # correctly and returns both departure and arrival data in one call.
    "aachen_morning": {
        "from_id":      AACHEN_HBF_ID,
        "to_id":        DUESSELDORF_HBF_ID,
        "from_name":    "Aachen Hbf",
        "to_name":      "Düsseldorf Hbf",
        "window_start": time(7, 0),
        "window_end":   time(10, 0),
        "use_journeys": True,   # use /journeys planner instead of dep+arr join
    },
    "aachen_evening": {
        "from_id":      DUESSELDORF_HBF_ID,
        "to_id":        AACHEN_HBF_ID,
        "from_name":    "Düsseldorf Hbf",
        "to_name":      "Aachen Hbf",
        "window_start": time(17, 0),
        "window_end":   time(20, 0),
        "use_journeys": True,
    },

    # ---- Wuppertal ↔ Düsseldorf (RE4, RE13, S8) ----
    # RE4/RE13 are through-trains passing through Düsseldorf Hbf (a HAFAS split point),
    # so departure-board tripIds from Wuppertal may not match the Düsseldorf arrival
    # board. Use /journeys to resolve through-journeys correctly.
    "wuppertal_morning": {
        "from_id":      WUPPERTAL_HBF_ID,
        "to_id":        DUESSELDORF_HBF_ID,
        "from_name":    "Wuppertal Hbf",
        "to_name":      "Düsseldorf Hbf",
        "window_start": time(7, 0),
        "window_end":   time(10, 0),
        "use_journeys": True,
    },
    "wuppertal_evening": {
        "from_id":      DUESSELDORF_HBF_ID,
        "to_id":        WUPPERTAL_HBF_ID,
        "from_name":    "Düsseldorf Hbf",
        "to_name":      "Wuppertal Hbf",
        "window_start": time(17, 0),
        "window_end":   time(20, 0),
        "use_journeys": True,
    },

    # ---- Bonn ↔ Düsseldorf (RE5, RE8) ----
    "bonn_morning": {
        "from_id":      BONN_HBF_ID,
        "to_id":        DUESSELDORF_HBF_ID,
        "from_name":    "Bonn Hbf",
        "to_name":      "Düsseldorf Hbf",
        "window_start": time(7, 0),
        "window_end":   time(10, 0),
        "use_journeys": True,
    },
    "bonn_evening": {
        "from_id":      DUESSELDORF_HBF_ID,
        "to_id":        BONN_HBF_ID,
        "from_name":    "Düsseldorf Hbf",
        "to_name":      "Bonn Hbf",
        "window_start": time(17, 0),
        "window_end":   time(20, 0),
        "use_journeys": True,
    },
}

# ---------------------------------------------------------------------------
# Train product filter
# ---------------------------------------------------------------------------
# Products to include (client-side filter on dep['line']['product'])
ALLOWED_PRODUCTS = {"suburban", "regional", "regionalExp"}

# Query-level product params for the API
PRODUCT_PARAMS = {
    "suburban": "true",
    "regional": "true",
    "regionalExp": "true",
    "national": "false",
    "nationalExpress": "false",
    "bus": "false",
    "ferry": "false",
    "subway": "false",
    "tram": "false",
    "taxi": "false",
}

# ---------------------------------------------------------------------------
# API settings
# ---------------------------------------------------------------------------
API_BASE_URL = "https://v6.db.transport.rest"
API_TIMEOUT_S = 20
# Leave comfortable margin below the 100 req/min hard limit
API_MIN_INTERVAL_S = 0.8  # ~75 req/min

# ---------------------------------------------------------------------------
# Scheduling
# ---------------------------------------------------------------------------
# Buffer around the journey windows during which we poll actively
ACTIVE_BUFFER_BEFORE_MIN = 15   # start polling this many minutes before window
ACTIVE_BUFFER_AFTER_MIN = 30    # keep polling this many minutes after window

# Poll intervals
POLL_INTERVAL_ACTIVE_S = 300    # 5 minutes while active window is open
POLL_INTERVAL_IDLE_MAX_S = 1800  # 30 minutes max sleep while idle (for clean shutdown)

# Two-stage refresh: re-query trains departing within this window to capture live delays
REFRESH_LOOKAHEAD_MIN = 35

# How many minutes of departures to fetch in one API call (should cover full window + buffer)
DEPARTURES_DURATION_MIN = 150   # 2h 30m covers the longest window with buffers

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------
DB_PATH = "bahn.db"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
LOG_FILE = "bahn-scraper.log"
LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
