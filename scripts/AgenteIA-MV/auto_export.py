#!/usr/bin/env python3
"""
auto_export.py
==============
Periodic exporter for the final predictions table.

Reads `predicted.eventos` from the SOC database on a fixed interval and writes
the whole table as a single, clean JSON file. The goal is isolation: no
external party (SOAR / Automation Factory) connects to the production database
directly -- they only read this exported JSON file.

Design notes:
  - Read-only DB access: the connection is opened in READ ONLY mode, so this
    script physically cannot modify the database.
  - Atomic file write: data is written to a temp file and then renamed, so a
    reader never sees a half-written file.
  - Change detection: the JSON is only rewritten when the data actually
    changed, to avoid rewriting a (potentially large) file every cycle.
  - Same logging convention as auto_splunk.py / auto_predict.py
    (logs/auto_export_YYYY-MM-DD.log, rotation at LOG_MAX_BYTES).

Run:
    python3 auto_export.py

Stop (clean):
    Ctrl+C
"""
import os
import json
import time
import gzip
import base64
import signal
import hashlib
import tempfile
import traceback
from datetime import datetime, date
from decimal import Decimal

import psycopg2
from psycopg2.extras import RealDictCursor

# ============================================================
# CONFIG
# ============================================================
DB_CONFIG = {
    "host": "localhost",
    "port": 1234,
    "database": "alertas_soc",
    "user": "postgres_user",
    "password": "DB_PASSWORD",
}

# Where the JSON snapshot is written.
# >>> CHANGE THIS to the agreed location once you decide where it should live.
OUTPUT_PATH = os.path.join("exports", "predicted_events.json")

# Path to the HTML template (soc_dashboard_template.html).
# The template must contain the marker /* __EMBEDDED_DATA__ */ inside its
# <script> block; auto_export.py replaces that marker with the JS constant.
TEMPLATE_HTML_PATH = os.path.join("templates", "soc_dashboard_template.html")

# Where the self-contained HTML snapshot (template + data) is written.
# server.py reads this file to answer GET /export.
# This is the ONLY file the SOAR needs to upload to SharePoint.
OUTPUT_HTML_PATH = os.path.join("exports", "soc_dashboard.html")

# How often (seconds) the table is exported.
EXPORT_INTERVAL_SECONDS = 10

# Maximum number of events embedded in the HTML snapshot.
# 0 = embed ALL events (recommended: the slim+compact strategy keeps the file
# under ~10 MB even for tens of thousands of alerts).
# Set to a positive integer (e.g. 5000) only if SharePoint rejects large files.
MAX_EMBEDDED_EVENTS = 0

# Sleep (seconds) after an unexpected error before retrying.
SLEEP_AFTER_ERROR = 30

# Columns exported to the JSON, in order. Edit this list to match whatever the
# Automation Factory team needs the SOAR to receive. The internal `client`
# flag is left out; processing time columns ARE included so the dashboard can
# show processing-time metrics.
EXPORT_COLUMNS = [
    "source_event_id",
    "orig_client",
    "orig_index",
    "orig_rule_title",
    "rule_description",
    "severity",
    "src_ip",
    "dest_ip",
    "dest_port",
    "action",
    "signature",
    "country",
    "user",
    "hostname",
    "sender",
    "subject",
    "recipient",
    "url",
    "prediction",
    "score",
    "created_at",
    "processing_started_at",
    "processing_seconds",
]

# Logs
LOG_DIR = "logs"
LOG_PREFIX = "auto_export"
LOG_MAX_BYTES = 10 * 1024 * 1024   # 10 MB per file
FLUSH_EVERY = 20                   # flush every N lines


# ============================================================
# LOGGING (same behaviour as auto_predict.py)
# ============================================================
class FileLogger:
    """
    Writes to logs/auto_export_YYYY-MM-DD.log where the date is the file's
    creation date. On start it resumes the most recent file that is still under
    LOG_MAX_BYTES; otherwise it opens a new one. Rotates when full.
    """

    def __init__(self, log_dir: str, prefix: str, max_bytes: int, flush_every: int = 20):
        self.log_dir = log_dir
        self.prefix = prefix
        self.max_bytes = max_bytes
        self.flush_every = flush_every
        self._line_count = 0
        self._byte_count = 0
        self.fp = None
        self.current_path = None

        os.makedirs(log_dir, exist_ok=True)

        existing = self._find_resumable()
        if existing:
            self.current_path = existing
            self._byte_count = os.path.getsize(existing)
            self.fp = open(existing, "a", encoding="utf-8")
        else:
            self._open_new()

    def _all_log_files(self) -> list:
        try:
            files = [
                os.path.join(self.log_dir, f)
                for f in os.listdir(self.log_dir)
                if f.startswith(self.prefix + "_") and f.endswith(".log")
            ]
            files.sort(key=lambda p: os.path.getmtime(p), reverse=True)
            return files
        except OSError:
            return []

    def _find_resumable(self) -> str | None:
        for path in self._all_log_files():
            try:
                if os.path.getsize(path) < self.max_bytes:
                    return path
            except OSError:
                continue
        return None

    def _new_path(self) -> str:
        date_str = datetime.now().strftime("%Y-%m-%d")
        path = os.path.join(self.log_dir, f"{self.prefix}_{date_str}.log")
        n = 2
        while os.path.exists(path):
            path = os.path.join(self.log_dir, f"{self.prefix}_{date_str}_{n}.log")
            n += 1
        return path

    def _open_new(self):
        self.current_path = self._new_path()
        self._byte_count = 0
        self.fp = open(self.current_path, "a", encoding="utf-8")

    def _rotate_if_needed(self):
        if self._byte_count >= self.max_bytes:
            if self.fp:
                try:
                    self.fp.flush()
                    self.fp.close()
                except Exception:
                    pass
            self._open_new()
            self._line_count = 0

    def _flush_if_needed(self):
        if self._line_count >= self.flush_every:
            try:
                self.fp.flush()
            except Exception:
                pass
            self._line_count = 0

    def _write_raw(self, text: str):
        encoded_len = len(text.encode("utf-8"))
        self.fp.write(text)
        self._byte_count += encoded_len
        self._line_count += text.count("\n")
        self._flush_if_needed()
        self._rotate_if_needed()

    def write(self, level: str, msg: str):
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self._write_raw(f"{ts} | {level} | {msg}\n")

    def close(self):
        if self.fp:
            try:
                self.fp.flush()
                self.fp.close()
            except Exception:
                pass


LOGGER: FileLogger | None = None


def _setup_logger():
    global LOGGER
    LOGGER = FileLogger(LOG_DIR, LOG_PREFIX, LOG_MAX_BYTES, FLUSH_EVERY)


def log(msg: str):
    if LOGGER:
        LOGGER.write("INFO ", msg)


def log_warn(msg: str):
    if LOGGER:
        LOGGER.write("WARN ", msg)


def log_error(msg: str):
    if LOGGER:
        LOGGER.write("ERROR", msg)


def term(line: str):
    """Print ONE line to the terminal with a short timestamp."""
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {line}", flush=True)


# ============================================================
# Clean Ctrl+C handling
# ============================================================
_running = True


def _signal_handler(signum, frame):
    global _running
    log(f"Signal {signum} received, stopping after current iteration")
    _running = False


signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)


# ============================================================
# DB ACCESS (read-only)
# ============================================================
def fetch_predicted_rows() -> list:
    """
    Read all rows from predicted.eventos as a list of dicts.

    The connection is opened READ ONLY, so this script can never modify the DB.
    A new connection is used per cycle to avoid stale/idle connections.
    """
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        conn.set_session(readonly=True, autocommit=True)
        cols = ", ".join(f'"{c}"' for c in EXPORT_COLUMNS)
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(f"SELECT {cols} FROM predicted.eventos ORDER BY created_at ASC")
            return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


# ============================================================
# JSON build + atomic write
# ============================================================
def _json_default(value):
    """Make non-JSON-native values serialisable."""
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    return str(value)


def build_payload(rows: list) -> dict:
    """
    Final JSON shape:
    {
        "generated_at": "2026-05-29T12:00:00",
        "count": 123,
        "events": [ { ...one object per prediction... } ]
    }
    To emit a bare array instead, just `return rows` here (and adjust the loop).
    """
    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "count": len(rows),
        "events": rows,
    }


def events_signature(rows: list) -> str:
    """
    MD5 of the events only (NOT the wrapper), so that generated_at changing each
    cycle does not force a rewrite. Used for change detection.
    """
    blob = json.dumps(rows, ensure_ascii=False, sort_keys=True, default=_json_default)
    return hashlib.md5(blob.encode("utf-8")).hexdigest()


def write_json_atomic(path: str, payload: dict):
    """Write JSON to a temp file in the same dir, then atomically replace."""
    directory = os.path.dirname(path) or "."
    os.makedirs(directory, exist_ok=True)

    fd, tmp_path = tempfile.mkstemp(prefix=".export_", suffix=".tmp", dir=directory)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2, default=_json_default)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, path)   # atomic on the same filesystem
    except Exception:
        if os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except Exception:
                pass
        raise


# ============================================================
# HTML build + atomic write
# ============================================================
_EMBED_MARKER = "/* __EMBEDDED_DATA__ */"

# Old init block present in soc_dashboard.html v1 (without the embedded check).
# build_html() replaces it automatically so the template can be either old or new.
_OLD_INIT = "showLoader();      // initial state while we try to fetch\nloadFromUrl();     // attempt automatic load\nstartPolling();"

_NEW_INIT = """\
/* ===========================================================================
   INIT — embedded data takes priority over fetch
   =========================================================================== */
(async function initDashboard() {
  // Wait for async gzip decompression (max 10 s, checks every 50 ms)
  if (typeof window.EMBEDDED_DATA === 'undefined') {
    for (let _i = 0; _i < 200; _i++) {
      await new Promise(r => setTimeout(r, 50));
      if (typeof window.EMBEDDED_DATA !== 'undefined') break;
    }
  }
  if (typeof window.EMBEDDED_DATA !== 'undefined') {
    // Data pre-injected by auto_export.py — no network request needed.
    // Works on SharePoint, local file://, or any static host.
    const { events, generatedAt } = normalize(window.EMBEDDED_DATA);
    setData(events, generatedAt, 'embedded');
  } else {
    // Fallback: attempt to fetch the companion JSON file (dev / local server).
    showLoader();
    loadFromUrl();
    startPolling();
  }
})();
"""


def build_html(rows: list) -> str:
    """
    Read the HTML template, inject the prediction data as an inline JS constant,
    and return the complete self-contained HTML string.

    The function handles two kinds of templates:
      - New template (soc_dashboard_template.html): already has the INIT block
        with the EMBEDDED_DATA check. Only the data marker is replaced.
      - Old template (soc_dashboard.html v1): has the old init block
        (showLoader / loadFromUrl / startPolling). build_html() replaces BOTH
        the data marker AND the old init, so the result is always correct
        regardless of which template version is in use.

    Events are capped at MAX_EMBEDDED_EVENTS (most recent by created_at) to
    avoid generating a multi-MB HTML file.
    """
    if not os.path.exists(TEMPLATE_HTML_PATH):
        raise FileNotFoundError(
            f"HTML template not found: {TEMPLATE_HTML_PATH}\n"
            "Place soc_dashboard_template.html (or the original soc_dashboard.html)\n"
            "in the templates/ folder and configure TEMPLATE_HTML_PATH."
        )

    with open(TEMPLATE_HTML_PATH, "r", encoding="utf-8") as f:
        template = f.read()

    # Normalize line endings (handles Windows CRLF templates saved on Windows)
    template = template.replace("\r\n", "\n").replace("\r", "\n")

    if _EMBED_MARKER not in template:
        raise ValueError(
            f"Marker '{_EMBED_MARKER}' not found in template {TEMPLATE_HTML_PATH}.\n"
            "The template must contain the marker inside its <script> block.\n"
            "Use soc_dashboard_template.html (generated by the TFG toolchain)."
        )

    # --- 1. Limit events if configured (most recent first) ---
    if MAX_EMBEDDED_EVENTS and len(rows) > MAX_EMBEDDED_EVENTS:
        # rows come from DB ordered created_at ASC; take the last N
        embed_rows = rows[-MAX_EMBEDDED_EVENTS:]
    else:
        embed_rows = rows

    # --- 2. Slim each event: drop null/empty fields to reduce HTML size ---
    # Strategy: only omit fields that are None or empty string. The dashboard JS
    # already handles missing keys gracefully (shows "–"). This typically cuts
    # the embedded JSON by 30–40% with no loss of information.
    slim_rows = [{k: v for k, v in e.items() if v is not None and v != ""} for e in embed_rows]

    # --- 3. Build the JS payload using compact JSON + gzip compression ---
    # Compact JSON + gzip typically reduces 20 MB → 2-3 MB, which SharePoint
    # can render without problems.
    payload = build_payload(slim_rows)
    payload["total_in_db"] = len(rows)   # full count so the footer can show it
    json_str = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), default=_json_default)
    compressed = gzip.compress(json_str.encode("utf-8"), compresslevel=9)
    b64 = base64.b64encode(compressed).decode("ascii")
    injection = (
        "(function(){"
        f'var _b="{b64}";'
        "var _s=atob(_b);"
        "var _u=new Uint8Array(_s.length);"
        "for(var _i=0;_i<_s.length;_i++)_u[_i]=_s.charCodeAt(_i);"
        "var _d=new DecompressionStream('gzip');"
        "var _w=_d.writable.getWriter();"
        "_w.write(_u);_w.close();"
        "new Response(_d.readable).text().then(function(t){"
        "window.EMBEDDED_DATA=JSON.parse(t);});"
        "})();"
    )

    # --- 3. Inject data marker ---
    html = template.replace(_EMBED_MARKER, injection, 1)

    # --- 4. Ensure the INIT block uses the embedded check ---
    # Use re.sub so whitespace / comment variations and CRLF→LF differences don't matter.
    # The pattern matches the three-line init block whether or not it has trailing comments.
    import re as _re
    _init_pattern = _re.compile(
        r"showLoader\(\)\s*;[^\n]*\n"   # showLoader(); // …
        r"\s*loadFromUrl\(\)\s*;[^\n]*\n"  # loadFromUrl(); // …
        r"\s*startPolling\(\)\s*;"         # startPolling();
    )
    if _init_pattern.search(html):
        html = _init_pattern.sub(_NEW_INIT, html, count=1)
    # Fallback: if the pattern above didn't match (very old template variant),
    # append the check just before </script> as a last resort.
    elif "typeof EMBEDDED_DATA" not in html:
        html = html.replace("</script>", _NEW_INIT + "\n</script>", 1)

    # --- 5. Set REFRESH_SECONDS = 0 if the template still has the old value ---
    html = html.replace(
        "const REFRESH_SECONDS = 10;",
        "const REFRESH_SECONDS = 0;  // embedded HTML is static",
        1,
    )

    return html


def write_html_atomic(path: str, html_content: str):
    """Write an HTML string to a temp file, then atomically replace."""
    directory = os.path.dirname(path) or "."
    os.makedirs(directory, exist_ok=True)

    fd, tmp_path = tempfile.mkstemp(prefix=".export_html_", suffix=".tmp", dir=directory)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(html_content)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, path)
    except Exception:
        if os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except Exception:
                pass
        raise


# ============================================================
# Main loop
# ============================================================
def main():
    _setup_logger()

    log(f"=== auto_export.py STARTED === "
        f"JSON={OUTPUT_PATH} HTML={OUTPUT_HTML_PATH} "
        f"INTERVAL={EXPORT_INTERVAL_SECONDS}s COLUMNS={len(EXPORT_COLUMNS)} "
        f"LOG_MAX={LOG_MAX_BYTES // (1024 * 1024)}MB")
    term(f"auto_export started. JSON: {OUTPUT_PATH} | HTML: {OUTPUT_HTML_PATH} | logs: {LOGGER.current_path}")

    last_signature = None

    while _running:
        try:
            rows = fetch_predicted_rows()
            sig = events_signature(rows)

            if sig == last_signature:
                term(f"no changes ({len(rows)} events)")
            else:
                # --- 1. Write JSON snapshot (kept for backward compat / debugging) ---
                payload = build_payload(rows)
                write_json_atomic(OUTPUT_PATH, payload)

                # --- 2. Write self-contained HTML snapshot (served by GET /export) ---
                html_content = build_html(rows)
                write_html_atomic(OUTPUT_HTML_PATH, html_content)

                embedded_n = min(len(rows), MAX_EMBEDDED_EVENTS) if MAX_EMBEDDED_EVENTS else len(rows)
                last_signature = sig
                log(f"OK export | {len(rows)} events total, {embedded_n} embedded -> "
                    f"JSON:{OUTPUT_PATH} HTML:{OUTPUT_HTML_PATH}")
                term(f"OK   {len(rows)} total / {embedded_n} embedded -> {OUTPUT_HTML_PATH}")

            time.sleep(EXPORT_INTERVAL_SECONDS)

        except KeyboardInterrupt:
            break
        except Exception as e:
            tb = " | ".join(traceback.format_exc().splitlines()[-4:])
            log_error(f"ERROR: {e} | {tb}")
            term(f"ERROR: {e} (see log)")
            time.sleep(SLEEP_AFTER_ERROR)

    log("=== auto_export.py STOPPED CLEANLY ===")
    if LOGGER:
        LOGGER.close()
    term("auto_export stopped cleanly.")


if __name__ == "__main__":
    main()