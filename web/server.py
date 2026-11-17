#!/usr/bin/env python3
"""
Nest Logger web UI — serves a chart page and a small JSON API over nest.db.

    python web/server.py            # listens on 127.0.0.1:8087 (put Caddy in front)

Env: NEST_DB_FILE (default ../nest.db next to this folder), NEST_WEB_HOST, NEST_WEB_PORT.
The database is opened read-only; the logger keeps writing to it.
"""
import json
import os
import sqlite3
import time
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

HERE = Path(__file__).resolve().parent
DB_FILE = Path(os.environ.get("NEST_DB_FILE", HERE.parent / "nest.db")).resolve()
HOST = os.environ.get("NEST_WEB_HOST", "127.0.0.1")
PORT = int(os.environ.get("NEST_WEB_PORT", "8087"))
MAX_HOURS = 24 * 366


def db():
    return sqlite3.connect(f"file:{DB_FILE}?mode=ro", uri=True, timeout=10)


def to_ms(ts):
    return int(datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp() * 1000)


def effective_heat_sp(row_heat, eco_mode, raw):
    """In eco mode the thermostat heats to the eco setpoint, not the normal one."""
    if eco_mode and eco_mode != "OFF" and raw:
        try:
            return json.loads(raw).get("sdm.devices.traits.ThermostatEco", {}).get("heatCelsius", row_heat)
        except ValueError:
            pass
    return row_heat


def readings(hours):
    since_ms = (time.time() - hours * 3600) * 1000
    with db() as c:
        rows = c.execute(
            "SELECT ts, source, name, ambient_c, humidity_pct, mode, eco_mode, hvac_status, heat_sp_c, cool_sp_c, raw "
            "FROM readings ORDER BY ts").fetchall()
    out, before = [], None
    for ts, source, name, amb, hum, mode, eco, hvac, heat, cool, raw in rows:
        r = {"t": to_ms(ts), "src": source, "name": name, "temp": amb, "hum": hum, "mode": mode,
             "eco": eco, "hvac": hvac, "heat": effective_heat_sp(heat, eco, raw), "cool": cool}
        if r["t"] < since_ms:
            before = r  # last row before the window, so the chart starts with the right state
        else:
            out.append(r)
    if before:
        out.insert(0, before | {"t": int(since_ms)})
    return out


class Handler(BaseHTTPRequestHandler):
    def send(self, code, body, ctype):
        data = body if isinstance(body, bytes) else body.encode()
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        u = urlparse(self.path)
        try:
            if u.path in ("/", "/index.html"):
                self.send(200, (HERE / "index.html").read_bytes(), "text/html; charset=utf-8")
            elif u.path == "/api/readings":
                try:
                    hours = float(parse_qs(u.query).get("hours", ["24"])[0])
                except ValueError:
                    hours = 24
                hours = min(max(hours, 1), MAX_HOURS)
                self.send(200, json.dumps({"now": int(time.time() * 1000), "rows": readings(hours)}),
                          "application/json")
            elif u.path == "/healthz":
                self.send(200, "ok", "text/plain")
            else:
                self.send(404, "not found", "text/plain")
        except sqlite3.Error as e:
            self.send(503, json.dumps({"error": f"database: {e}"}), "application/json")

    def log_message(self, fmt, *args):  # quieter journal: skip successful requests
        if args and str(args[1]) not in ("200", "304"):
            super().log_message(fmt, *args)


if __name__ == "__main__":
    print(f"Serving on http://{HOST}:{PORT} (db: {DB_FILE})", flush=True)
    ThreadingHTTPServer((HOST, PORT), Handler).serve_forever()
