#!/usr/bin/env python3
"""
Nest Logger web UI — serves a chart page and a small JSON API over nest.db.

    python web/server.py            # listens on 127.0.0.1:8087 (put Caddy in front)

Env: NEST_DB_FILE (default ../nest.db next to this folder), NEST_WEB_HOST, NEST_WEB_PORT.
For the fan control it also needs the same NEST_* credentials and token file as the logger.
The database is opened read-only; the logger keeps writing to it.

API
    GET  /api/readings?hours=24   Nest thermostat rows
    GET  /api/zigbee?hours=24     Zigbee room sensors, per device
    GET  /api/fan                 fan timer state as last reported by the thermostat
    POST /api/fan {"minutes": n}  run the fan n minutes; 0 stops it (needs header X-Requested-With)
"""
import json
import os
import sqlite3
import sys
import time
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
import nest_logger  # noqa: E402  (credentials, token refresh, fan command)

DB_FILE = Path(os.environ.get("NEST_DB_FILE", HERE.parent / "nest.db")).resolve()
HOST = os.environ.get("NEST_WEB_HOST", "127.0.0.1")
PORT = int(os.environ.get("NEST_WEB_PORT", "8087"))
MAX_HOURS = 24 * 366


def db():
    return sqlite3.connect(f"file:{DB_FILE}?mode=ro", uri=True, timeout=10)


def to_ms(ts):
    return int(datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp() * 1000)


def since_iso(hours):
    return nest_logger.fmt_ts(datetime.now(timezone.utc) - timedelta(hours=hours))


def effective_heat_sp(row_heat, eco_mode, raw):
    """In eco mode the thermostat heats to the eco setpoint, not the normal one."""
    if eco_mode and eco_mode != "OFF" and raw:
        try:
            return json.loads(raw).get("sdm.devices.traits.ThermostatEco", {}).get("heatCelsius", row_heat)
        except ValueError:
            pass
    return row_heat


def readings(hours):
    since = since_iso(hours)
    cols = "ts, source, name, ambient_c, humidity_pct, mode, eco_mode, hvac_status, heat_sp_c, cool_sp_c, raw"
    with db() as c:
        before = c.execute(f"SELECT {cols} FROM readings WHERE ts < ? ORDER BY ts DESC LIMIT 1", (since,)).fetchone()
        rows = c.execute(f"SELECT {cols} FROM readings WHERE ts >= ? ORDER BY ts", (since,)).fetchall()
    out = []
    for ts, source, name, amb, hum, mode, eco, hvac, heat, cool, raw in ([before] if before else []) + rows:
        out.append({"t": to_ms(ts), "src": source, "name": name, "temp": amb, "hum": hum, "mode": mode,
                    "eco": eco, "hvac": hvac, "heat": effective_heat_sp(heat, eco, raw), "cool": cool})
    if before:
        out[0]["t"] = to_ms(since)  # the chart starts with the state that was current at the window start
    return out


def zigbee(hours):
    """Per device: latest name, latest reading (for tiles) and the series in the window."""
    since = since_iso(hours)
    with db() as c:
        if not c.execute("SELECT 1 FROM sqlite_master WHERE name='zigbee'").fetchone():
            return []
        devs = c.execute("""
            SELECT z.device_id, z.name, z.ts, z.temperature, z.humidity, z.battery
            FROM zigbee z JOIN (SELECT device_id, MAX(ts) m FROM zigbee
                                WHERE temperature IS NOT NULL GROUP BY device_id) l
              ON z.device_id = l.device_id AND z.ts = l.m""").fetchall()
        out = []
        for dev_id, name, ts, temp, hum, batt in devs:
            # the device's most recent friendly name (older rows may predate its rename)
            name = c.execute("SELECT name FROM zigbee WHERE device_id=? ORDER BY ts DESC LIMIT 1",
                             (dev_id,)).fetchone()[0] or name
            before = c.execute("""SELECT ts, temperature, humidity FROM zigbee WHERE device_id=? AND ts < ?
                                  AND temperature IS NOT NULL ORDER BY ts DESC LIMIT 1""", (dev_id, since)).fetchone()
            pts = c.execute("""SELECT ts, temperature, humidity FROM zigbee WHERE device_id=? AND ts >= ?
                               AND temperature IS NOT NULL ORDER BY ts""", (dev_id, since)).fetchall()
            series = ([[to_ms(since), before[1], before[2]]] if before else []) + \
                     [[to_ms(t), te, hu] for t, te, hu in pts]
            out.append({"id": dev_id, "name": name,
                        "last": {"t": to_ms(ts), "temp": temp, "hum": hum, "battery": batt},
                        "points": series})
    return sorted(out, key=lambda d: d["name"].lower())


def fan_state():
    """Fan timer as last reported (poll or event). timerTimeout is when a running timer ends."""
    with db() as c:
        row = c.execute("SELECT ts, device_id, raw FROM readings ORDER BY ts DESC LIMIT 1").fetchone()
    if not row:
        return {"known": False}
    ts, device_id, raw = row
    fan = json.loads(raw or "{}").get("sdm.devices.traits.Fan", {})
    timeout = fan.get("timerTimeout")
    return {"known": True, "device_id": device_id, "reported_at": to_ms(ts),
            "mode": fan.get("timerMode", "OFF"), "until": to_ms(timeout) if timeout else None}


class Handler(BaseHTTPRequestHandler):
    def send(self, code, body, ctype="application/json"):
        data = body if isinstance(body, bytes) else (body if isinstance(body, str) else json.dumps(body)).encode()
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(data)

    def hours(self, u):
        try:
            h = float(parse_qs(u.query).get("hours", ["24"])[0])
        except ValueError:
            h = 24
        return min(max(h, 1), MAX_HOURS)

    def do_GET(self):
        u = urlparse(self.path)
        try:
            if u.path in ("/", "/index.html"):
                self.send(200, (HERE / "index.html").read_bytes(), "text/html; charset=utf-8")
            elif u.path == "/api/readings":
                self.send(200, {"now": int(time.time() * 1000), "rows": readings(self.hours(u))})
            elif u.path == "/api/zigbee":
                self.send(200, {"now": int(time.time() * 1000), "devices": zigbee(self.hours(u))})
            elif u.path == "/api/fan":
                self.send(200, fan_state() | {"now": int(time.time() * 1000)})
            elif u.path == "/healthz":
                self.send(200, "ok", "text/plain")
            else:
                self.send(404, "not found", "text/plain")
        except sqlite3.Error as e:
            self.send(503, {"error": f"database: {e}"})

    def do_POST(self):
        u = urlparse(self.path)
        if u.path != "/api/fan":
            return self.send(404, "not found", "text/plain")
        # A custom header + JSON body forces a CORS preflight, which this server never approves,
        # so other web pages open in a browser on the LAN can't trigger the fan.
        if self.headers.get("X-Requested-With") != "nest-ui" or \
                not self.headers.get("Content-Type", "").startswith("application/json"):
            return self.send(403, {"error": "forbidden"})
        try:
            body = json.loads(self.rfile.read(min(int(self.headers.get("Content-Length", 0)), 1024)) or b"{}")
            minutes = float(body["minutes"])
            if not 0 <= minutes <= nest_logger.MAX_FAN_SECONDS / 60:
                raise ValueError
        except (ValueError, KeyError, TypeError):
            return self.send(400, {"error": f"minutes must be 0–{nest_logger.MAX_FAN_SECONDS // 60}"})
        try:
            state = fan_state()
            if not state.get("known"):
                return self.send(503, {"error": "no thermostat reading yet"})
            params = nest_logger.set_fan_timer(state["device_id"], round(minutes * 60))
            print(nest_logger.now_iso(), "fan command", params, flush=True)
            self.send(200, {"ok": True, "requested": params, "at": int(time.time() * 1000)})
        except Exception as e:  # report API/auth errors to the page
            self.send(502, {"error": str(e)[:300]})

    def log_message(self, fmt, *args):  # quieter journal: skip successful requests
        if args and str(args[1]) not in ("200", "304"):
            super().log_message(fmt, *args)


if __name__ == "__main__":
    print(f"Serving on http://{HOST}:{PORT} (db: {DB_FILE})", flush=True)
    ThreadingHTTPServer((HOST, PORT), Handler).serve_forever()
