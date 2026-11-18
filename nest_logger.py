#!/usr/bin/env python3
"""
Nest Logger — log Google Nest thermostats (SDM API) to SQLite.
Real-time via Pub/Sub events, with a periodic poll as a safety net.

Setup (once):
    pip install requests google-cloud-pubsub
    export NEST_CLIENT_ID="<your-oauth-client-id>.apps.googleusercontent.com"
    export NEST_CLIENT_SECRET="<your client secret>"
    export NEST_PROJECT_ID="<your-device-access-project-id>"
    # For events (optional — without these, `run` just polls):
    export GOOGLE_APPLICATION_CREDENTIALS="/path/to/nest-events-reader-key.json"
    export NEST_PUBSUB_SUBSCRIPTION="projects/<gcp-project>/subscriptions/<subscription>"

    python nest_logger.py auth      # prints the consent link, then paste the google.com URL you land on
    python nest_logger.py devices   # sanity check: dump current device state
    python nest_logger.py run       # events + poll (default every 30 min when events are on, 5 min otherwise)
    python nest_logger.py import-zigbee DIR   # one-off: load zigbee2mqtt JSONL history (DIR/<device>/*.jsonl)

    # Zigbee room sensors (optional): set MQTT_HOST and `run` also records zigbee2mqtt messages.
    export MQTT_HOST="mochi-mqtt"   # MQTT_PORT (1883), MQTT_TOPIC (zigbee2mqtt/#), MQTT_USER/MQTT_PASSWORD if needed

Tables in nest.db:
    readings — one full snapshot per poll or per event (source = 'poll' | 'event')
    events   — every raw Pub/Sub message, for anything you want to dig into later
    zigbee   — one row per zigbee2mqtt device message (temperature, humidity, battery, raw payload)

All timestamps are UTC, formatted like 2026-11-17T10:42:56.123+00:00 so they sort correctly as text.

Token is stored in nest_token.json (chmod 600). Keep it, the key file and your secret private.
"""
import fcntl
import json
import os
import re
import sqlite3
import sys
import threading
import time
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path

import requests

CLIENT_ID = os.environ.get("NEST_CLIENT_ID", "")
CLIENT_SECRET = os.environ.get("NEST_CLIENT_SECRET", "")
PROJECT_ID = os.environ.get("NEST_PROJECT_ID", "")
SUBSCRIPTION = os.environ.get("NEST_PUBSUB_SUBSCRIPTION", "")
REDIRECT_URI = "https://www.google.com"
SCOPE = "https://www.googleapis.com/auth/sdm.service"
TOKEN_FILE = Path(os.environ.get("NEST_TOKEN_FILE", "nest_token.json"))
DB_FILE = Path(os.environ.get("NEST_DB_FILE", "nest.db"))
_default_poll = "1800" if SUBSCRIPTION else "300"
POLL_SECONDS = int(os.environ.get("NEST_POLL_SECONDS", _default_poll))  # SDM rate limits: stay >= 60s
MQTT_HOST = os.environ.get("MQTT_HOST", "")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
MQTT_TOPIC = os.environ.get("MQTT_TOPIC", "zigbee2mqtt/#")
MQTT_BASE = MQTT_TOPIC.split("/")[0]

TOKEN_URL = "https://oauth2.googleapis.com/token"
API = "https://smartdevicemanagement.googleapis.com/v1"


def fmt_ts(dt):
    return dt.astimezone(timezone.utc).isoformat(timespec="milliseconds")


def now_iso():
    return fmt_ts(datetime.now(timezone.utc))


def norm_ts(s):
    """Normalise an event timestamp (e.g. '...T10:42:56.123456Z') to the same format as now_iso()."""
    try:
        return fmt_ts(datetime.fromisoformat(s.replace("Z", "+00:00")))
    except (AttributeError, ValueError):
        return now_iso()


def log(*a):
    print(now_iso(), *a, flush=True)


def need_env():
    missing = [n for n, v in [("NEST_CLIENT_ID", CLIENT_ID),
                              ("NEST_CLIENT_SECRET", CLIENT_SECRET),
                              ("NEST_PROJECT_ID", PROJECT_ID)] if not v]
    if missing:
        sys.exit(f"Set these environment variables first: {', '.join(missing)}")


# ---------- auth ----------

def auth_url():
    q = urllib.parse.urlencode({
        "redirect_uri": REDIRECT_URI,
        "access_type": "offline",
        "prompt": "consent",
        "client_id": CLIENT_ID,
        "response_type": "code",
        "scope": SCOPE,
    })
    return f"https://nestservices.google.com/partnerconnections/{PROJECT_ID}/auth?{q}"


def save_token(tok):
    TOKEN_FILE.write_text(json.dumps(tok, indent=2))
    try:
        os.chmod(TOKEN_FILE, 0o600)
    except OSError:
        pass


def cmd_auth():
    need_env()
    print("1) Open this link, choose your home/devices, and allow access:\n")
    print(auth_url(), "\n")
    print("2) You'll land on google.com. Paste that whole URL (or just the code) here.")
    raw = input("> ").strip()
    code = urllib.parse.parse_qs(urllib.parse.urlparse(raw).query).get("code", [raw])[0]
    r = requests.post(TOKEN_URL, data={
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "code": code,
        "grant_type": "authorization_code",
        "redirect_uri": REDIRECT_URI,
    }, timeout=30)
    if r.status_code != 200:
        sys.exit(f"Token exchange failed ({r.status_code}): {r.text}\n"
                 "Codes are single-use and expire quickly — rerun `auth` and paste promptly.")
    tok = r.json()
    if "refresh_token" not in tok:
        sys.exit("No refresh_token returned. Rerun `auth` (prompt=consent should force one).")
    tok["expires_at"] = time.time() + tok.get("expires_in", 3600) - 60
    save_token(tok)
    print(f"Saved tokens to {TOKEN_FILE}. Next: python {sys.argv[0]} devices")


_token_lock = threading.Lock()


class _FileLock:
    """Cross-process lock so the logger and web containers never refresh the token at the same time."""
    def __init__(self, path):
        self.path = path

    def __enter__(self):
        self.f = open(self.path, "a")
        fcntl.flock(self.f, fcntl.LOCK_EX)

    def __exit__(self, *exc):
        fcntl.flock(self.f, fcntl.LOCK_UN)
        self.f.close()


def access_token():
    with _token_lock, _FileLock(TOKEN_FILE.with_name(TOKEN_FILE.name + ".lock")):
        if not TOKEN_FILE.exists():
            sys.exit(f"No {TOKEN_FILE}. Run: python {sys.argv[0]} auth")
        tok = json.loads(TOKEN_FILE.read_text())
        if time.time() < tok.get("expires_at", 0):
            return tok["access_token"]
        r = requests.post(TOKEN_URL, data={
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "refresh_token": tok["refresh_token"],
            "grant_type": "refresh_token",
        }, timeout=30)
        if r.status_code != 200:
            raise RuntimeError(f"Refresh failed ({r.status_code}): {r.text} — "
                               "if 'invalid_grant', the refresh token is dead; rerun `auth`.")
        new = r.json()
        tok["access_token"] = new["access_token"]
        tok["expires_at"] = time.time() + new.get("expires_in", 3600) - 60
        save_token(tok)
        return tok["access_token"]


def list_devices():
    r = requests.get(f"{API}/enterprises/{PROJECT_ID}/devices",
                     headers={"Authorization": f"Bearer {access_token()}"}, timeout=30)
    r.raise_for_status()
    return r.json().get("devices", [])


MAX_FAN_SECONDS = 43200  # SDM limit for Fan.SetTimer


def set_fan_timer(device_id, seconds):
    """Run the fan for `seconds` (1..43200), or stop it with 0. Returns once the command is accepted;
    the new state arrives later as an event (it took ~45 s in testing)."""
    seconds = int(seconds)
    if not 0 <= seconds <= MAX_FAN_SECONDS:
        raise ValueError(f"duration must be 0..{MAX_FAN_SECONDS} seconds")
    params = {"timerMode": "ON", "duration": f"{seconds}s"} if seconds else {"timerMode": "OFF"}
    r = requests.post(f"{API}/enterprises/{PROJECT_ID}/devices/{device_id}:executeCommand",
                      headers={"Authorization": f"Bearer {access_token()}"},
                      json={"command": "sdm.devices.commands.Fan.SetTimer", "params": params}, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"Fan command failed ({r.status_code}): {r.text[:300]}")
    return params


# ---------- storage ----------

SCHEMA = """
CREATE TABLE IF NOT EXISTS readings (
    ts            TEXT NOT NULL,      -- UTC ISO8601 (event timestamp for events)
    source        TEXT NOT NULL DEFAULT 'poll',
    device_id     TEXT NOT NULL,
    name          TEXT,
    connectivity  TEXT,
    ambient_c     REAL,
    humidity_pct  REAL,
    mode          TEXT,               -- HEAT / COOL / HEATCOOL / OFF
    eco_mode      TEXT,
    hvac_status   TEXT,               -- HEATING / COOLING / OFF
    heat_sp_c     REAL,
    cool_sp_c     REAL,
    fan_timer     TEXT,
    raw           TEXT                -- full traits JSON
);
CREATE INDEX IF NOT EXISTS idx_readings_dev_ts ON readings(device_id, ts);
CREATE TABLE IF NOT EXISTS events (
    event_id      TEXT PRIMARY KEY,
    ts            TEXT NOT NULL,
    device_id     TEXT,
    traits        TEXT,               -- only the traits that changed
    payload       TEXT NOT NULL       -- full message JSON
);
CREATE INDEX IF NOT EXISTS idx_events_ts ON events(ts);
CREATE TABLE IF NOT EXISTS zigbee (
    ts            TEXT NOT NULL,      -- UTC ISO8601, when the message was received
    device_id     TEXT NOT NULL,      -- IEEE address (0x…) when known, else the friendly name
    name          TEXT,               -- friendly name without the address, e.g. '3rd Floor'
    temperature   REAL,
    humidity      REAL,
    battery       REAL,
    linkquality   REAL,
    payload       TEXT NOT NULL,      -- full message JSON
    UNIQUE (device_id, ts)
);
CREATE INDEX IF NOT EXISTS idx_zigbee_ts ON zigbee(ts);
"""


class Store:
    def __init__(self, path):
        self.db = sqlite3.connect(path, check_same_thread=False)
        self.lock = threading.Lock()
        with self.lock:
            cols = [r[1] for r in self.db.execute("PRAGMA table_info(readings)")]
            if cols and "source" not in cols:  # upgrade a db created by the older version
                self.db.execute("ALTER TABLE readings ADD COLUMN source TEXT NOT NULL DEFAULT 'poll'")
            self.db.executescript(SCHEMA)
            self.db.commit()
        self.state = {}  # device_id -> {"name": ..., "traits": {...}} — last known full state

    def insert_reading(self, ts, source, device_id):
        with self.lock:
            st = self.state[device_id]
            row = parse_traits(st["traits"]) | {"device_id": device_id, "name": st.get("name")}
            cols = ["ts", "source"] + list(row)
            self.db.execute(f"INSERT INTO readings ({','.join(cols)}) VALUES ({','.join('?' * len(cols))})",
                            [ts, source] + list(row.values()))
            self.db.commit()
        return row

    def insert_event(self, event_id, ts, device_id, traits, payload):
        with self.lock:
            cur = self.db.execute("INSERT OR IGNORE INTO events VALUES (?,?,?,?,?)",
                                  (event_id, ts, device_id, json.dumps(traits) if traits else None, payload))
            self.db.commit()
            return cur.rowcount == 1  # False = duplicate delivery

    def insert_zigbee(self, rows):
        """rows: iterable of zigbee_row() tuples. Duplicates (same device + ts) are ignored."""
        with self.lock:
            before = self.db.total_changes
            self.db.executemany("INSERT OR IGNORE INTO zigbee VALUES (?,?,?,?,?,?,?,?)", rows)
            self.db.commit()
            return self.db.total_changes - before


def parse_traits(t):
    g = lambda trait, key: t.get(f"sdm.devices.traits.{trait}", {}).get(key)
    return {
        "connectivity": g("Connectivity", "status"),
        "ambient_c": g("Temperature", "ambientTemperatureCelsius"),
        "humidity_pct": g("Humidity", "ambientHumidityPercent"),
        "mode": g("ThermostatMode", "mode"),
        "eco_mode": g("ThermostatEco", "mode"),
        "hvac_status": g("ThermostatHvac", "status"),
        "heat_sp_c": g("ThermostatTemperatureSetpoint", "heatCelsius"),
        "cool_sp_c": g("ThermostatTemperatureSetpoint", "coolCelsius"),
        "fan_timer": g("Fan", "timerMode"),
        "raw": json.dumps(t),
    }


def device_name(dev):
    rel = dev.get("parentRelations") or [{}]
    return dev.get("traits", {}).get("sdm.devices.traits.Info", {}).get("customName") or rel[0].get("displayName")


def summary(row):
    return f"{row['name']}: {row['ambient_c']}°C {row['humidity_pct']}% {row['hvac_status']}"


# ---------- poll ----------

def poll_once(store):
    devs = [d for d in list_devices() if d.get("type", "").endswith("THERMOSTAT")]
    ts = now_iso()
    out = []
    for d in devs:
        did = d["name"].split("/")[-1]
        with store.lock:
            store.state[did] = {"name": device_name(d), "traits": d.get("traits", {})}
        out.append(summary(store.insert_reading(ts, "poll", did)))
    log("poll", ", ".join(out) or "no thermostats returned")


# ---------- events ----------

def handle_message(store, message):
    payload = message.data.decode("utf-8")
    try:
        ev = json.loads(payload)
    except json.JSONDecodeError:
        log("event: non-JSON message, skipped")
        message.ack()
        return
    ts = norm_ts(ev.get("timestamp"))
    ru = ev.get("resourceUpdate") or {}
    did = (ru.get("name") or "").split("/")[-1] or None
    traits = ru.get("traits") or {}
    is_new = store.insert_event(ev.get("eventId") or message.message_id, ts, did, traits, payload)
    # Merge changed traits into last-known state and log a full snapshot row.
    if is_new and did and traits and did in store.state:
        with store.lock:
            st = store.state[did]["traits"]
            for k, v in traits.items():
                st[k] = {**st.get(k, {}), **v}
        log("event", summary(store.insert_reading(ts, "event", did)), "| changed:",
            ", ".join(k.split(".")[-1] for k in traits))
    elif is_new:
        log("event", ev.get("relationUpdate") and "relation update" or f"device {did} (not yet polled)")
    message.ack()


def start_subscriber(store):
    try:
        from google.cloud import pubsub_v1
    except ImportError:
        sys.exit("Events need: pip install google-cloud-pubsub")
    if not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
        sys.exit("Set GOOGLE_APPLICATION_CREDENTIALS to the service-account key JSON.")
    sub = pubsub_v1.SubscriberClient()
    future = sub.subscribe(SUBSCRIPTION, callback=lambda m: handle_message(store, m))
    log(f"listening on {SUBSCRIPTION}")
    return future


# ---------- zigbee (MQTT) ----------

_IEEE = re.compile(r"0x[0-9a-fA-F]{16}")


def zigbee_row(ts, topic, payload):
    """Turn one zigbee2mqtt device message into a row tuple, or None if it isn't a device state message."""
    parts = topic.split("/")
    if len(parts) != 2 or parts[0] != MQTT_BASE or parts[1] == "bridge" or not isinstance(payload, dict):
        return None  # skips bridge/…, …/set, …/get, …/availability and empty (deleted) payloads
    friendly = parts[1]
    m = _IEEE.search(friendly)
    device_id = m.group(0).lower() if m else friendly
    name = _IEEE.sub("", friendly).strip(" _-") or device_id
    num = lambda k: payload.get(k) if isinstance(payload.get(k), (int, float)) else None
    return (ts, device_id, name, num("temperature"), num("humidity"), num("battery"), num("linkquality"),
            json.dumps(payload, separators=(",", ":")))


def start_mqtt(store):
    try:
        import paho.mqtt.client as mqtt
    except ImportError:
        sys.exit("Zigbee logging needs: pip install paho-mqtt")

    def on_connect(client, userdata, flags, reason_code, properties=None):
        log(f"mqtt connected to {MQTT_HOST}:{MQTT_PORT} ({reason_code}); subscribing {MQTT_TOPIC}")
        client.subscribe(MQTT_TOPIC)

    def on_message(client, userdata, msg):
        if msg.retain:  # replayed last-known state on (re)connect, not a new reading
            return
        try:
            payload = json.loads(msg.payload or b"null")
        except ValueError:
            return
        row = zigbee_row(now_iso(), msg.topic, payload)
        if row:
            try:
                store.insert_zigbee([row])
            except sqlite3.Error as e:
                log("zigbee insert ERROR:", e)

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"nest-logger-{os.getpid()}")
    if os.environ.get("MQTT_USER"):
        client.username_pw_set(os.environ["MQTT_USER"], os.environ.get("MQTT_PASSWORD"))
    client.on_connect = on_connect
    client.on_message = on_message
    client.reconnect_delay_set(1, 60)
    client.connect_async(MQTT_HOST, MQTT_PORT, keepalive=60)
    client.loop_start()  # background thread; reconnects on its own
    return client


def cmd_import_zigbee():
    """Load zigbee2mqtt JSONL history: DIR/<device folder>/*.jsonl with lines {ts, topic, payload}."""
    if len(sys.argv) != 3:
        sys.exit(f"usage: python {sys.argv[0]} import-zigbee DIR")
    root = Path(sys.argv[2])
    store = Store(DB_FILE)
    total = added = 0
    for f in sorted(root.glob("*/*.jsonl")):
        batch = []
        with open(f, encoding="utf-8", errors="replace") as fh:
            for line in fh:
                try:
                    r = json.loads(line)
                except ValueError:
                    continue
                row = zigbee_row(norm_ts(r.get("ts")), r.get("topic", ""), r.get("payload"))
                if row:
                    batch.append(row)
        total += len(batch)
        added += store.insert_zigbee(batch)
    print(f"read {total} zigbee messages from {root}, added {added} new rows to {DB_FILE}")


# ---------- commands ----------

def cmd_devices():
    need_env()
    for d in list_devices():
        print(d.get("type"), json.dumps({"name": device_name(d)} | parse_traits(d.get("traits", {})) | {"raw": "…"},
                                        indent=2))


def cmd_run():
    need_env()
    store = Store(DB_FILE)
    poll_once(store)  # seeds full state (and SDM needs one devices.list before it sends events)
    future = start_subscriber(store) if SUBSCRIPTION else None
    if MQTT_HOST:
        start_mqtt(store)
    log(f"logging to {DB_FILE}; polling every {POLL_SECONDS}s"
        + ("" if future else " (events off — set NEST_PUBSUB_SUBSCRIPTION to enable)")
        + ("" if MQTT_HOST else " (zigbee off — set MQTT_HOST to enable)"))
    try:
        while True:
            time.sleep(POLL_SECONDS)
            if future and future.done():  # subscriber died — surface the error and restart it
                log("subscriber stopped:", future.exception(), "— restarting")
                future = start_subscriber(store)
            try:
                poll_once(store)
            except Exception as e:  # keep running through transient errors
                log("poll ERROR:", e)
    except KeyboardInterrupt:
        if future:
            future.cancel()


if __name__ == "__main__":
    cmds = {"auth": cmd_auth, "devices": cmd_devices, "run": cmd_run, "import-zigbee": cmd_import_zigbee}
    if len(sys.argv) < 2 or sys.argv[1] not in cmds:
        sys.exit(__doc__)
    cmds[sys.argv[1]]()
