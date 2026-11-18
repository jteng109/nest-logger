# nest-logger

Home climate stack: logs a Google Nest thermostat (Google SDM API) and Zigbee room sensors
(Zigbee2MQTT) to one SQLite database, and serves a chart UI with a furnace fan timer.

| Service | What it does |
|---|---|
| `zigbee2mqtt` | Zigbee coordinator on `/dev/ttyUSB0` → MQTT. UI on `zigbee2mqtt:8080` (caddy_network, host port 8082) → https://zigbee.tengs.ca |
| `mochi-mqtt` | MQTT broker (host port 1883, no auth — LAN only) |
| `nest-logger` | Nest Pub/Sub events in real time + a poll every 30 min, and every Zigbee2MQTT device message → `data/nest.db` |
| `nest-web` | Chart page + JSON API + fan timer on `nest-web:8087` (caddy_network) → https://nest.tengs.ca (LAN only) |

## Data (`data/`, gitignored — never commit it, this repo is public)
- `nest.db` — tables `readings` (Nest snapshots), `events` (raw Nest Pub/Sub messages), `zigbee` (every Zigbee device message, history since 2026-04-30)
- `nest_token.json` — Nest OAuth refresh token (written by `auth`, refreshed by the logger/web)
- `pubsub-key.json` — Pub/Sub subscriber service-account key
- `zigbee2mqtt/` — Zigbee2MQTT state: `configuration.yaml` (**Zigbee network key**), `coordinator_backup.json`, `database.db` (paired devices)
- `.env` (next to compose.yml) — OAuth client id/secret, SDM project id, Pub/Sub subscription (see `.env.example`)

A copy of the Zigbee2MQTT config + coordinator backup is kept in the private homelab repo
(`backups/zigbee2mqtt/`) as the off-disk backup. Refresh it after pairing devices or changing settings:

    cp data/zigbee2mqtt/{configuration.yaml,coordinator_backup.json} ~/homelab/backups/zigbee2mqtt/ && \
      git -C ~/homelab add backups/zigbee2mqtt && git -C ~/homelab commit -m "Refresh Zigbee2MQTT backup" && git -C ~/homelab push

## Common tasks
    docker compose up -d --build          # after a git pull
    docker logs -f nest-logger            # or zigbee2mqtt, mochi-mqtt, nest-web
    docker compose run --rm -it nest-logger python nest_logger.py auth     # re-authorise Nest (password change etc.)
    docker compose run --rm nest-logger python nest_logger.py devices
    # import zigbee2mqtt JSONL logs (DIR/<device>/*.jsonl); exact duplicates are skipped
    docker compose run --rm -v /path/to/logs:/zlogs:ro nest-logger python nest_logger.py import-zigbee /zlogs
