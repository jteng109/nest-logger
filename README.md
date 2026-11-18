# nest-logger

Logs Google Nest thermostats (Google SDM API) to SQLite and serves a chart UI.

- `nest-logger` — Nest Pub/Sub events in real time + a poll every 30 min, and every zigbee2mqtt
  device message from `mochi-mqtt` → `data/nest.db` (tables `readings`, `events`, `zigbee`)
- `nest-web` — chart page + JSON API on `nest-web:8087` (caddy_network) → https://nest.tengs.ca (LAN only)

## Files
- `.env` — OAuth client id/secret, SDM project id, Pub/Sub subscription (see `.env.example`)
- `data/nest_token.json` — OAuth refresh token (written by `auth`, refreshed by the logger)
- `data/pubsub-key.json` — Pub/Sub subscriber service-account key
- `data/nest.db` — readings + raw events

## Common tasks
    docker compose up -d --build          # after editing app/
    docker logs -f nest-logger
    docker compose run --rm -it nest-logger python nest_logger.py auth     # re-authorise (password change etc.)
    docker compose run --rm nest-logger python nest_logger.py devices
    # one-off: import zigbee2mqtt JSONL history (safe to re-run; duplicates are skipped)
    docker compose run --rm -v ../zigbee2mqtt/mqtt-logs/devices:/zlogs:ro nest-logger python nest_logger.py import-zigbee /zlogs
