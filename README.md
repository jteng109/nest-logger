# nest-logger

Logs Google Nest thermostats (Google SDM API) to SQLite and serves a chart UI.

- `nest-logger` — Pub/Sub events in real time + a poll every 30 min → `data/nest.db`
- `nest-web` — chart page + JSON API on `nest-web:8087` (caddy_network) → https://nest.tengs.ca

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
