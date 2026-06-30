---
output:
  html_document: default
  pdf_document: default
---
# BAITS COAR Notify Prototype

A minimal two-service prototype demonstrating COAR Notify–style
inbox/outbox notification patterns for the Botswana Animal
Identification and Traceability System (BAITS).

The old MQTT/Faker simulation pipeline has been removed — the
Streamlit dashboard is now the sole data-entry point, standing in for
real BAITS field actions since no live BAITS access is available.

## Structure

```
baits_coar/
├── docker-compose.yml
├── cleanup_db.py            # run manually to wipe the DB and start fresh
├── data/                    # shared SQLite file lives here (baits_system.db)
├── ldn_inbox/                # FastAPI COAR Notify inbox service
│   ├── Dockerfile
│   └── main.py
└── dashboard/                # Streamlit Farmer/Vet/Admin UI
    ├── Dockerfile
    └── dashboard.py
```

## Running it

From the `baits_coar/` directory:

```bash
docker compose up --build
```

- Dashboard: http://localhost:8501
- LDN inbox API: http://localhost:8000

Both containers mount `./data` so they share the same
`baits_system.db` file. `ldn_inbox` creates the schema on startup
(`events`, `movement_permits`, `inbox`, `outbox`).

## Resetting the database

Simplest option — since it's just a local SQLite file, stop
everything and delete it:

```bash
docker compose down
rm -f data/baits_system.db
docker compose up --build
```

`ldn_inbox`'s `init_db()` recreates the schema automatically on
startup, so deleting the file is the cleanest "start over" option.

Alternatively, run the cleanup script against the running container
without deleting the file (e.g. if you want to keep WAL settings
intact):

```bash
docker compose cp cleanup_db.py ldn_inbox:/app/cleanup_db.py
docker compose exec ldn_inbox python /app/cleanup_db.py
```

## What's intentionally NOT here anymore

- `ldn_engine.py` (MQTT consumer)
- `simulator.py` (Faker event generator)
- Mosquitto broker service

These were a generic pub/sub simulation layer with no real
relationship to the COAR Notify protocol, and have been retired now
that the goal is specifically demonstrating COAR Notify patterns
(actor → inbox → request/accept/reject) for BAITS.
