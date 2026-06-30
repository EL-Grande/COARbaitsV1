# BAITS — Botswana Animal Identification and Traceability System

> The COAR Bots-egov-agri prototype — event-driven livestock traceability with KLD relevance ranking and UNESCO-CODATA DPs4Crises alignment.

[![COAR Notify](https://img.shields.io/badge/COAR%20Notify-1.0.1-blue)](https://coar-notify.net)
[![W3C LDN](https://img.shields.io/badge/W3C-LDN-blueviolet)](https://www.w3.org/TR/ldn/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

---

## Overview

COAR Bots-egov-agri is a research prototype showing how [COAR Notify](https://coar-notify.net) — an open protocol built on W3C Linked Data Notifications (LDN) and Activity Streams 2.0 (AS2) — can serve as the notification backbone for a national livestock traceability system during disease outbreaks and other agricultural crises.

**Scenario**: 5 farmers across Botswana districts × 5 animals each. An active FMD alert in the North-East District. A veterinary authority (VET) processing movement permit requests ordered by crisis relevance.


---

## Features

- **COAR Notify patterns**: Announcement (animal registration), Request–Review (movement permits), Offer (ownership transfer)
- **LDN-compliant inboxes**: `POST` (201 + Location), `GET` (LDP Container), individual notification dereference
- **KLD Relevance Ranking**: Kullback–Leibler Divergence over 6 Boolean features scores each notification against a crisis prior — surfacing the most atypical events first in the VET Work Queue
- **Streamlit dashboard**: Farmer view, VET Work Queue (KLD-ordered), KLD Analytics tab, Admin view
- **Colour-coded badges**: Red (KLD > 0.30) / Amber (0.15–0.30) / Green (< 0.15)

---

## Architecture

```
COARbaitsV1/
├── baits_coar/
│   ├── docker-compose.yml
│   ├── cleanup_db.py            # wipe DB and start fresh
│   ├── data/                    # shared SQLite volume (baits_system.db)
│   ├── ldn_inbox/               # FastAPI LDN receiver + KLD engine
│   │   ├── Dockerfile
│   │   └── main.py
│   └── dashboard/               # Streamlit UI
│       ├── Dockerfile
│       └── dashboard.py
├── BAITS_Documentation.tex      # Full documentation (Overleaf-compatible)
├── BAITS_Documentation.docx     # Full documentation (Word)
└── BAITS_KLD_Proposal.docx      # KLD ranking proposal document
```

---

## Quick Start

```bash
cd baits_coar
docker compose up --build
```

| Service | URL |
|---------|-----|
| Dashboard (Streamlit) | http://localhost:8501 |
| LDN Inbox API (FastAPI) | http://localhost:8000 |
| API docs (Swagger) | http://localhost:8000/docs |

---

## API Endpoints

| Method | Endpoint | Returns | Description |
|--------|----------|---------|-------------|
| `POST` | `/inbox/vet` | 201 + Location | Submit notification to VET inbox |
| `POST` | `/inbox/farmer/{id}` | 201 + Location | Submit to farmer inbox |
| `GET` | `/inbox/vet` | LDP Container | List all VET notifications |
| `GET` | `/inbox/farmer/{id}` | LDP Container | List farmer notifications |
| `GET` | `/notifications/{uuid}` | AS2 JSON-LD | Dereference notification |
| `GET` | `/analytics/top-events` | JSON | Top-k events by KLD score |

---

## KLD Relevance Ranking

Each incoming notification is scored using **D_KL(Q_e ‖ P)** over 6 Boolean features:

| Feature | Definition | Prior P(i) |
|---------|-----------|-----------|
| b1 | Origin district is HIGH risk | 0.20 |
| b2 | Livestock type is cattle | 0.55 |
| b3 | Active disease alert in origin district | 0.10 |
| b4 | Cross-district movement | 0.30 |
| b5 | High-risk purpose (slaughter/auction) | 0.20 |
| b6 | New record (registration or transfer offer) | 0.40 |

Higher KLD = more atypical = higher priority in the VET Work Queue.

---

## Resetting the Database

```bash
docker compose down
rm -f data/baits_system.db
docker compose up --build
```

Or with the cleanup script:

```bash
docker compose cp cleanup_db.py ldn_inbox:/app/cleanup_db.py
docker compose exec ldn_inbox python /app/cleanup_db.py
```

---

## Alignment with UNESCO-CODATA DPs4Crises

The COAR Bots-egov-agri prototype is explicitly linked to the [UNESCO-CODATA Data Policy for Times of Crisis (DPTC)](https://codata.org/initiatives/data-policy/dptc/) framework:

| DPs4Crises Principle | COAR Bots-egov-agri Implementation |
|----------------------|----------------------|
| Transparency | KLD formula fully documented; feature breakdown visible in dashboard |
| Accessibility | Standard LDN HTTP GET endpoints; no proprietary tools required |
| Interoperability | AS2 JSON-LD with W3C + COAR Notify contexts |
| Accountability | Immutable events ledger; composite inbox key; full workflow state |
| Real-Time Responsiveness | KLD computed at receipt; Work Queue ordered by score |
| Collaboration | Multi-actor notification graph; farmer-to-farmer offers; VET CC |
| FAIR Data | urn:uuid identifiers; rich provenance metadata; dereferenceable URIs |

---

## Tech Stack

- **FastAPI** — LDN receiver service
- **Streamlit** — Dashboard
- **SQLite** (WAL mode) — Shared database
- **Docker Compose** — Multi-service deployment
- **Python 3.10+**

---

## Project Evolution

The COAR Bots-egov-agri prototype went through two distinct architectural phases before reaching its current form.

**Phase 1 — MQTT simulation** (`legacy/`): The prototype began with a Mosquitto MQTT broker, a Faker-based event generator (`Baits2_mqtt_pubsub.py`) publishing livestock events to a `baits/events` topic, and a subscriber (`Baits3_sqlite3.py`) acting as the COAR LDN engine. The database 