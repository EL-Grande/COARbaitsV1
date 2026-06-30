# Legacy — Pre-COAR Notify Architecture

These files represent the original MQTT-based architecture that preceded the current COAR Notify implementation. They are kept for historical reference only and are **not part of the running system**.

## Files

### `Baits2_mqtt_pubsub.py`
The original livestock event simulator. Generates random events (animal registration, movement, health treatment, death, ownership transfer) using [Faker](https://faker.readthedocs.io/) and publishes them to a Mosquitto MQTT broker on the `baits/events` topic.

Key concepts carried forward into the current system:
- Village → district mapping (Maun → North-West, Gaborone → South-East, etc.)
- Event types: registration, movement, ownership transfer (request → accept/reject → ack)
- Botswana cattle breed list (Brahman, Nguni, Tswana, Tuli, etc.)

**Not compatible with the current system** — uses MQTT transport and the old flat event schema (`event_type` strings, not AS2 JSON-LD).

### `Baits3_sqlite3.py`
The original COAR LDN engine. Subscribed to the MQTT broker, consumed events from `Baits2_mqtt_pubsub.py`, and wrote them to SQLite using an earlier schema (`notifications` table, different field names).

Key concepts carried forward:
- Immutable event ledger concept
- Inbox routing (VET vs. farmer)
- Movement permit workflow (request → approve/reject → ack)
- WAL mode + busy_timeout for SQLite concurrency

**Not compatible with the current system** — MQTT infrastructure retired; replaced by the FastAPI LDN inbox (`ldn_inbox/main.py`) which receives HTTP POST notifications directly.

## Why MQTT was retired

The MQTT pub/sub layer added infrastructure complexity (Mosquitto broker service) without contributing to the core research goal of demonstrating COAR Notify protocol compliance. The current system uses HTTP POST directly to LDN inbox endpoints, which is precisely what COAR Notify specifies.
