import paho.mqtt.client as mqtt
import json
import sqlite3
import time
import uuid

# =========================================================
# LDN INBOX REGISTRY (COAR DELIVERY LAYER)
# =========================================================
INBOXES = {
    "VET_OFFICER": "inbox/vet",
    "FARMER": "inbox/farmer"
}

DB_PATH = "/app/data/baits_system.db"


# =========================================================
# DB INIT
# =========================================================
def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("PRAGMA busy_timeout=5000;")

    # EVENT LEDGER (COAR SOURCE OF TRUTH)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS events (
        event_id TEXT PRIMARY KEY,
        event_type TEXT,
        timestamp TEXT,
        payload TEXT,
        status TEXT
    )
    """)

    # WORKFLOW STATE (PERMITS)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS movement_permits (
        permit_id TEXT PRIMARY KEY,
        tag_id TEXT,
        to_location TEXT,
        requested_by TEXT,
        status TEXT,
        request_timestamp TEXT,
        decision_timestamp TEXT,
        reason TEXT
    )
    """)

    # LDN INBOX (NOTIFICATIONS)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS notifications (
        notification_id TEXT PRIMARY KEY,
        recipient TEXT,
        inbox TEXT,
        notification_type TEXT,
        payload TEXT,
        is_read INTEGER DEFAULT 0,
        created_at TEXT
    )
    """)

    conn.commit()
    conn.close()
    print("✅ COAR LDN DB initialized")


init_db()


# =========================================================
# SAFE DB WRITER (handles SQLite locks)
# =========================================================
def safe_db_write(fn, retries=5):
    for attempt in range(retries):
        try:
            conn = sqlite3.connect(DB_PATH, timeout=30)
            conn.execute("PRAGMA busy_timeout=5000;")
            cur = conn.cursor()

            fn(cur)

            conn.commit()
            conn.close()
            return

        except sqlite3.OperationalError as e:
            if "locked" in str(e).lower():
                time.sleep(0.2 * (attempt + 1))
            else:
                raise


# =========================================================
# LDN NOTIFICATION CREATOR (INBOX WRITE)
# =========================================================
def create_notification(notification_id, recipient, inbox, notification_type, payload):

    def write(cur):
        cur.execute("""
            INSERT OR REPLACE INTO notifications
            (notification_id, recipient, inbox, notification_type, payload, is_read, created_at)
            VALUES (?, ?, ?, ?, ?, 0, ?)
        """, (
            notification_id,
            recipient,
            inbox,
            notification_type,
            json.dumps(payload),
            time.strftime("%Y-%m-%dT%H:%M:%SZ")
        ))

    safe_db_write(write)


# =========================================================
# ACK PUBLISHER (COAR FEEDBACK LOOP)
# =========================================================
def publish_ack(client, permit_id, tag_id, status):

    ack = {
        "event_id": f"ACK-{permit_id}-{uuid.uuid4().hex[:6]}",
        "event_type": "MOVEMENT_PERMIT_ACK",
        "permit_id": permit_id,
        "tag_id": tag_id,
        "status": status,
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ")
    }

    client.publish("baits/events", json.dumps(ack))


# =========================================================
# NORMALIZE EVENT
# =========================================================
def normalize(data):
    return {
        "event_id": data.get("event_id", str(uuid.uuid4())),
        "event_type": data.get("event_type", "UNKNOWN"),
        "timestamp": data.get("timestamp", time.strftime("%Y-%m-%dT%H:%M:%SZ")),
        "payload": json.dumps(data),
        "status": data.get("status", "NEW")
    }


# =========================================================
# MQTT HANDLER (CORE COAR LDN ENGINE)
# =========================================================
def on_message(client, userdata, msg):

    try:
        data = json.loads(msg.payload.decode())

        event_type = data.get("event_type", "UNKNOWN")
        tag_id = data.get("tag_id", "UNKNOWN")
        permit_id = data.get("permit_id") or data.get("event_id")

        print(f"📩 EVENT: {event_type} | TAG: {tag_id}")

        evt = normalize(data)

        # =====================================================
        # 1. COAR EVENT LEDGER (IMMUTABLE RECORD)
        # =====================================================
        def write_event(cur):
            cur.execute("""
                INSERT OR REPLACE INTO events
                (event_id, event_type, timestamp, payload, status)
                VALUES (?, ?, ?, ?, ?)
            """, (
                evt["event_id"],
                evt["event_type"],
                evt["timestamp"],
                evt["payload"],
                evt["status"]
            ))

        safe_db_write(write_event)

        # =====================================================
        # 2. MOVEMENT REQUEST → VET INBOX
        # =====================================================
        if event_type == "ANIMAL_MOVEMENT_REQUEST":

            destination = data.get("to")
            requested_by = data.get("requested_by", "FARMER")

            def write_permit(cur):
                cur.execute("""
                    INSERT OR REPLACE INTO movement_permits
                    (permit_id, tag_id, to_location, requested_by, status, request_timestamp)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    permit_id,
                    tag_id,
                    destination,
                    requested_by,
                    "PENDING",
                    evt["timestamp"]
                ))

            safe_db_write(write_permit)

            create_notification(
                notification_id=f"NOTIF-{permit_id}",
                recipient="VET_OFFICER",
                inbox=INBOXES["VET_OFFICER"],
                notification_type="MOVEMENT_PERMIT_REQUEST",
                payload=data
            )

            print(f"📨 Routed to VET inbox: {permit_id}")

        # =====================================================
        # 3. APPROVAL → FARMER INBOX
        # =====================================================
        elif event_type == "MOVEMENT_PERMIT_APPROVED":

            def approve(cur):
                cur.execute("""
                    UPDATE movement_permits
                    SET status='APPROVED',
                        decision_timestamp=?
                    WHERE permit_id=?
                """, (evt["timestamp"], permit_id))

            safe_db_write(approve)

            conn = sqlite3.connect(DB_PATH)
            cur = conn.cursor()
            cur.execute("""
                SELECT requested_by, tag_id FROM movement_permits WHERE permit_id=?
            """, (permit_id,))
            row = cur.fetchone()
            conn.close()

            if row:
                farmer, tag = row

                create_notification(
                    notification_id=f"APP-{permit_id}",
                    recipient=farmer,
                    inbox=INBOXES["FARMER"],
                    notification_type="MOVEMENT_PERMIT_APPROVED",
                    payload={
                        "permit_id": permit_id,
                        "tag_id": tag,
                        "status": "APPROVED"
                    }
                )

            publish_ack(client, permit_id, tag_id, "APPROVED")
            print("✅ APPROVED ROUTED")

        # =====================================================
        # 4. REJECTION → FARMER INBOX
        # =====================================================
        elif event_type == "MOVEMENT_PERMIT_REJECTED":

            reason = data.get("reason", "No reason provided")

            def reject(cur):
                cur.execute("""
                    UPDATE movement_permits
                    SET status='REJECTED',
                        reason=?,
                        decision_timestamp=?
                    WHERE permit_id=?
                """, (reason, evt["timestamp"], permit_id))

            safe_db_write(reject)

            conn = sqlite3.connect(DB_PATH)
            cur = conn.cursor()
            cur.execute("""
                SELECT requested_by, tag_id FROM movement_permits WHERE permit_id=?
            """, (permit_id,))
            row = cur.fetchone()
            conn.close()

            if row:
                farmer, tag = row

                create_notification(
                    notification_id=f"REJ-{permit_id}",
                    recipient=farmer,
                    inbox=INBOXES["FARMER"],
                    notification_type="MOVEMENT_PERMIT_REJECTED",
                    payload={
                        "permit_id": permit_id,
                        "tag_id": tag,
                        "reason": reason,
                        "status": "REJECTED"
                    }
                )

            publish_ack(client, permit_id, tag_id, "REJECTED")
            print("❌ REJECTED ROUTED")

    except Exception as e:
        print("❌ LDN ENGINE ERROR:", e)


# =========================================================
# MQTT BOOTSTRAP
# =========================================================
client = mqtt.Client()
client.on_message = on_message

client.connect("mosquitto", 1883, 60)
client.subscribe("baits/events")

print("📡 COAR LDN v2 Inbox Engine Running")
client.loop_forever()