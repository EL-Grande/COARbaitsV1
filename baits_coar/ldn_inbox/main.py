from fastapi import FastAPI, Request, BackgroundTasks, Response
from fastapi.responses import JSONResponse
import sqlite3
import json
import math
import time
import uuid
import requests as http_req

app = FastAPI()
DB_PATH = "/app/data/baits_system.db"

# COAR Notify preferred context URI (https://purl.org/coar/notify is deprecated)
AS2_CONTEXT = [
    "https://www.w3.org/ns/activitystreams",
    "https://coar-notify.net"
]

LDN_BASE = "http://ldn_inbox:8000"

FARMERS = {
    f"farmer_{i}": {
        "id": f"https://baits.bw/actors/farmer-{i}",
        "name": f"Farmer {i}",
        "service_id": f"https://baits.bw/services/farmer-{i}-portal",
        "inbox": f"{LDN_BASE}/inbox/farmer/farmer_{i}"
    }
    for i in range(1, 6)
}

# ── KLD Relevance Ranking ─────────────────────────────────────────────────────
# Each farmer's home district (used for b1 / b3 / b4 feature extraction).
FARMER_DISTRICTS = {
    "farmer_1": "South-East",   # Gaborone — low risk
    "farmer_2": "Central",      # Serowe   — medium risk
    "farmer_3": "North-East",   # Francistown — HIGH RISK / active FMD alert
    "farmer_4": "Central",      # Palapye  — medium risk
    "farmer_5": "Southern",     # Kanye    — low risk
}

# Breeds that classify as cattle (bovine) for feature b2.
CATTLE_BREEDS = {
    "Tswana", "Brahman", "Nguni", "Bonsmara", "Hereford", "Simmental",
    "Charolais", "Angus", "Afrikaner", "Boran", "Simentaler", "Tuli",
    "Drakensberger", "Beefmaster", "Fleckvieh", "Limousin", "Shorthorn",
    "Mashona", "Santa Gertrudis", "Murray Grey",
}

# Prior P = [b1, b2, b3, b4, b5, b6] — baseline probability each feature is active.
KLD_PRIOR = [0.20, 0.55, 0.10, 0.30, 0.20, 0.40]
KLD_ALPHA  = 0.1   # Laplace smoothing constant

# Each farmer keeps a distinct herd — different breeds per farmer
FARMER_BREEDS = {
    1: ["Tswana", "Brahman", "Nguni", "Bonsmara", "Hereford"],
    2: ["Simmental", "Charolais", "Angus", "Afrikaner", "Mashona"],
    3: ["Boran", "Simentaler", "Tuli", "Drakensberger", "Beefmaster"],
    4: ["Brahman", "Fleckvieh", "Limousin", "Shorthorn", "Afrikaner"],
    5: ["Nguni", "Tswana", "Santa Gertrudis", "Murray Grey", "Bonsmara"],
}


# =========================================================
# DB INIT
# =========================================================
def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("PRAGMA busy_timeout=5000;")

    # Global event ledger — every notification received is stored here.
    cur.execute("""
    CREATE TABLE IF NOT EXISTS events (
        event_id  TEXT PRIMARY KEY,
        notify_type TEXT,
        pattern   TEXT,
        timestamp TEXT,
        payload   TEXT
    )
    """)

    # Workflow state for the Request Review pattern (movement permits).
    cur.execute("""
    CREATE TABLE IF NOT EXISTS movement_permits (
        permit_id          TEXT PRIMARY KEY,
        tag_id             TEXT,
        to_location        TEXT,
        requested_by       TEXT,
        status             TEXT,
        request_timestamp  TEXT,
        decision_timestamp TEXT,
        reason             TEXT
    )
    """)

    # Per-actor inboxes (received messages).
    cur.execute("""
    CREATE TABLE IF NOT EXISTS inbox (
        message_id TEXT PRIMARY KEY,
        recipient  TEXT,
        notify_type TEXT,
        payload    TEXT,
        is_read    INTEGER DEFAULT 0,
        created_at TEXT
    )
    """)

    # Per-actor outboxes (sent messages).
    cur.execute("""
    CREATE TABLE IF NOT EXISTS outbox (
        message_id      TEXT PRIMARY KEY,
        sender          TEXT,
        notify_type     TEXT,
        payload         TEXT,
        delivery_status TEXT,
        created_at      TEXT
    )
    """)

    # Animal registry — ownership changes as transfers are accepted.
    cur.execute("""
    CREATE TABLE IF NOT EXISTS animals (
        tag_id       TEXT PRIMARY KEY,
        owner_id     TEXT,
        breed        TEXT,
        registered_at TEXT
    )
    """)

    # State for the Offer (animal transfer) pattern.
    cur.execute("""
    CREATE TABLE IF NOT EXISTS transfer_offers (
        offer_id    TEXT PRIMARY KEY,
        tag_id      TEXT,
        from_farmer TEXT,
        to_farmer   TEXT,
        status      TEXT,
        offered_at  TEXT,
        decided_at  TEXT,
        summary     TEXT
    )
    """)

    # ── KLD support tables ─────────────────────────────────────────────────────
    cur.execute("""
    CREATE TABLE IF NOT EXISTS district_risk (
        district_name TEXT PRIMARY KEY,
        risk_level    TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS disease_alerts (
        alert_id      INTEGER PRIMARY KEY AUTOINCREMENT,
        district_name TEXT,
        disease       TEXT,
        start_date    TEXT,
        end_date      TEXT
    )
    """)

    # Add relevance_score to inbox — safe to run on an existing DB.
    try:
        cur.execute("ALTER TABLE inbox ADD COLUMN relevance_score REAL DEFAULT 0.0")
    except Exception:
        pass  # column already exists

    # Seed district risk register.
    for district, level in [
        ("North-East", "HIGH"),
        ("North-West", "LOW"),
        ("Central",    "MEDIUM"),
        ("South-East", "LOW"),
        ("Southern",   "LOW"),
        ("Kweneng",    "MEDIUM"),
    ]:
        cur.execute(
            "INSERT OR IGNORE INTO district_risk (district_name, risk_level) VALUES (?, ?)",
            (district, level)
        )

    # Seed one active FMD alert in North-East for the demo.
    cur.execute("""
        INSERT INTO disease_alerts (district_name, disease, start_date, end_date)
        SELECT 'North-East', 'FMD', '2026-06-01', '2026-12-31'
        WHERE NOT EXISTS (
            SELECT 1 FROM disease_alerts WHERE district_name='North-East' AND disease='FMD'
        )
    """)

    conn.commit()

    # Seed 5 animals per farmer on first run — each farmer has a distinct herd.
    now = time.strftime("%Y-%m-%dT%H:%M:%SZ")
    for fn in range(1, 6):
        for an, breed in enumerate(FARMER_BREEDS[fn], start=1):
            cur.execute(
                "INSERT OR IGNORE INTO animals (tag_id, owner_id, breed, registered_at) VALUES (?, ?, ?, ?)",
                (f"F{fn}-{an:03d}", f"farmer_{fn}", breed, now)
            )

    conn.commit()
    conn.close()
    print("✅ DB initialized — 5 farmers × 5 animals seeded")


init_db()


# =========================================================
# DB HELPERS
# =========================================================
def db_exec(query, params=()):
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("PRAGMA busy_timeout=5000;")
    cur = conn.cursor()
    cur.execute(query, params)
    conn.commit()
    conn.close()


def db_query_one(query, params=()):
    conn = sqlite3.connect(DB_PATH, timeout=30)
    cur = conn.cursor()
    cur.execute(query, params)
    row = cur.fetchone()
    conn.close()
    return row


# =========================================================
# TYPE HELPERS
# =========================================================
def get_types(data: dict) -> set:
    """Normalise the AS2 type field to a Python set."""
    t = data.get("type", [])
    return {t} if isinstance(t, str) else set(t)


def primary_as2_type(data: dict) -> str:
    """Return the primary AS2 type (first non coar-notify: entry)."""
    t = data.get("type", "Unknown")
    if isinstance(t, list):
        for item in t:
            if not item.startswith("coar-notify:"):
                return item
        return t[0] if t else "Unknown"
    return t


def coar_subtype(data: dict) -> str:
    """Return the coar-notify: subtype, if any, without the prefix."""
    t = data.get("type", [])
    if isinstance(t, str):
        t = [t]
    for item in t:
        if item.startswith("coar-notify:"):
            return item.replace("coar-notify:", "")
    return ""


# =========================================================
# KLD RELEVANCE SCORING
# =========================================================
def _actor_district(data: dict) -> str:
    """Map the event's actor.id → farmer_id → home district."""
    actor_id = (data.get("actor") or {}).get("id", "")
    farmer_id = next(
        (fid for fid, info in FARMERS.items() if info["id"] == actor_id), None
    )
    return FARMER_DISTRICTS.get(farmer_id, "") if farmer_id else ""


def extract_features(data: dict) -> list:
    """
    Return a 6-element list of Boolean (0/1) feature values for the event.
    b1 — origin district is HIGH risk
    b2 — livestock type is cattle
    b3 — active disease alert in origin district
    b4 — cross-district movement
    b5 — high-risk purpose (slaughter or auction)
    b6 — new record (registration or new transfer offer)
    """
    obj     = data.get("object") or {}
    types   = get_types(data)
    breed   = obj.get("breed", "")
    purpose = (obj.get("purpose") or "").lower()

    origin_district = _actor_district(data)
    dest_district   = obj.get("district", "")

    # b1: high-risk origin district?
    row = db_query_one(
        "SELECT risk_level FROM district_risk WHERE district_name = ?",
        (origin_district,)
    )
    b1 = 1 if (row and row[0] == "HIGH") else 0

    # b2: cattle (bovine breed)?
    b2 = 1 if breed in CATTLE_BREEDS else 0

    # b3: active disease alert in origin district?
    today = time.strftime("%Y-%m-%d")
    row3 = db_query_one(
        "SELECT 1 FROM disease_alerts "
        "WHERE district_name = ? AND start_date <= ? AND end_date >= ?",
        (origin_district, today, today)
    )
    b3 = 1 if row3 else 0

    # b4: cross-district movement?
    b4 = 1 if (origin_district and dest_district
               and origin_district != dest_district) else 0

    # b5: high-risk movement purpose (slaughter or auction)?
    b5 = 1 if any(w in purpose for w in ("slaughter", "auction")) else 0

    # b6: new record (registration or new transfer offer)?
    b6 = 1 if (
        ("Announce" in types and "coar-notify:IngestAction" in types)
        or "Offer" in types
    ) else 0

    return [b1, b2, b3, b4, b5, b6]


def compute_kld(data: dict) -> float:
    """
    KLD relevance score: D_KL(Q_e || P)
    Q_e is the Laplace-smoothed distribution over the event's Boolean features.
    P is the prior (KLD_PRIOR). Higher score = more atypical = higher relevance.
    """
    b = extract_features(data)
    denom = sum(b) + len(b) * KLD_ALPHA
    Q = [(bi + KLD_ALPHA) / denom for bi in b]

    score = 0.0
    for q_i, p_i in zip(Q, KLD_PRIOR):
        if q_i > 0 and p_i > 0:
            score += q_i * math.log2(q_i / p_i)
    return round(score, 4)


# =========================================================
# LEDGER + INBOX/OUTBOX WRITERS
# =========================================================
def write_event(data: dict):
    event_id = data.get("id", f"urn:uuid:{uuid.uuid4()}")
    timestamp = data.get("timestamp", time.strftime("%Y-%m-%dT%H:%M:%SZ"))
    db_exec("""
        INSERT OR REPLACE INTO events (event_id, notify_type, pattern, timestamp, payload)
        VALUES (?, ?, ?, ?, ?)
    """, (event_id, primary_as2_type(data), coar_subtype(data), timestamp, json.dumps(data)))


def write_inbox(data: dict, recipient: str):
    # Composite key: notification_id|recipient — allows the same notification
    # to land in multiple inboxes (e.g. farmer + VET) without one replacing the other.
    notification_id = data.get("id", f"urn:uuid:{uuid.uuid4()}")
    message_id = f"{notification_id}|{recipient}"
    score = compute_kld(data)
    db_exec("""
        INSERT OR IGNORE INTO inbox
            (message_id, recipient, notify_type, payload, is_read, created_at, relevance_score)
        VALUES (?, ?, ?, ?, 0, ?, ?)
    """, (message_id,
          recipient,
          primary_as2_type(data),
          json.dumps(data),
          time.strftime("%Y-%m-%dT%H:%M:%SZ"),
          score))


def write_outbox(data: dict, sender: str):
    db_exec("""
        INSERT OR REPLACE INTO outbox (message_id, sender, notify_type, payload, delivery_status, created_at)
        VALUES (?, ?, ?, ?, 'DELIVERED', ?)
    """, (data.get("id", f"urn:uuid:{uuid.uuid4()}"),
          sender,
          primary_as2_type(data),
          json.dumps(data),
          time.strftime("%Y-%m-%dT%H:%M:%SZ")))


# =========================================================
# VALIDATION
# =========================================================
def validate_notification(data: dict):
    """Baseline COAR Notify structural validation."""
    if "id" not in data:
        return "Missing required property: id"
    if "type" not in data:
        return "Missing required property: type"
    if "origin" not in data:
        return "Missing required property: origin"
    if "target" not in data:
        return "Missing required property: target"
    types = get_types(data)
    # object is required for most patterns; Undo and Flag are narrow exceptions
    if "object" not in data and not types.intersection({"Undo", "Flag"}):
        return "Missing required property: object"
    return None


def send_unprocessable_notification(data: dict, reason: str):
    """
    Process Pattern — Unprocessable Notification.
    type: ["Flag", "coar-notify:UnprocessableNotification"]
    Sent back to the original sender's origin inbox via inReplyTo.
    """
    origin = data.get("origin") or {}
    reply_to_inbox = origin.get("inbox")
    original_id = data.get("id", "unknown")

    flag = {
        "@context": AS2_CONTEXT,
        "id": f"urn:uuid:{uuid.uuid4()}",
        "type": ["Flag", "coar-notify:UnprocessableNotification"],
        "inReplyTo": original_id,
        "summary": reason,
        "origin": {
            "id": "https://baits.bw/services/ldn-inbox",
            "type": "Service",
            "inbox": f"{LDN_BASE}/inbox/vet"
        },
        "target": origin if origin else {"id": "unknown"},
        "object": {"id": original_id},
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

    write_event(flag)
    write_outbox(flag, sender="SYSTEM")

    if reply_to_inbox:
        try:
            http_req.post(reply_to_inbox, json=flag, timeout=5)
        except Exception as e:
            print(f"⚠️ Could not deliver UnprocessableNotification: {e}")

    return flag


# =========================================================
# HELPER: resolve farmer_id from origin.inbox URL
# =========================================================
def farmer_id_from_origin(data: dict):
    origin_inbox = (data.get("origin") or {}).get("inbox", "")
    parts = origin_inbox.rstrip("/").split("/")
    candidate = parts[-1] if parts else ""
    return candidate if candidate in FARMERS else None


# =========================================================
# READ ENDPOINTS — used by the dashboard
# =========================================================
@app.get("/farmers")
async def list_farmers():
    return list(FARMERS.values())


@app.get("/animals/{farmer_id}")
async def get_farmer_animals(farmer_id: str):
    conn = sqlite3.connect(DB_PATH, timeout=30)
    cur = conn.cursor()
    cur.execute("SELECT tag_id, breed FROM animals WHERE owner_id = ? ORDER BY tag_id", (farmer_id,))
    rows = cur.fetchall()
    conn.close()
    return [{"tag_id": r[0], "breed": r[1]} for r in rows]


@app.get("/analytics/top-events")
async def top_events(k: int = 5):
    """Return the top-k VET inbox events ranked by KLD relevance score."""
    conn = sqlite3.connect(DB_PATH, timeout=30)
    cur = conn.cursor()
    cur.execute("""
        SELECT message_id, notify_type, relevance_score, payload
        FROM inbox
        WHERE recipient = 'VET'
        ORDER BY relevance_score DESC
        LIMIT ?
    """, (k,))
    rows = cur.fetchall()
    conn.close()
    return [
        {
            "message_id":      r[0],
            "notify_type":     r[1],
            "relevance_score": r[2],
            "payload":         json.loads(r[3]) if r[3] else {},
        }
        for r in rows
    ]


# =========================================================
# VET INBOX
# Handles:
#   Announcement Pattern  — Announce + coar-notify:IngestAction (registration)
#   Request/Offer Pattern — Request + coar-notify:ReviewAction  (movement permit)
#   Process Pattern       — Undo (withdraw permit request)
# =========================================================
@app.post("/inbox/vet")
async def vet_inbox(request: Request):
    try:
        data = await request.json()
    except Exception:
        return JSONResponse({"status": "rejected", "reason": "invalid JSON"}, status_code=400)

    error = validate_notification(data)
    if error:
        send_unprocessable_notification(data, error)
        return JSONResponse({"status": "unprocessable", "reason": error}, status_code=422)

    types = get_types(data)
    obj = data.get("object") or {}

    write_event(data)
    write_inbox(data, recipient="VET")

    # ------------------------------------------------------------------
    # Announcement Pattern — animal registration
    # type: ["Announce", "coar-notify:IngestAction"]
    # Adds the animal to the animals table under the registering farmer.
    # ------------------------------------------------------------------
    if "Announce" in types and "coar-notify:IngestAction" in types:
        tag_id = obj.get("tag_id")
        breed  = obj.get("breed", "")
        actor_id = (data.get("actor") or {}).get("id", "")
        owner_id = next(
            (fid for fid, info in FARMERS.items() if info["id"] == actor_id),
            None
        )
        if tag_id and owner_id:
            db_exec("""
                INSERT OR IGNORE INTO animals (tag_id, owner_id, breed, registered_at)
                VALUES (?, ?, ?, ?)
            """, (tag_id, owner_id, breed, time.strftime("%Y-%m-%dT%H:%M:%SZ")))

    # ------------------------------------------------------------------
    # Request Review pattern
    # type: ["Request", "coar-notify:ReviewAction"]
    # ------------------------------------------------------------------
    elif "Request" in types and "coar-notify:ReviewAction" in types:
        db_exec("""
            INSERT OR REPLACE INTO movement_permits
            (permit_id, tag_id, to_location, requested_by, status, request_timestamp)
            VALUES (?, ?, ?, ?, 'PENDING', ?)
        """, (
            data.get("id"),
            obj.get("tag_id"),
            obj.get("destination"),
            (data.get("actor") or {}).get("id", "UNKNOWN"),
            time.strftime("%Y-%m-%dT%H:%M:%SZ")
        ))

    # ------------------------------------------------------------------
    # Undo Offer pattern — farmer withdraws a pending permit request
    # type: ["Undo"]
    # ------------------------------------------------------------------
    elif "Undo" in types:
        target_id = obj.get("id")
        if target_id:
            row = db_query_one("SELECT status FROM movement_permits WHERE permit_id = ?", (target_id,))
            if row and row[0] == "PENDING":
                db_exec("""
                    UPDATE movement_permits SET status = 'WITHDRAWN', decision_timestamp = ?
                    WHERE permit_id = ?
                """, (time.strftime("%Y-%m-%dT%H:%M:%SZ"), target_id))

    notif_uuid = data.get("id", "").replace("urn:uuid:", "")
    headers = {"Location": f"{LDN_BASE}/notifications/{notif_uuid}"} if notif_uuid else {}
    return JSONResponse({"status": "accepted", "inbox": "vet"}, status_code=201, headers=headers)


@app.get("/inbox/vet")
async def vet_inbox_listing():
    """LDN receiver: GET inbox — returns JSON-LD LDP container of notification URLs."""
    conn = sqlite3.connect(DB_PATH, timeout=30)
    cur = conn.cursor()
    cur.execute(
        "SELECT message_id FROM inbox WHERE recipient = 'VET' ORDER BY created_at DESC"
    )
    rows = cur.fetchall()
    conn.close()

    contained = []
    for (msg_id,) in rows:
        notif_id = msg_id.split("|")[0]
        if notif_id.startswith("urn:uuid:"):
            contained.append({"@id": f"{LDN_BASE}/notifications/{notif_id[9:]}"})

    return JSONResponse(
        content={
            "@context": {"ldp": "http://www.w3.org/ns/ldp#"},
            "@id": f"{LDN_BASE}/inbox/vet",
            "@type": ["ldp:Container", "ldp:BasicContainer"],
            "ldp:contains": contained,
        },
        media_type="application/ld+json",
        headers={"Allow": "GET, HEAD, POST, OPTIONS"},
    )


# =========================================================
# FARMER INBOX  /inbox/farmer/{farmer_id}
# Handles:
#   Offer Pattern         — Offer (incoming transfer from another farmer)
#                           → auto-sends TentativeAccept (Acknowledgement Pattern)
#   Acknowledgement       — TentativeAccept (stored; no extra state)
#   Acknowledgement       — Accept / Reject (from vet OR from another farmer)
#   Process Pattern       — Undo (offering farmer withdraws their Offer)
# =========================================================
def _post_in_background(url: str, payload: dict):
    """Fire-and-forget HTTP POST used for intra-service callbacks."""
    try:
        http_req.post(url, json=payload,
                      headers={"Content-Type": "application/ld+json"}, timeout=10)
    except Exception as e:
        print(f"⚠️ Background POST to {url} failed: {e}")


@app.post("/inbox/farmer/{farmer_id}")
async def farmer_inbox(farmer_id: str, request: Request, background_tasks: BackgroundTasks):
    if farmer_id not in FARMERS:
        return JSONResponse(
            {"status": "rejected", "reason": f"Unknown farmer: {farmer_id}"}, status_code=404
        )

    try:
        data = await request.json()
    except Exception:
        return JSONResponse({"status": "rejected", "reason": "invalid JSON"}, status_code=400)

    error = validate_notification(data)
    if error:
        send_unprocessable_notification(data, error)
        return JSONResponse({"status": "unprocessable", "reason": error}, status_code=422)

    types = get_types(data)
    obj = data.get("object") or {}
    farmer_info = FARMERS[farmer_id]
    now = time.strftime("%Y-%m-%dT%H:%M:%SZ")

    write_event(data)
    write_inbox(data, recipient=farmer_id)
    # All farmer-inbox events are also forwarded to the Vet Officer.
    write_inbox(data, recipient="VET")

    # ------------------------------------------------------------------
    # Offer Pattern — another farmer offers an animal transfer
    # type: ["Offer"]
    # Auto-response: Acknowledgement Pattern — TentativeAccept
    # ------------------------------------------------------------------
    if "Offer" in types:
        offer_id = data.get("id")
        tag_id = obj.get("tag_id")
        from_farmer = farmer_id_from_origin(data)

        db_exec("""
            INSERT OR REPLACE INTO transfer_offers
            (offer_id, tag_id, from_farmer, to_farmer, status, offered_at, summary)
            VALUES (?, ?, ?, ?, 'PENDING', ?, ?)
        """, (offer_id, tag_id, from_farmer, farmer_id, now, data.get("summary", "")))

        # Acknowledgement Pattern — TentativeAccept sent automatically back to the offering farmer.
        # This confirms the notification was received and will be reviewed.
        origin_inbox = (data.get("origin") or {}).get("inbox")
        if origin_inbox:
            ack = {
                "@context": AS2_CONTEXT,
                "id": f"urn:uuid:{uuid.uuid4()}",
                "type": ["TentativeAccept"],
                "inReplyTo": offer_id,
                "actor": {
                    "id": farmer_info["service_id"],
                    "type": "Service",
                    "name": farmer_info["name"]
                },
                "origin": {
                    "id": farmer_info["service_id"],
                    "type": "Service",
                    "inbox": farmer_info["inbox"]
                },
                "target": data.get("origin"),
                "object": {"id": offer_id, "type": "Offer"},
                "summary": (
                    f"Offer received by {farmer_info['name']} — "
                    f"transfer of {tag_id} is under review"
                ),
                "timestamp": now,
            }
            write_event(ack)
            write_outbox(ack, sender=farmer_id)
            # Also land the TentativeAccept in the offering farmer's inbox
            if from_farmer:
                write_inbox(ack, recipient=from_farmer)
            # Send after the current request completes to avoid intra-service deadlock
            background_tasks.add_task(_post_in_background, origin_inbox, ack)

    # ------------------------------------------------------------------
    # Acknowledgement Pattern — Accept or Reject
    # Could be:
    #   (a) vet Accept/Reject for a movement permit (inReplyTo = permit_id)
    #   (b) farmer Accept/Reject for a transfer offer (inReplyTo = offer_id)
    # ------------------------------------------------------------------
    elif "Accept" in types or "Reject" in types:
        in_reply_to = data.get("inReplyTo")
        is_accept = "Accept" in types
        reason = data.get("summary", "")

        # Case (a): movement permit decision
        row = db_query_one("SELECT permit_id FROM movement_permits WHERE permit_id = ?", (in_reply_to,))
        if row:
            db_exec("""
                UPDATE movement_permits SET status = ?, reason = ?, decision_timestamp = ?
                WHERE permit_id = ?
            """, ("APPROVED" if is_accept else "REJECTED", reason, now, in_reply_to))

        else:
            # Case (b): transfer offer decision
            row = db_query_one(
                "SELECT offer_id, tag_id, to_farmer FROM transfer_offers WHERE offer_id = ?",
                (in_reply_to,)
            )
            if row:
                _, tag_id, to_farmer = row
                db_exec("""
                    UPDATE transfer_offers SET status = ?, decided_at = ?
                    WHERE offer_id = ?
                """, ("ACCEPTED" if is_accept else "REJECTED", now, in_reply_to))
                # On Accept, transfer animal ownership to the farmer who accepted
                if is_accept and tag_id and to_farmer:
                    db_exec("UPDATE animals SET owner_id = ? WHERE tag_id = ?", (to_farmer, tag_id))

    # ------------------------------------------------------------------
    # TentativeAccept — Acknowledgement already stored; nothing else needed
    # ------------------------------------------------------------------
    elif "TentativeAccept" in types:
        pass

    # ------------------------------------------------------------------
    # Undo Offer Pattern — the offering farmer withdraws a pending offer
    # type: ["Undo"]
    # ------------------------------------------------------------------
    elif "Undo" in types:
        target_id = obj.get("id")
        if target_id:
            row = db_query_one("SELECT status FROM transfer_offers WHERE offer_id = ?", (target_id,))
            if row and row[0] == "PENDING":
                db_exec("""
                    UPDATE transfer_offers SET status = 'WITHDRAWN', decided_at = ?
                    WHERE offer_id = ?
                """, (now, target_id))

    notif_uuid = data.get("id", "").replace("urn:uuid:", "")
    headers = {"Location": f"{LDN_BASE}/notifications/{notif_uuid}"} if notif_uuid else {}
    return JSONResponse(
        {"status": "accepted", "inbox": farmer_id}, status_code=201, headers=headers
    )


@app.get("/inbox/farmer/{farmer_id}")
async def farmer_inbox_listing(farmer_id: str):
    """LDN receiver: GET farmer inbox — returns JSON-LD LDP container."""
    if farmer_id not in FARMERS:
        return JSONResponse({"error": f"Unknown farmer: {farmer_id}"}, status_code=404)

    conn = sqlite3.connect(DB_PATH, timeout=30)
    cur = conn.cursor()
    cur.execute(
        "SELECT message_id FROM inbox WHERE recipient = ? ORDER BY created_at DESC",
        (farmer_id,)
    )
    rows = cur.fetchall()
    conn.close()

    contained = []
    for (msg_id,) in rows:
        notif_id = msg_id.split("|")[0]
        if notif_id.startswith("urn:uuid:"):
            contained.append({"@id": f"{LDN_BASE}/notifications/{notif_id[9:]}"})

    inbox_url = FARMERS[farmer_id]["inbox"]
    return JSONResponse(
        content={
            "@context": {"ldp": "http://www.w3.org/ns/ldp#"},
            "@id": inbox_url,
            "@type": ["ldp:Container", "ldp:BasicContainer"],
            "ldp:contains": contained,
        },
        media_type="application/ld+json",
        headers={"Allow": "GET, HEAD, POST, OPTIONS"},
    )


@app.get("/notifications/{notification_uuid}")
async def get_notification(notification_uuid: str):
    """LDN: retrieve an individual notification by UUID."""
    notif_id = f"urn:uuid:{notification_uuid}"
    conn = sqlite3.connect(DB_PATH, timeout=30)
    cur = conn.cursor()
    cur.execute(
        "SELECT payload FROM inbox WHERE message_id LIKE ? LIMIT 1",
        (f"{notif_id}|%",)
    )
    row = cur.fetchone()
    conn.close()
    if not row:
        return JSONResponse({"error": "Notification not found"}, status_code=404)
    return JSONResponse(
        content=json.loads(row[0]),
        media_type="application/ld+json",
    )
