import streamlit as st
import pandas as pd
import json
import math
import uuid
import time
import sqlite3
import requests
import os

# =========================================================
# CONFIG
# =========================================================
st.set_page_config(page_title="BAITS COAR Notify", layout="wide")

DB_PATH = "/app/data/baits_system.db"
LDN_BASE = os.getenv("LDN_BASE", "http://ldn_inbox:8000")
LDN_VET_INBOX = f"{LDN_BASE}/inbox/vet"

# COAR Notify preferred context URI (https://purl.org/coar/notify is deprecated)
AS2_CONTEXT = [
    "https://www.w3.org/ns/activitystreams",
    "https://coar-notify.net"
]

VET_SERVICE_ID = "https://baits.bw/services/vet-portal"
VET_ACTOR = {
    "id": "https://baits.bw/actors/vet-officer",
    "type": "Person",
    "name": "Veterinary Officer"
}

FARMERS = {
    f"farmer_{i}": {
        "id": f"https://baits.bw/actors/farmer-{i}",
        "name": f"Farmer {i}",
        "service_id": f"https://baits.bw/services/farmer-{i}-portal",
        "inbox": f"{LDN_BASE}/inbox/farmer/farmer_{i}"
    }
    for i in range(1, 6)
}

VILLAGE_DISTRICT_MAP = {
    "Mogoditshane": "Kweneng",
    "Letlhakane": "Central",
    "Palapye": "Central",
    "Maun": "North-West",
    "Serowe": "Central",
    "Gaborone": "South-East",
    "Kanye": "Southern",
    "Shakawe": "North-West",
    "Nxamasere": "North-West",
    "Francistown": "North-East",
    "Masunga": "North-East",
    "Tonota": "North-East"
}

# ── KLD constants (mirrors main.py — kept in sync manually) ──────────────────
FARMER_DISTRICTS_DASH = {
    "farmer_1": "South-East",
    "farmer_2": "Central",
    "farmer_3": "North-East",
    "farmer_4": "Central",
    "farmer_5": "Southern",
}

CATTLE_BREEDS_DASH = {
    "Tswana", "Brahman", "Nguni", "Bonsmara", "Hereford", "Simmental",
    "Charolais", "Angus", "Afrikaner", "Boran", "Simentaler", "Tuli",
    "Drakensberger", "Beefmaster", "Fleckvieh", "Limousin", "Shorthorn",
    "Mashona", "Santa Gertrudis", "Murray Grey",
}

KLD_PRIOR_DASH  = [0.20, 0.55, 0.10, 0.30, 0.20, 0.40]
KLD_ALPHA_DASH  = 0.1
KLD_FEATURE_LABELS = [
    "b1 High-risk district",
    "b2 Cattle",
    "b3 Disease alert",
    "b4 Cross-district",
    "b5 High-risk purpose",
    "b6 New record",
]


# =========================================================
# KLD FEATURE EXTRACTION (dashboard-side, for analytics display)
# =========================================================
def get_feature_vector(payload: dict) -> list:
    """Return [b1..b6] Boolean list for a payload; queries DB for b1 and b3."""
    obj     = payload.get("object") or {}
    types_  = payload.get("type", [])
    types_s = {types_} if isinstance(types_, str) else set(types_)
    breed   = obj.get("breed", "")
    purpose = (obj.get("purpose") or "").lower()

    actor_id  = (payload.get("actor") or {}).get("id", "")
    farmer_id = next(
        (fid for fid, info in FARMERS.items() if info["id"] == actor_id), None
    )
    origin_district = FARMER_DISTRICTS_DASH.get(farmer_id, "") if farmer_id else ""
    dest_district   = obj.get("district", "")

    try:
        conn = sqlite3.connect(DB_PATH)
        cur  = conn.cursor()
        cur.execute("SELECT risk_level FROM district_risk WHERE district_name = ?",
                    (origin_district,))
        r1   = cur.fetchone()
        b1   = 1 if (r1 and r1[0] == "HIGH") else 0

        today = time.strftime("%Y-%m-%d")
        cur.execute(
            "SELECT 1 FROM disease_alerts "
            "WHERE district_name = ? AND start_date <= ? AND end_date >= ?",
            (origin_district, today, today)
        )
        b3   = 1 if cur.fetchone() else 0
        conn.close()
    except Exception:
        b1, b3 = 0, 0

    b2 = 1 if breed in CATTLE_BREEDS_DASH else 0
    b4 = 1 if (origin_district and dest_district
               and origin_district != dest_district) else 0
    b5 = 1 if any(w in purpose for w in ("slaughter", "auction")) else 0
    b6 = 1 if (
        ("Announce" in types_s and "coar-notify:IngestAction" in types_s)
        or "Offer" in types_s
    ) else 0

    return [b1, b2, b3, b4, b5, b6]


# =========================================================
# DB HELPERS
# =========================================================
def load_events():
    conn = sqlite3.connect(DB_PATH)
    try:
        df = pd.read_sql_query("SELECT * FROM events ORDER BY timestamp DESC", conn)
    except Exception:
        df = pd.DataFrame()
    conn.close()
    return df


def load_farmer_inbox(farmer_id):
    """Messages received by this farmer (from inbox table)."""
    conn = sqlite3.connect(DB_PATH)
    try:
        df = pd.read_sql_query(
            "SELECT * FROM inbox WHERE recipient = ? ORDER BY created_at DESC",
            conn, params=(farmer_id,)
        )
    except Exception:
        df = pd.DataFrame()
    conn.close()
    return df


def load_farmer_outbox(farmer_id):
    """Messages sent by this farmer — derived from events by actor.id."""
    farmer_actor_id = FARMERS[farmer_id]["id"]
    conn = sqlite3.connect(DB_PATH)
    try:
        df = pd.read_sql_query("SELECT * FROM events ORDER BY timestamp DESC", conn)
        if not df.empty:
            df["_actor"] = df["payload"].apply(
                lambda p: safe_json(p).get("actor", {}).get("id", "")
            )
            df = df[df["_actor"] == farmer_actor_id].drop(columns=["_actor"])
    except Exception:
        df = pd.DataFrame()
    conn.close()
    return df


def load_farmer_animals(farmer_id):
    conn = sqlite3.connect(DB_PATH)
    try:
        df = pd.read_sql_query(
            "SELECT tag_id, breed FROM animals WHERE owner_id = ? ORDER BY tag_id",
            conn, params=(farmer_id,)
        )
    except Exception:
        df = pd.DataFrame(columns=["tag_id", "breed"])
    conn.close()
    return df


def load_permit_status():
    conn = sqlite3.connect(DB_PATH)
    try:
        df = pd.read_sql_query("SELECT permit_id, status FROM movement_permits", conn)
    except Exception:
        df = pd.DataFrame(columns=["permit_id", "status"])
    conn.close()
    return dict(zip(df["permit_id"], df["status"]))


def load_pending_offers(farmer_id):
    """Transfer offers pending this farmer's decision."""
    conn = sqlite3.connect(DB_PATH)
    try:
        df = pd.read_sql_query(
            "SELECT * FROM transfer_offers WHERE to_farmer = ? AND status = 'PENDING'",
            conn, params=(farmer_id,)
        )
    except Exception:
        df = pd.DataFrame()
    conn.close()
    return df


def load_outgoing_offers_status(farmer_id):
    """Status map offer_id → status for offers sent by this farmer."""
    conn = sqlite3.connect(DB_PATH)
    try:
        df = pd.read_sql_query(
            "SELECT offer_id, status FROM transfer_offers WHERE from_farmer = ?",
            conn, params=(farmer_id,)
        )
    except Exception:
        df = pd.DataFrame(columns=["offer_id", "status"])
    conn.close()
    return dict(zip(df["offer_id"], df["status"]))


# =========================================================
# SAFE JSON
# =========================================================
def safe_json(x):
    try:
        return json.loads(x) if isinstance(x, str) else x
    except Exception:
        return {}


# =========================================================
# TYPE HELPERS
# =========================================================
def get_types(payload) -> set:
    t = payload.get("type", [])
    return {t} if isinstance(t, str) else set(t)


def classify(payload):
    """Return (primary_as2_type, coar_subtype_label) for style lookup."""
    ts = get_types(payload)
    if "Request" in ts and "coar-notify:ReviewAction" in ts:
        return "Request", "ReviewAction"
    if "Announce" in ts and "coar-notify:IngestAction" in ts:
        return "Announce", "IngestAction"
    if "Announce" in ts:
        return "Announce", ""
    if "Offer" in ts:
        return "Offer", ""
    if "TentativeAccept" in ts:
        return "TentativeAccept", ""
    if "TentativeReject" in ts:
        return "TentativeReject", ""
    if "Accept" in ts:
        return "Accept", ""
    if "Reject" in ts:
        return "Reject", ""
    if "Undo" in ts:
        return "Undo", ""
    if "Flag" in ts:
        return "Flag", "UnprocessableNotification"
    t_list = list(ts)
    return (t_list[0] if t_list else "Unknown"), ""


STYLE_MAP = {
    # Request/Offer Patterns
    ("Request", "ReviewAction"):           ("🚚", "#5a3d8f", "#f1ecfa", "Movement permit request"),
    ("Offer",   ""):                       ("🔁", "#0c5460", "#e7f6f8", "Animal transfer offer"),
    # Acknowledgement Patterns
    ("TentativeAccept", ""):               ("📨", "#1a6b8a", "#e3f4f8", "Offer acknowledged — pending review"),
    ("TentativeReject", ""):               ("📭", "#a0522d", "#fdf3ec", "Offer tentatively declined"),
    ("Accept",  ""):                       ("✅", "#1e7e34", "#eafaf0", "Accepted"),
    ("Reject",  ""):                       ("❌", "#a71d2a", "#fdecea", "Rejected"),
    # Announcement Patterns
    ("Announce", "IngestAction"):          ("🐄", "#856404", "#fff9e6", "Animal registration"),
    ("Announce", ""):                      ("📣", "#856404", "#fff9e6", "Announcement"),
    # Process Patterns
    ("Undo",    ""):                       ("↩️", "#6c757d", "#f1f1f1", "Request withdrawn"),
    ("Flag",    "UnprocessableNotification"): ("⚠️", "#b8860b", "#fff4e0", "Unprocessable notification"),
}


def get_inner_object(payload):
    """Unwrap one level for Accept/Reject, which embed the original Request as object."""
    ts = get_types(payload)
    obj = payload.get("object") or {}
    if ts.intersection({"Accept", "Reject"}):
        obj = obj.get("object") or obj
    return obj


def enrich_df(df, payload_col="payload"):
    if df.empty:
        return df
    df = df.copy()
    df["payload_json"] = df[payload_col].apply(safe_json)
    return df


# =========================================================
# NOTIFICATION CARD RENDERER
# =========================================================
def render_card(payload, relevance_score=None):
    if not payload:
        return
    primary, sub = classify(payload)
    icon, color, bg, label = STYLE_MAP.get(
        (primary, sub), ("ℹ️", "#333", "#f5f5f5", primary)
    )

    actor_name = (payload.get("actor") or {}).get("name", "")
    ts = payload.get("timestamp", "")
    summary_txt = payload.get("summary") or ""
    in_reply_to = payload.get("inReplyTo") or ""
    inner = get_inner_object(payload)
    tag = inner.get("tag_id") or (payload.get("object") or {}).get("id", "")
    dest = inner.get("destination")
    district = inner.get("district")
    breed = inner.get("breed")

    if primary == "TentativeAccept":
        detail = summary_txt or f"Offer <code>{in_reply_to[:40]}…</code> acknowledged"
    elif primary in ("TentativeReject",):
        detail = summary_txt or "Offer tentatively declined."
    elif primary == "Flag":
        detail = summary_txt or "A notification could not be processed."
    elif primary == "Undo":
        detail = (
            f"Withdrawn offer for tag <b>{tag}</b>" if tag
            else (summary_txt or "A pending request was withdrawn.")
        )
    else:
        parts = [f"Tag <b>{tag}</b>" if tag else ""]
        if dest:
            parts.append(f"→ <b>{dest}</b>")
            if district:
                parts.append(f"({district})")
        if breed:
            parts.append(f"· Breed: <b>{breed}</b>")
        detail = " ".join(p for p in parts if p)
        if summary_txt:
            detail += f"<br><span style='color:#666'>{summary_txt}</span>"

    if actor_name:
        detail += f"<br><span style='color:#999'>From: {actor_name}</span>"

    types_label = payload.get("type", "")
    if isinstance(types_label, list):
        types_label = ", ".join(types_label)

    # ── KLD relevance badge ──────────────────────────────────────────────────
    badge_html = ""
    if relevance_score is not None:
        score = float(relevance_score)
        if score > 0.30:
            bc, bl = "#c0392b", f"🔴 HIGH  {score:.3f}"
        elif score > 0.15:
            bc, bl = "#e67e22", f"🟠 MED  {score:.3f}"
        else:
            bc, bl = "#27ae60", f"🟢 LOW  {score:.3f}"
        badge_html = (
            f'<span style="float:right;font-size:11px;font-weight:700;'
            f'color:{bc};background:{bc}18;padding:2px 8px;border-radius:12px;'
            f'border:1px solid {bc}44;">{bl}</span>'
        )

    st.markdown(f"""
    <div style="background:{bg};border-left:5px solid {color};border-radius:8px;
                padding:14px 18px;margin-bottom:10px;">
        <div style="font-size:16px;font-weight:600;color:{color};">
            {icon} {label}{badge_html}
        </div>
        <div style="font-size:14px;color:#444;margin-top:4px;">{detail}</div>
        <div style="font-size:11px;color:#aaa;margin-top:4px;">
            AS2 type: <code>{types_label}</code>
        </div>
        <div style="font-size:12px;color:#888;margin-top:2px;">{ts}</div>
    </div>
    """, unsafe_allow_html=True)


# =========================================================
# AS2 NOTIFICATION BUILDER
# =========================================================
def make_notification(notify_type, actor, origin_id, origin_inbox,
                       target_id, target_inbox, obj,
                       in_reply_to=None, summary=None):
    """
    Build a COAR Notify–compliant AS2 notification.
    notify_type MUST be a list, e.g. ["Offer"] or ["Request", "coar-notify:ReviewAction"].
    inReplyTo and summary are omitted entirely when None (spec: OPTIONAL fields must not be null).
    """
    notif = {
        "@context": AS2_CONTEXT,
        "id": f"urn:uuid:{uuid.uuid4()}",
        "type": notify_type,
        "actor": actor,
        "origin": {"id": origin_id, "type": "Service", "inbox": origin_inbox},
        "target": {"id": target_id, "type": "Service", "inbox": target_inbox},
        "object": obj,
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    if in_reply_to is not None:
        notif["inReplyTo"] = in_reply_to
    if summary is not None:
        notif["summary"] = summary
    return notif


# =========================================================
# LDN SENDER
# Returns True on success, False on any error.
# Displays st.error automatically so callers only need to handle the happy path.
# =========================================================
def send_ldn(url, notification):
    if not url:
        st.error("Cannot send: target inbox URL is missing.")
        return False
    try:
        r = requests.post(
            url, json=notification,
            headers={"Content-Type": "application/ld+json"},
            timeout=5
        )
        if r.status_code not in (200, 201, 202):
            st.error(f"Server returned {r.status_code}: {r.text[:300]}")
            return False
        return True
    except Exception as e:
        st.error(f"LDN send failed: {e}")
        return False


# =========================================================
# SIDEBAR SELECTOR
# =========================================================
def sidebar_selector():
    role = st.sidebar.selectbox("Select Role", ["Farmer", "Veterinary Officer", "Admin"])
    farmer_id = None
    if role == "Farmer":
        keys = list(FARMERS.keys())
        labels = [FARMERS[k]["name"] for k in keys]
        idx = st.sidebar.selectbox(
            "Select Farmer", range(len(keys)),
            format_func=lambda i: labels[i]
        )
        farmer_id = keys[idx]
    return role, farmer_id


# =========================================================
# FARMER VIEW
# =========================================================
def farmer_view(farmer_id):
    info = FARMERS[farmer_id]
    farmer_actor = {"id": info["id"], "type": "Person", "name": info["name"]}

    st.title(f"🚜 {info['name']} Portal")

    animals_df = load_farmer_animals(farmer_id)
    animal_tags = animals_df["tag_id"].tolist() if not animals_df.empty else []

    # Sidebar: owned animals — scrollable panel
    st.sidebar.markdown(f"**{info['name']}'s Animals** ({len(animals_df)})")
    if animals_df.empty:
        st.sidebar.caption("No animals currently owned.")
    else:
        rows_html = "".join(
            f"<div style='display:flex;justify-content:space-between;padding:4px 6px;"
            f"border-bottom:1px solid #e0e0e0;font-size:13px;'>"
            f"<span style='font-family:monospace;color:#333'>{r['tag_id']}</span>"
            f"<span style='color:#666'>{r['breed']}</span></div>"
            for _, r in animals_df.iterrows()
        )
        st.sidebar.markdown(f"""
        <div style="height:280px;overflow-y:auto;border:1px solid #ddd;
                    border-radius:6px;background:#fafafa;margin-top:4px;">
            <div style="display:flex;justify-content:space-between;padding:4px 6px;
                        background:#f0f0f0;font-size:11px;font-weight:600;color:#555;
                        border-bottom:2px solid #ccc;">
                <span>TAG ID</span><span>BREED</span>
            </div>
            {rows_html}
        </div>
        """, unsafe_allow_html=True)

    tab_inbox, tab_notifs, tab_sent, tab_reg, tab_move, tab_transfer = st.tabs([
        "📥 Inbox",
        "🔔 Incoming Notifications",
        "📤 Sent",
        "🐄 Register Animal",
        "🚚 Movement Permit",
        "📦 Offer Transfer",
    ])

    inbox_df  = enrich_df(load_farmer_inbox(farmer_id))
    outbox_df = enrich_df(load_farmer_outbox(farmer_id))

    # ---- INBOX --------------------------------------------------------
    with tab_inbox:
        st.subheader("Inbox")
        st.caption("All incoming messages — offers, acknowledgements, permit decisions, and withdrawals.")
        if inbox_df.empty:
            st.info("No messages yet.")
        else:
            for _, row in inbox_df.iterrows():
                render_card(row["payload_json"])

    # ---- INCOMING NOTIFICATIONS ---------------------------------------
    with tab_notifs:
        st.subheader("Incoming Notifications")
        st.caption(
            "Pending transfer offers awaiting your decision. "
            "Full message details are available in your **Inbox** tab."
        )
        pending = load_pending_offers(farmer_id)
        if pending.empty:
            st.info("No pending notifications.")
        else:
            for _, offer_row in pending.iterrows():
                from_info = FARMERS.get(offer_row["from_farmer"], {})

                # Compact header row
                st.markdown(f"""
                <div style="display:flex;align-items:center;justify-content:space-between;
                            background:#f7f9fc;border:1px solid #d0dce8;border-radius:6px;
                            padding:10px 16px;margin-bottom:4px;">
                    <div>
                        <span style="font-size:15px;font-weight:600;color:#0c5460;">🔁 Transfer Offer</span>
                        &nbsp;·&nbsp;
                        <span style="font-family:monospace;color:#333;">{offer_row['tag_id']}</span>
                        &nbsp;from&nbsp;
                        <span style="font-weight:600;">{from_info.get('name', offer_row['from_farmer'])}</span>
                    </div>
                    <span style="font-size:11px;color:#888;">{offer_row.get('offered_at','')}</span>
                </div>
                """, unsafe_allow_html=True)

                col1, col2 = st.columns(2)
                with col1:
                    if st.button("✅ Accept", key=f"acc_{offer_row['offer_id']}"):
                        conn = sqlite3.connect(DB_PATH)
                        cur = conn.cursor()
                        cur.execute(
                            "SELECT payload FROM inbox WHERE message_id = ?",
                            (f"{offer_row['offer_id']}|{farmer_id}",)
                        )
                        row_p = cur.fetchone()
                        conn.close()
                        original = safe_json(row_p[0]) if row_p else {"id": offer_row["offer_id"]}
                        embedded = {k: v for k, v in original.items() if k != "@context"}

                        accept = make_notification(
                            ["Accept"], farmer_actor,
                            info["service_id"], info["inbox"],
                            from_info.get("service_id", ""), from_info.get("inbox", ""),
                            obj=embedded,
                            in_reply_to=offer_row["offer_id"],
                            summary=f"{info['name']} accepted transfer of {offer_row['tag_id']}"
                        )
                        if send_ldn(from_info.get("inbox", ""), accept):
                            st.success(
                                f"✅ Accepted — {offer_row['tag_id']} is now in your herd. "
                                f"Accept notification sent to "
                                f"{from_info.get('name', offer_row['from_farmer'])}. "
                                f"Check your **Inbox** for the full record."
                            )
                            st.rerun()

                with col2:
                    if st.button("❌ Reject", key=f"rej_{offer_row['offer_id']}"):
                        conn = sqlite3.connect(DB_PATH)
                        cur = conn.cursor()
                        cur.execute(
                            "SELECT payload FROM inbox WHERE message_id = ?",
                            (f"{offer_row['offer_id']}|{farmer_id}",)
                        )
                        row_p = cur.fetchone()
                        conn.close()
                        original = safe_json(row_p[0]) if row_p else {"id": offer_row["offer_id"]}
                        embedded = {k: v for k, v in original.items() if k != "@context"}

                        reject = make_notification(
                            ["Reject"], farmer_actor,
                            info["service_id"], info["inbox"],
                            from_info.get("service_id", ""), from_info.get("inbox", ""),
                            obj=embedded,
                            in_reply_to=offer_row["offer_id"],
                            summary=f"{info['name']} rejected transfer of {offer_row['tag_id']}"
                        )
                        if send_ldn(from_info.get("inbox", ""), reject):
                            st.warning(
                                f"❌ Rejected — {offer_row['tag_id']} stays with "
                                f"{from_info.get('name', offer_row['from_farmer'])}. "
                                f"Reject notification sent to them. "
                                f"Check your **Inbox** for the full record."
                            )
                            st.rerun()

                st.markdown("<hr style='margin:6px 0;border:none;border-top:1px solid #eee;'>",
                            unsafe_allow_html=True)

    # ---- SENT ---------------------------------------------------------
    with tab_sent:
        st.subheader("Sent Notifications")
        offer_status = load_outgoing_offers_status(farmer_id)
        permit_status = load_permit_status()

        if outbox_df.empty:
            st.info("Nothing sent yet.")
        else:
            for _, row in outbox_df.iterrows():
                p = row["payload_json"]
                render_card(p)
                ts_p = get_types(p)
                ev_id = p.get("id", "")

                # Undo button for pending Offer (transfer)
                if "Offer" in ts_p and offer_status.get(ev_id) == "PENDING":
                    if st.button("↩️ Withdraw offer", key=f"undo_offer_{ev_id}"):
                        # Find who received the offer
                        conn = sqlite3.connect(DB_PATH)
                        cur = conn.cursor()
                        cur.execute(
                            "SELECT to_farmer FROM transfer_offers WHERE offer_id = ?", (ev_id,)
                        )
                        tr = cur.fetchone()
                        conn.close()
                        if tr:
                            to_info = FARMERS.get(tr[0], {})
                            tag_label = (p.get("object") or {}).get("tag_id", "")
                            undo = make_notification(
                                ["Undo"], farmer_actor,
                                info["service_id"], info["inbox"],
                                to_info.get("service_id", ""), to_info.get("inbox", ""),
                                obj={"id": ev_id, "type": "Offer"},
                                summary=f"{info['name']} withdrew transfer offer for {tag_label}"
                            )
                            if send_ldn(to_info.get("inbox", ""), undo):
                                st.warning(
                                    f"↩️ Offer for {tag_label} withdrawn. "
                                    f"An Undo notification has been sent to "
                                    f"{to_info.get('name', tr[0])}."
                                )
                                st.rerun()
                        else:
                            st.error("Could not find the offer record — please refresh and try again.")

                # Undo button for pending permit Request
                elif (
                    "Request" in ts_p
                    and "coar-notify:ReviewAction" in ts_p
                    and permit_status.get(ev_id) == "PENDING"
                ):
                    if st.button("↩️ Withdraw permit request", key=f"undo_permit_{ev_id}"):
                        tag_label = (p.get("object") or {}).get("tag_id", "")
                        undo = make_notification(
                            ["Undo"], farmer_actor,
                            info["service_id"], info["inbox"],
                            VET_SERVICE_ID, LDN_VET_INBOX,
                            obj={"id": ev_id, "type": "Request"},
                            summary=f"{info['name']} withdrew permit request for {tag_label}"
                        )
                        if send_ldn(LDN_VET_INBOX, undo):
                            st.warning(
                                f"↩️ Permit request for {tag_label} withdrawn. "
                                f"An Undo notification has been sent to the Vet."
                            )
                            st.rerun()

    # ---- REGISTER ANIMAL ----------------------------------------------
    with tab_reg:
        st.subheader("Register Animal")
        st.caption(
            "**Announcement Pattern** — `Announce` + `coar-notify:IngestAction`  \n"
            "Informs the vet service of a new animal resource."
        )

        ALL_BREEDS = sorted([
            "Afrikaner", "Angus", "Beefmaster", "Bonsmara", "Boran",
            "Brahman", "Charolais", "Drakensberger", "Fleckvieh", "Hereford",
            "Limousin", "Mashona", "Murray Grey", "Nguni", "Santa Gertrudis",
            "Shorthorn", "Simentaler", "Simmental", "Tswana", "Tuli",
        ])

        farmer_num = farmer_id.split("_")[1]  # e.g. "1" from "farmer_1"
        tag_hint = f"F{farmer_num}-NNN  (e.g. F{farmer_num}-006)"

        selected_breed = st.selectbox("Breed", ALL_BREEDS, key="reg_breed")
        new_tag = st.text_input(
            "Tag ID",
            placeholder=tag_hint,
            help=f"Use the format F{farmer_num}-NNN where NNN is a 3-digit number, e.g. F{farmer_num}-006",
            key="reg_tag"
        )

        import re
        tag_pattern = re.compile(rf"^F{farmer_num}-\d{{3}}$")

        def animal_exists(tag_id):
            conn = sqlite3.connect(DB_PATH)
            cur = conn.cursor()
            cur.execute("SELECT 1 FROM animals WHERE tag_id = ?", (tag_id,))
            found = cur.fetchone() is not None
            conn.close()
            return found

        if st.button("Submit Registration"):
            if not new_tag:
                st.error("Tag ID is required.")
            elif not tag_pattern.match(new_tag):
                st.error(f"Invalid format. Expected: {tag_hint}")
            elif animal_exists(new_tag):
                st.exception(Exception(f"Animal '{new_tag}' is already registered in the system."))
            else:
                obj = {
                    "id": f"urn:tag:{new_tag}",
                    "type": "Animal",
                    "tag_id": new_tag,
                    "breed": selected_breed
                }
                notif = make_notification(
                    ["Announce", "coar-notify:IngestAction"],
                    farmer_actor,
                    info["service_id"], info["inbox"],
                    VET_SERVICE_ID, LDN_VET_INBOX,
                    obj,
                    summary=f"{info['name']} registered {new_tag} ({selected_breed})"
                )
                if send_ldn(LDN_VET_INBOX, notif):
                    st.success(
                        f"🐄 Animal {new_tag} ({selected_breed}) registered and "
                        f"announcement sent to Vet Inbox."
                    )

    # ---- MOVEMENT PERMIT ----------------------------------------------
    with tab_move:
        st.subheader("Request Movement Permit")
        st.caption(
            "**Request/Offer Pattern** — `Request` + `coar-notify:ReviewAction`  \n"
            "Asks the vet to review and decide on an animal movement."
        )
        if not animal_tags:
            st.info("You have no animals to move.")
        else:
            tag     = st.selectbox("Animal", animal_tags, key="mv_tag")
            village = st.selectbox("Destination Village", list(VILLAGE_DISTRICT_MAP.keys()), key="mv_village")
            district = VILLAGE_DISTRICT_MAP[village]
            st.caption(f"District: **{district}**")
            purpose = st.selectbox("Purpose", [
                "Sale or transfer of ownership",
                "Movement to auction or livestock market",
                "Transportation to slaughterhouse or abattoir",
                "Veterinary treatment or diagnostic services",
                "Breeding and genetic improvement programs",
                "Grazing or seasonal relocation",
                "Agricultural exhibitions and competitions",
                "Farm-to-farm relocation",
                "Export or cross-border transportation",
                "Quarantine or disease control measures",
                "Emergency relocation due to disasters or security concerns",
                "Research and educational activities"
            ])
            if st.button("Request Permit"):
                obj = {
                    "id": f"urn:tag:{tag}",
                    "type": "Animal",
                    "tag_id": tag,
                    "destination": village,
                    "district": district,
                    "purpose": purpose
                }
                notif = make_notification(
                    ["Request", "coar-notify:ReviewAction"],
                    farmer_actor,
                    info["service_id"], info["inbox"],
                    VET_SERVICE_ID, LDN_VET_INBOX,
                    obj,
                    summary=f"Permit request: {tag} → {village} ({district})"
                )
                if send_ldn(LDN_VET_INBOX, notif):
                    st.success(
                        f"🚚 Permit request for {tag} → {village} ({district}) "
                        f"sent to the Vet. You will be notified of their decision in your Inbox."
                    )

    # ---- OFFER TRANSFER -----------------------------------------------
    with tab_transfer:
        st.subheader("Offer Animal Transfer")
        st.caption(
            "**Offer Pattern** — `Offer` (AS2 Activity Type)  \n"
            "Offers to transfer one of your animals to another farmer. "
            "The receiving farmer's system will automatically send you a "
            "**TentativeAccept** acknowledgement (Acknowledgement Pattern). "
            "They can then Accept or Reject the offer."
        )

        other_farmers = {k: v for k, v in FARMERS.items() if k != farmer_id}

        if not animal_tags:
            st.info("You have no animals to offer.")
        else:
            tag_sel = st.selectbox("Animal to offer", animal_tags, key="tr_tag")
            breed_val = animals_df.loc[animals_df["tag_id"] == tag_sel, "breed"].values
            breed_val = breed_val[0] if len(breed_val) > 0 else ""

            target_keys   = list(other_farmers.keys())
            target_labels = [FARMERS[k]["name"] for k in target_keys]
            t_idx = st.selectbox(
                "Offer to", range(len(target_keys)),
                format_func=lambda i: target_labels[i],
                key="tr_target"
            )
            target_key  = target_keys[t_idx]
            target_info = FARMERS[target_key]

            if st.button("Send Offer"):
                obj = {
                    "id": f"urn:tag:{tag_sel}",
                    "type": "Animal",
                    "tag_id": tag_sel,
                    "breed": breed_val
                }
                notif = make_notification(
                    ["Offer"],
                    farmer_actor,
                    info["service_id"], info["inbox"],
                    target_info["service_id"], target_info["inbox"],
                    obj,
                    summary=(
                        f"{info['name']} offers to transfer {tag_sel} ({breed_val}) "
                        f"to {target_info['name']}"
                    )
                )
                if send_ldn(target_info["inbox"], notif):
                    st.success(
                        f"🔁 Offer for {tag_sel} ({breed_val}) sent to {target_info['name']}. "
                        f"Watch your Inbox — you will receive a TentativeAccept "
                        f"acknowledgement shortly."
                    )


# =========================================================
# VET VIEW
# =========================================================
def vet_view():
    st.title("🩺 Veterinary Officer Portal")

    events_df = enrich_df(load_events())

    tab_inbox, tab_queue, tab_ledger, tab_kld = st.tabs(
        ["📥 Inbox", "🗂️ Work Queue", "📜 Ledger", "📊 KLD Analytics"]
    )

    # ---- VET INBOX — ranked by KLD relevance score ----------------------
    with tab_inbox:
        st.subheader("All System Notifications")
        st.caption(
            "Notifications are ranked by **KLD relevance score** — most atypical "
            "(highest divergence from the baseline) appear first. "
            "Badge colour: 🔴 HIGH (>0.30)  🟠 MED (0.15–0.30)  🟢 LOW (<0.15)"
        )
        conn = sqlite3.connect(DB_PATH)
        try:
            vet_df = pd.read_sql_query(
                "SELECT * FROM inbox WHERE recipient = 'VET' "
                "ORDER BY COALESCE(relevance_score, 0.0) DESC, created_at DESC",
                conn
            )
        except Exception:
            vet_df = pd.DataFrame()
        conn.close()
        vet_df = enrich_df(vet_df)
        if vet_df.empty:
            st.info("No notifications yet.")
        else:
            for _, row in vet_df.iterrows():
                render_card(row["payload_json"],
                            relevance_score=row.get("relevance_score"))

    # ---- WORK QUEUE ---------------------------------------------------
    with tab_queue:
        st.subheader("Pending Movement Permit Requests")
        st.caption(
            "**Request/Offer Pattern** — `Request` + `coar-notify:ReviewAction`  \n"
            "Ranked by **KLD relevance score** — highest-risk requests appear first. "
            "Respond with **Accept** or **Reject** (Acknowledgement Pattern)."
        )

        permit_status = load_permit_status()

        conn = sqlite3.connect(DB_PATH)
        try:
            queue_df = pd.read_sql_query(
                "SELECT payload, COALESCE(relevance_score, 0.0) AS relevance_score "
                "FROM inbox WHERE recipient = 'VET' AND notify_type = 'Request' "
                "ORDER BY relevance_score DESC",
                conn
            )
        except Exception:
            queue_df = pd.DataFrame()
        conn.close()

        queue_df = enrich_df(queue_df)

        def is_pending_permit(p):
            ts = get_types(p)
            return "Request" in ts and "coar-notify:ReviewAction" in ts

        if not queue_df.empty:
            queue_df = queue_df[queue_df["payload_json"].apply(is_pending_permit)]
            queue_df["_eid"] = queue_df["payload_json"].apply(lambda p: p.get("id", ""))
            queue_df = queue_df[
                queue_df["_eid"].apply(lambda eid: permit_status.get(eid, "PENDING") == "PENDING")
            ]

        if queue_df.empty:
            st.info("No pending permit requests.")
        else:
            for _, row in queue_df.iterrows():
                payload = row["payload_json"]
                obj = payload.get("object") or {}
                score = float(row["relevance_score"])
                eid = row["_eid"]

                farmer_origin    = payload.get("origin") or {}
                farmer_inbox_url = farmer_origin.get("inbox", "")
                farmer_svc_id    = farmer_origin.get("id", "")

                # Relevance badge
                if score > 0.30:
                    bc, bl = "#c0392b", f"🔴 HIGH  {score:.3f}"
                elif score > 0.15:
                    bc, bl = "#e67e22", f"🟠 MED  {score:.3f}"
                else:
                    bc, bl = "#27ae60", f"🟢 LOW  {score:.3f}"

                st.markdown(f"""
                <div style="background:#f7f9fc;border:1px solid #d0dce8;border-radius:6px;
                            padding:10px 16px;margin-bottom:4px;">
                    <div style="display:flex;justify-content:space-between;align-items:center;">
                        <span style="font-size:15px;font-weight:600;">
                            🐄 <b>{obj.get('tag_id','—')}</b>
                            &nbsp;→&nbsp; <b>{obj.get('destination','—')}</b>
                            &nbsp;<span style="color:#666;font-weight:400;">
                                ({obj.get('district','—')})
                            </span>
                        </span>
                        <span style="font-size:11px;font-weight:700;color:{bc};
                                     background:{bc}18;padding:2px 8px;border-radius:12px;
                                     border:1px solid {bc}44;">{bl}</span>
                    </div>
                    <div style="font-size:12px;color:#666;margin-top:4px;">
                        Purpose: {obj.get('purpose','—')}
                    </div>
                </div>
                """, unsafe_allow_html=True)

                if not farmer_inbox_url:
                    st.error("Cannot respond: farmer inbox URL not found in this notification.")
                else:
                    col1, col2 = st.columns(2)
                    with col1:
                        if st.button("✅ Approve", key=f"a_{eid}"):
                            embedded = {k: v for k, v in payload.items() if k != "@context"}
                            accept = make_notification(
                                ["Accept"], VET_ACTOR,
                                VET_SERVICE_ID, LDN_VET_INBOX,
                                farmer_svc_id, farmer_inbox_url,
                                obj=embedded,
                                in_reply_to=payload.get("id"),
                                summary=f"Movement permit approved for {obj.get('tag_id')}"
                            )
                            if send_ldn(farmer_inbox_url, accept):
                                st.success(
                                    f"✅ Permit for {obj.get('tag_id')} approved. "
                                    f"Accept notification sent to the farmer."
                                )
                                st.rerun()
                    with col2:
                        if st.button("❌ Reject", key=f"r_{eid}"):
                            embedded = {k: v for k, v in payload.items() if k != "@context"}
                            reject = make_notification(
                                ["Reject"], VET_ACTOR,
                                VET_SERVICE_ID, LDN_VET_INBOX,
                                farmer_svc_id, farmer_inbox_url,
                                obj=embedded,
                                in_reply_to=payload.get("id"),
                                summary=f"Movement permit rejected for {obj.get('tag_id')}"
                            )
                            if send_ldn(farmer_inbox_url, reject):
                                st.warning(
                                    f"❌ Permit for {obj.get('tag_id')} rejected. "
                                    f"Reject notification sent to the farmer."
                                )
                                st.rerun()

                st.markdown(
                    "<hr style='margin:6px 0;border:none;border-top:1px solid #eee;'>",
                    unsafe_allow_html=True
                )

    # ---- LEDGER -------------------------------------------------------
    with tab_ledger:
        st.subheader("Full Event Ledger")
        if events_df.empty:
            st.info("No events yet.")
        else:
            st.dataframe(
                events_df[["event_id", "notify_type", "pattern", "timestamp"]],
                use_container_width=True
            )

    # ---- KLD ANALYTICS ------------------------------------------------
    with tab_kld:
        st.subheader("KLD Relevance Analytics")
        st.caption(
            "Top events ranked by Kullback-Leibler Divergence from the prior "
            "distribution P. Each row shows the KLD score and which Boolean "
            "features drove it."
        )

        conn = sqlite3.connect(DB_PATH)
        try:
            top_df = pd.read_sql_query(
                "SELECT notify_type, relevance_score, payload, created_at "
                "FROM inbox WHERE recipient = 'VET' "
                "ORDER BY relevance_score DESC LIMIT 10",
                conn
            )
        except Exception:
            top_df = pd.DataFrame()
        conn.close()

        if top_df.empty:
            st.info("No events scored yet. Send some notifications first.")
        else:
            top_df["payload_json"] = top_df["payload"].apply(safe_json)
            top_df["actor"] = top_df["payload_json"].apply(
                lambda p: (p.get("actor") or {}).get("name", "—")
            )
            top_df["summary"] = top_df["payload_json"].apply(
                lambda p: (p.get("summary") or "")[:60]
            )

            # ── Bar chart ────────────────────────────────────────────────
            chart_df = top_df[["notify_type", "relevance_score"]].copy()
            chart_df["label"] = (
                chart_df["notify_type"].astype(str)
                + " ("
                + top_df["actor"]
                + ")"
            )
            chart_df = chart_df.set_index("label")[["relevance_score"]]
            st.bar_chart(chart_df, height=280)

            st.markdown("---")
            st.markdown("**Feature breakdown for top events**")

            feature_rows = []
            for _, row in top_df.iterrows():
                fv = get_feature_vector(row["payload_json"])
                feature_rows.append({
                    "Type":        row["notify_type"],
                    "Actor":       row["actor"],
                    "KLD Score":   round(float(row["relevance_score"]), 3),
                    "b1 HighRisk": "✓" if fv[0] else "·",
                    "b2 Cattle":   "✓" if fv[1] else "·",
                    "b3 Alert":    "✓" if fv[2] else "·",
                    "b4 CrossDist":"✓" if fv[3] else "·",
                    "b5 HiPurpose":"✓" if fv[4] else "·",
                    "b6 NewRecord":"✓" if fv[5] else "·",
                    "Summary":     row["summary"],
                })

            st.dataframe(pd.DataFrame(feature_rows), use_container_width=True)

            # ── Prior reference card ──────────────────────────────────────
            with st.expander("Prior distribution P (baseline)"):
                prior_df = pd.DataFrame({
                    "Feature":  KLD_FEATURE_LABELS,
                    "Prior pᵢ": KLD_PRIOR_DASH,
                })
                st.dataframe(prior_df, use_container_width=True, hide_index=True)
                st.caption(
                    "Events whose feature distribution diverges most from P "
                    "receive the highest KLD score and appear at the top of the Inbox."
                )


# =========================================================
# ADMIN VIEW
# =========================================================
def admin_view():
    st.title("🧑‍💼 Admin")

    tab_ledger, tab_animals, tab_permits, tab_offers = st.tabs([
        "📜 Event Ledger", "🐄 Animals", "🚚 Permits", "🔁 Transfer Offers"
    ])

    with tab_ledger:
        df = load_events()
        if df.empty:
            st.info("No events yet.")
        else:
            st.dataframe(df[["event_id", "notify_type", "pattern", "timestamp"]], use_container_width=True)

    with tab_animals:
        conn = sqlite3.connect(DB_PATH)
        try:
            animals = pd.read_sql_query(
                "SELECT tag_id, owner_id, breed, registered_at FROM animals ORDER BY owner_id, tag_id",
                conn
            )
        except Exception:
            animals = pd.DataFrame()
        conn.close()
        if animals.empty:
            st.info("No animals.")
        else:
            st.dataframe(animals, use_container_width=True)

    with tab_permits:
        conn = sqlite3.connect(DB_PATH)
        try:
            permits = pd.read_sql_query(
                "SELECT * FROM movement_permits ORDER BY request_timestamp DESC", conn
            )
        except Exception:
            permits = pd.DataFrame()
        conn.close()
        if permits.empty:
            st.info("No permits.")
        else:
            st.dataframe(permits, use_container_width=True)

    with tab_offers:
        conn = sqlite3.connect(DB_PATH)
        try:
            offers = pd.read_sql_query(
                "SELECT * FROM transfer_offers ORDER BY offered_at DESC", conn
            )
        except Exception:
            offers = pd.DataFrame()
        conn.close()
        if offers.empty:
            st.info("No transfer offers.")
        else:
            st.dataframe(offers, use_container_width=True)


# =========================================================
# MAIN
# =========================================================
def main():
    role, farmer_id = sidebar_selector()
    if role == "Farmer":
        farmer_view(farmer_id)
    elif role == "Veterinary Officer":
        vet_view()
    elif role == "Admin":
        admin_view()


main()
