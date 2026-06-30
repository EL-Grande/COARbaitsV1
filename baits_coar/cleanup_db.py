"""
Cleanup script — wipes all notification/permit/inbox/outbox data so
you can start fresh with the AS2 / COAR Notify event schema.

Usage:
    python cleanup_db.py
"""

import sqlite3
import os

DB_PATH = "/app/data/baits_system.db"


def reset_database():
    if not os.path.exists(DB_PATH):
        print(f"No database found at {DB_PATH} — nothing to clean.")
        return

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # DROP rather than DELETE: the schema itself changed (events now
    # uses notify_type/pattern columns instead of event_type/status),
    # so old tables must be recreated, not just emptied.
    tables = [
        "events", "movement_permits", "inbox", "outbox",
        "animals", "transfer_offers", "district_risk", "disease_alerts",
    ]

    for table in tables:
        cur.execute(f"DROP TABLE IF EXISTS {table}")
        print(f"✅ Dropped table: {table}")

    conn.commit()
    cur.execute("VACUUM")
    conn.commit()
    conn.close()

    print("\n🎉 Database reset complete.")
    print("   Restart ldn_inbox so init_db() recreates the AS2 schema.")


if __name__ == "__main__":
    reset_database()
