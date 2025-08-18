#!/usr/bin/env python3
"""
Standalone database initializer.
- Creates a SQLite database in this folder: database/filings.db
- Defines tables used by the pollers (idempotent):
  * seen(adsh TEXT PRIMARY KEY)
  * adsh_seen(adsh TEXT PRIMARY KEY, first_seen_ts TEXT)
  * dispatch_queue(...)

This script is intentionally isolated; it is not imported by the app.
Run directly to (re)create the database schema:
    python database/create_db.py
"""

import sqlite3
import pathlib


DB_PATH = pathlib.Path(__file__).parent / "filings.db"


def create_database(db_path: pathlib.Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # Minimal table used by the test poller for de-duplication
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS seen (
            adsh TEXT PRIMARY KEY
        )
        """
    )

    # De-dup table used by the production poller
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS adsh_seen (
            adsh TEXT PRIMARY KEY,
            first_seen_ts TEXT
        )
        """
    )

    # Queue for downstream processing
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS dispatch_queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            adsh TEXT,
            form TEXT,
            cik  TEXT,
            url  TEXT,
            company_name TEXT,
            filing_date TEXT,
            filing_href TEXT,
            enqueued_ts TEXT,
            processed INTEGER DEFAULT 0
        )
        """
    )

    conn.commit()
    conn.close()


if __name__ == "__main__":
    create_database(DB_PATH)
    print(f"[DB] Initialized database at: {DB_PATH}")


