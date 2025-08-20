#!/usr/bin/env python3
"""
Simple SEC EDGAR poller - clean version based on poll_sec.py
Polls EFTS every 60 seconds for new filings
"""

import datetime as dt
import pathlib
import sqlite3
import time
import requests
from typing import Dict, List

# ──────────────────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────────────────
DB_PATH = pathlib.Path("filings.db")
USER_AGENT = "sec-poller/1.0 (you@example.com)"

BASE_URL = "https://efts.sec.gov/LATEST/search-index"
HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "application/json",
    "Accept-Encoding": "gzip"
}

# ──────────────────────────────────────────────────────────
# Database setup
# ──────────────────────────────────────────────────────────
def init_db(path: pathlib.Path) -> sqlite3.Connection:
    print(f"[INFO] Initializing database at {path}")
    conn = sqlite3.connect(path)
    cur = conn.cursor()
    
    cur.execute("""CREATE TABLE IF NOT EXISTS adsh_seen (
                       adsh TEXT PRIMARY KEY,
                       first_seen_ts TEXT
                   )""")
    
    cur.execute("""CREATE TABLE IF NOT EXISTS dispatch_queue (
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
                   )""")
    
    conn.commit()
    print(f"[INFO] Database initialized successfully")
    return conn

# ──────────────────────────────────────────────────────────
# Fetch latest filings from EFTS
# ──────────────────────────────────────────────────────────
def fetch_latest(date_iso: str) -> List[Dict]:
    print(f"[POLL] Fetching latest filings for {date_iso}")
    all_hits = []
    
    # Fetch multiple pages to get more than 100 filings
    for page in range(1, 6):  # Get pages 1, 2, 3, 4, 5 (up to 500 filings)
        params = {
            "forms": "-0",     # all forms
            "startdt": date_iso,
            "enddt": date_iso,
            "page": page,
            "from": (page-1)*100,
        }
        
        try:
            r = requests.get(BASE_URL, params=params, headers=HEADERS, timeout=10)
            r.raise_for_status()
            hits = r.json()["hits"]["hits"]
            
            if not hits:  # No more filings on this page
                break
                
            all_hits.extend(hits)
            print(f"[POLL] Page {page}: Found {len(hits)} filings")
            
            if len(hits) < 100:  # Last page has fewer than 100 filings
                break
                
        except Exception as e:
            print(f"[ERROR] Failed to fetch page {page}: {e}")
            break
    
    print(f"[POLL] Total filings found: {len(all_hits)}")
    return all_hits

# ──────────────────────────────────────────────────────────
# Build URL for filing
# ──────────────────────────────────────────────────────────
def build_url(hit: Dict) -> str:
    src = hit["_source"]
    cik = src["ciks"][0].lstrip("0")
    accession = src["adsh"].replace("-", "")
    primary = hit["_id"].split(":", 1)[1]
    return f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession}/{primary}"

# ──────────────────────────────────────────────────────────
# Main polling loop
# ──────────────────────────────────────────────────────────
def main() -> None:
    print("[START] SEC EDGAR poller starting up...")
    print(f"[CONFIG] Database: {DB_PATH}")
    print(f"[CONFIG] User Agent: {USER_AGENT}")
    print("[INFO] Polling every 1 second for new SEC filings (MAXIMUM SPEED - 60x faster!)...")
    print("[INFO] Press Ctrl+C to stop")
    print("-" * 60)
    
    conn = init_db(DB_PATH)
    cur = conn.cursor()

    while True:
        start = time.time()
        today = dt.date.today().isoformat()

        try:
            for hit in fetch_latest(today):
                adsh = hit["_source"]["adsh"]

                # Check if already seen (INSERT OR IGNORE = fast O(1) check)
                cur.execute("INSERT OR IGNORE INTO adsh_seen(adsh, first_seen_ts) VALUES(?, datetime('now'))",
                            (adsh,))
                if cur.rowcount == 0:          # already seen
                    continue

                form = hit["_source"]["form"]
                cik = hit["_source"]["ciks"][0]
                url = build_url(hit)
                
                # Extract additional attributes
                company_name = hit["_source"].get("companyName", "")
                filing_date = hit["_source"].get("filingDate", "")
                filing_href = hit["_source"].get("filingHref", "")

                cur.execute("""INSERT INTO dispatch_queue(adsh, form, cik, url, company_name, filing_date, filing_href, enqueued_ts)
                               VALUES(?, ?, ?, ?, ?, ?, ?, datetime('now'))""",
                            (adsh, form, cik, url, company_name, filing_date, filing_href))
                print(f"[NEW] {adsh} {form} {company_name} → queued", flush=True)

            conn.commit()
            print(f"[LOOP] Polling cycle completed at {dt.datetime.now().strftime('%H:%M:%S')}")

        except Exception as exc:
            print(f"[ERROR] {exc}", flush=True)

        # Sleep until 1 second since loop start (MAXIMUM SPEED - 60x faster!)
        sleep_left = 1 - (time.time() - start)
        if sleep_left > 0:
            print(f"[SLEEP] Waiting {sleep_left:.1f} seconds until next poll...")
            time.sleep(sleep_left)

if __name__ == "__main__":
    main()
