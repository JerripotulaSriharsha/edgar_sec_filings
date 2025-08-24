#!/usr/bin/env python3
"""
historical_filings_collector.py – SEC EDGAR historical data collector
–––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––
• Fetches all SEC filings from the past 365 days
• Uses same database schema as poll_sec.py and pol_sec_test.py
• Creates backtest database for historical analysis
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
BACKTEST_DB_PATH = pathlib.Path("backtest/historical_filings.db")
USER_AGENT = "sec-poller/1.0 (you@example.com)"   # use your email

BASE_URL = "https://efts.sec.gov/LATEST/search-index"
HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "application/json",
    "Accept-Encoding": "gzip"
}

# ──────────────────────────────────────────────────────────
# Database setup (same schema as poll_sec.py)
# ──────────────────────────────────────────────────────────
def init_backtest_db(path: pathlib.Path) -> sqlite3.Connection:
    # Create backtest directory if it doesn't exist
    path.parent.mkdir(exist_ok=True)
    
    print(f"[INFO] Initializing backtest database at {path}")
    conn = sqlite3.connect(path)
    cur = conn.cursor()
    
    # Same schema as poll_sec.py
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
    print(f"[INFO] Backtest database initialized successfully")
    return conn

# ──────────────────────────────────────────────────────────
# Fetch filings for a specific date (multiple pages)
# ──────────────────────────────────────────────────────────
def fetch_filings_for_date(date_iso: str) -> List[Dict]:
    print(f"[FETCH] Getting all filings for {date_iso}")
    all_hits = []
    
    # Fetch multiple pages to get all filings for the day
    for page in range(1, 21):  # Get up to 20 pages (2000 filings max per day)
        params = {
            "forms": "-0",     # all forms
            "startdt": date_iso,
            "enddt": date_iso,
            "page": page,
            "from": (page-1)*100,
        }
        
        try:
            r = requests.get(BASE_URL, params=params, headers=HEADERS, timeout=15)
            r.raise_for_status()
            hits = r.json()["hits"]["hits"]
            
            if not hits:  # No more filings on this page
                break
                
            all_hits.extend(hits)
            print(f"[FETCH] Page {page}: Found {len(hits)} filings")
            
            if len(hits) < 100:  # Last page has fewer than 100 filings
                break
                
            # Rate limiting - be respectful to SEC servers
            time.sleep(0.1)
            
        except Exception as e:
            print(f"[ERROR] Failed to fetch page {page} for {date_iso}: {e}")
            break
    
    print(f"[FETCH] Total filings for {date_iso}: {len(all_hits)}")
    return all_hits

# ──────────────────────────────────────────────────────────
# Build URL for filing (same as poll_sec.py)
# ──────────────────────────────────────────────────────────
def build_url(hit: Dict) -> str:
    src = hit["_source"]
    cik = src["ciks"][0].lstrip("0")
    accession = src["adsh"].replace("-", "")
    primary = hit["_id"].split(":", 1)[1]
    return f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession}/{primary}"

# ──────────────────────────────────────────────────────────
# Generate date range for last 365 days (all business days)
# ──────────────────────────────────────────────────────────
def get_date_range(days_back: int = 365) -> List[str]:
    today = dt.date.today()
    dates = []
    
    # Get all business days from the last 365 days (including recent days)
    for i in range(1, days_back + 1):  # 1 to 365 days back from today
        date = today - dt.timedelta(days=i)
        # Skip weekends (SEC doesn't publish on weekends)
        if date.weekday() < 5:  # 0=Monday, 6=Sunday
            dates.append(date.isoformat())
    
    return sorted(dates)  # Start from oldest to newest

# ──────────────────────────────────────────────────────────
# Main collection process
# ──────────────────────────────────────────────────────────
def main() -> None:
    print("[START] Historical SEC EDGAR filings collector starting...")
    print(f"[CONFIG] Backtest Database: {BACKTEST_DB_PATH}")
    print(f"[CONFIG] User Agent: {USER_AGENT}")
    print("[INFO] Collecting ALL filings from the last 365 days (including recent days)...")
    print("-" * 60)
    
    conn = init_backtest_db(BACKTEST_DB_PATH)
    cur = conn.cursor()
    
    # Get all business days from the last 365 days (including recent days)
    date_range = get_date_range(365)
    print(f"[INFO] Will process {len(date_range)} business days from the last 365 days")
    print(f"[INFO] Date range: {date_range[0]} to {date_range[-1]}")
    
    total_new_filings = 0
    total_duplicate_filings = 0
    processed_dates = 0
    
    for date_str in date_range:
        processed_dates += 1
        print(f"\n[DATE] Processing {date_str}... ({processed_dates}/{len(date_range)} dates)")
        
        try:
            filings = fetch_filings_for_date(date_str)
            new_for_date = 0
            duplicates_for_date = 0
            
            for hit in filings:
                adsh = hit["_source"]["adsh"]
                
                # Check for duplicates (same logic as poll_sec.py)
                cur.execute("INSERT OR IGNORE INTO adsh_seen(adsh, first_seen_ts) VALUES(?, datetime('now'))",
                           (adsh,))
                
                if cur.rowcount == 0:  # already seen
                    duplicates_for_date += 1
                    continue
                
                # Extract filing data (same format as poll_sec.py)
                form = hit["_source"]["form"]
                cik = hit["_source"]["ciks"][0]
                url = build_url(hit)
                company_name = hit["_source"].get("companyName", "")
                filing_date = hit["_source"].get("filingDate", "")
                filing_href = hit["_source"].get("filingHref", "")
                
                # Insert into dispatch queue
                cur.execute("""INSERT INTO dispatch_queue(adsh, form, cik, url, company_name, filing_date, filing_href, enqueued_ts)
                               VALUES(?, ?, ?, ?, ?, ?, ?, datetime('now'))""",
                           (adsh, form, cik, url, company_name, filing_date, filing_href))
                
                new_for_date += 1
                if new_for_date % 100 == 0:  # Progress indicator
                    print(f"[PROGRESS] {new_for_date} new filings processed for {date_str}")
            
            conn.commit()
            total_new_filings += new_for_date
            total_duplicate_filings += duplicates_for_date
            
            print(f"[SUMMARY] {date_str}: {new_for_date} new, {duplicates_for_date} duplicates")
            print(f"[OVERALL] Total progress: {total_new_filings} new filings collected so far")
            
            # Rate limiting between dates
            time.sleep(1)
            
        except Exception as exc:
            print(f"[ERROR] Failed processing {date_str}: {exc}")
    
    # Final summary
    print("\n" + "=" * 60)
    print(f"[COMPLETE] Historical data collection finished!")
    print(f"[STATS] Total new filings collected: {total_new_filings}")
    print(f"[STATS] Total duplicates skipped: {total_duplicate_filings}")
    print(f"[STATS] Total business days processed: {processed_dates}")
    print(f"[STATS] Database location: {BACKTEST_DB_PATH}")
    
    # Show some database stats
    cur.execute("SELECT COUNT(*) FROM dispatch_queue")
    queue_count = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(*) FROM adsh_seen")
    seen_count = cur.fetchone()[0]
    
    print(f"[STATS] Dispatch queue entries: {queue_count}")
    print(f"[STATS] Total unique filings seen: {seen_count}")
    
    # Show form type breakdown
    cur.execute("SELECT form, COUNT(*) FROM dispatch_queue GROUP BY form ORDER BY COUNT(*) DESC LIMIT 10")
    form_stats = cur.fetchall()
    
    print(f"[STATS] Top 10 form types:")
    for form, count in form_stats:
        print(f"  - {form}: {count}")
    
    conn.close()

if __name__ == "__main__":
    main()