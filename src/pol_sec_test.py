#!/usr/bin/env python3
"""
Simple EFTS poller (all forms)
- Polls every ~1 second
- De-dupes on accession (adsh)
- Prints accession, form, cik, and primary doc URL
"""

import asyncio, datetime as dt, random, sqlite3, pathlib
import httpx
import logging
from logging.handlers import RotatingFileHandler

DB_PATH   = pathlib.Path("filings.db")
USER_AGENT = "sec-poller/1.0 (you@example.com)"
EFTS_URL  = "https://efts.sec.gov/LATEST/search-index"
ARCHIVES  = "https://www.sec.gov/Archives/edgar/data"

HEADERS = {"User-Agent": USER_AGENT, "Accept": "application/json"}

# ── Logging setup ─────────────────────────────────────────
def setup_logging() -> logging.Logger:
    logs_dir = pathlib.Path("logs")
    logs_dir.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("pol_sec_test")
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")

        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(formatter)

        file_handler = RotatingFileHandler(logs_dir / "pol_sec_test.log", maxBytes=5_000_000, backupCount=3, encoding="utf-8")
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(formatter)

        logger.addHandler(console_handler)
        logger.addHandler(file_handler)

    return logger

# ── DB setup ────────────────────────────────────────────
def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS seen(adsh TEXT PRIMARY KEY)")
    conn.commit()
    return conn, cur

def already_seen(cur, adsh: str) -> bool:
    cur.execute("INSERT OR IGNORE INTO seen VALUES (?)", (adsh,))
    return cur.rowcount == 0  # True if it was already there

# ── EFTS helper ─────────────────────────────────────────
def build_url(hit: dict) -> tuple[str, str, str, str]:
    src = hit["_source"]
    adsh = src["adsh"]
    form = src.get("form", "")
    cik  = src["ciks"][0].lstrip("0")
    primary = hit["_id"].split(":", 1)[1]
    url  = f"{ARCHIVES}/{cik}/{adsh.replace('-', '')}/{primary}"
    return adsh, form, cik, url

async def poll_loop():
    logger = logging.getLogger("pol_sec_test")
    conn, cur = init_db()
    async with httpx.AsyncClient(headers=HEADERS, http2=True) as client:
        while True:
            today = dt.date.today().isoformat()
            try:
                page = 1
                total_hits = 0
                while True:
                    r = await client.get(
                        EFTS_URL,
                        params={"q": "*", "startdt": today, "enddt": today, "size": 100, "page": page},
                    )
                    hits = r.json().get("hits", {}).get("hits", [])
                    if not hits:
                        break
                    logger.info(f"Fetched {len(hits)} filings for {today} (page {page})")
                    for hit in hits:
                        adsh, form, cik, url = build_url(hit)
                        if not already_seen(cur, adsh):
                            logger.info(f"NEW adsh={adsh} form={form} cik={cik} url={url}")
                    total_hits += len(hits)
                    page += 1
                    # brief pause between pages to be polite
                    await asyncio.sleep(0.1)
                conn.commit()
                logger.info(f"Cycle complete: processed {total_hits} filings for {today}")
            except Exception as e:
                logger.exception("Polling error")
            # sleep ~1s with jitter
            await asyncio.sleep(1 + random.uniform(-0.2, 0.2))

if __name__ == "__main__":
    logger = setup_logging()
    logger.info("Starting pol_sec_test poller")
    asyncio.run(poll_loop())
