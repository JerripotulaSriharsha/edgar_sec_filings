# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

**Environment Setup:**
```bash
pip install -r requirements.txt
```

**Run the SEC Polling Service:**
```bash
python poll_sec.py
```

**Start Jupyter Notebook (for analysis):**
```bash
jupyter notebook
```

## Architecture

This is a SEC filings analysis system with two main components:

### 1. Real-time Polling Service (`poll_sec.py`)
- Polls EDGAR's `/LATEST/search-index` every 60 seconds for new filings
- Uses SQLite database (`filings.db`) with two tables:
  - `adsh_seen`: Deduplication tracking by accession number (adsh)
  - `dispatch_queue`: New filings queue for downstream processing
- Builds direct URLs to SEC filing documents
- Requires User-Agent header with email (currently set to placeholder)

### 2. Analysis Tools (`edgar_tools.ipynb`)
- Uses `edgartools` library for structured data extraction
- Demonstrates parsing of:
  - 10-K/10-Q financial statements (balance sheet, income statement, cash flow)
  - Form 3/4/5 insider trading filings
  - 13F-HR institutional holdings
  - NPORT-P fund reports
- Converts XBRL data to pandas DataFrames for analysis

## Key Configuration

- `DB_PATH`: SQLite database location (default: `filings.db`)
- `USER_AGENT`: Must include valid email for SEC compliance
- Base URL points to EDGAR search API

The polling service is designed as a "fire-hose" collector that queues new filings for separate processing workers.