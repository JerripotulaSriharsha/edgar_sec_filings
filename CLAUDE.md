# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

**Environment Setup:**
```bash
pip install -r requirements.txt
```

**Run the SEC Polling Service:**
```bash
python src/poll_sec.py
```

**Run the Complete Processing Pipeline:**
```bash
python run_processor.py
```

**Start Jupyter Notebook (for analysis):**
```bash
jupyter notebook
```

## Architecture

This is a SEC filings analysis system with a three-layer architecture:

### 1. Data Collection Layer (`src/poll_sec.py`)
- Polls EDGAR's `/LATEST/search-index` every 60 seconds for new filings
- Uses SQLite database (`filings.db`) with two tables:
  - `adsh_seen`: Deduplication tracking by accession number (adsh)
  - `dispatch_queue`: New filings queue with company name, filing date, URLs
- Builds direct URLs to SEC filing documents
- Requires User-Agent header with email (currently set to placeholder)

### 2. Extraction Layer (`extractors/`)
- **BaseExtractor**: Abstract base class for all form-specific extractors
- **Form10KExtractor**: Extracts data from 10-K annual reports 
- Modular design allows easy addition of new form types (8-K, 4, 13F-HR, etc.)
- Each extractor inherits common methods like `get_form_type()` and `get_cik()`

### 3. Signal Generation Layer (`signals/`)
- **SignalGenerator**: Processes extracted filing data to generate investment/trading signals
- Placeholder implementation ready for custom signal logic
- Tracks signal type, strength, reasoning, and timestamps
- Maintains history of all generated signals

### 4. Processing Coordination (`run_processor.py`)
- Orchestrates the complete pipeline from data extraction to signal generation
- References `src.processor.FilingProcessor` (main coordinator class)
- Outputs summary of generated signals by CIK and form type

### 5. Analysis Tools (`test/*.ipynb`)
- Jupyter notebooks for interactive analysis and research
- Uses `edgartools`, `yfinance`, and other financial libraries
- `filings_extraction.ipynb`: Filing data extraction examples
- `stock_research.ipynb`: Stock analysis and research

## Key Configuration

Configuration is centralized in `config/settings.py`:
- `DATABASE_PATH`: SQLite database location
- `USER_AGENT`: Must include valid email for SEC compliance  
- `POLL_INTERVAL_SECONDS`: Polling frequency (default: 60)
- `SUPPORTED_FORMS`: List of form types to process (10-K, 10-Q, 8-K, 4, 13F-HR)

The system follows a producer-consumer pattern where the poller continuously feeds new filings into a queue for batch processing by downstream extractors and signal generators.