"""
Configuration settings for SEC filing analysis
"""

# Database settings
DATABASE_PATH = "filings.db"

# SEC API settings
SEC_BASE_URL = "https://efts.sec.gov/LATEST/search-index"
USER_AGENT = "sec-poller/1.0 (you@example.com)"

# Processing settings
POLL_INTERVAL_SECONDS = 60
MAX_FILINGS_PER_POLL = 100

# Form types to process
SUPPORTED_FORMS = [
    "10-K",    # Annual reports
    "10-Q",    # Quarterly reports
    "8-K",     # Current reports
    "4",       # Insider trading
    "13F-HR"  # Institutional holdings
]
