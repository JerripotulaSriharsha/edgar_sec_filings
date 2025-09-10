import os
import requests
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Get API credentials from environment variables
API_KEY = os.getenv("alpaca_api")
API_SECRET = os.getenv("alpaca_secret")
BASE_URL = "https://data.alpaca.markets/v2"

# Check if API credentials are loaded
if not API_KEY or not API_SECRET:
    print("Error: API credentials not found in .env file")
    print("Please ensure alpaca_api and alpaca_secret are set in your .env file")
    exit(1)

print(f"API Key loaded: {API_KEY[:8]}...")
print(f"API Secret loaded: {API_SECRET[:8]}...")

symbol = "AAPL"
endpoint = f"{BASE_URL}/stocks/{symbol}/bars"

end = datetime.utcnow()
start = end - timedelta(days=5*365)   # ~5 years

params = {
    "timeframe": "1Hour",
    "start": start.isoformat() + "Z",
    "end": end.isoformat() + "Z",
    "limit": 10000,
    "adjustment": "all",
    "feed": "iex",
    "sort": "asc"
}

headers = {
    "APCA-API-KEY-ID": API_KEY,
    "APCA-API-SECRET-KEY": API_SECRET
}

print(f"Fetching data for {symbol} from {start.date()} to {end.date()}")
print("This may take a few minutes...")

rows = []
page_count = 0
while True:
    page_count += 1
    print(f"Fetching page {page_count}...")
    
    r = requests.get(endpoint, headers=headers, params=params)
    r.raise_for_status()
    data = r.json()
    bars = data.get("bars", [])
    
    if not bars:
        break
    
    rows.extend(bars)
    print(f"  Got {len(bars)} bars, total: {len(rows)}")
    
    if "next_page_token" in data and data["next_page_token"]:
        params["page_token"] = data["next_page_token"]
    else:
        break

print(f"\nTotal bars fetched: {len(rows)}")

if rows:
    df = pd.DataFrame(rows)
    df.rename(columns={"t":"time","o":"open","h":"high","l":"low","c":"close","v":"volume"}, inplace=True)
    df["time"] = pd.to_datetime(df["time"])
    
    # Convert timezone-aware datetimes to timezone-naive for Excel compatibility
    df["time"] = df["time"].dt.tz_localize(None)
    
    print("\nDataFrame Info:")
    print(df.info())
    
    print("\nFirst 5 rows:")
    print(df.head())
    
    print("\nLast 5 rows:")
    print(df.tail())
    
    print(f"\nDate range: {df['time'].min()} to {df['time'].max()}")
    print(f"Total rows: {len(df)}")
    
    # Save to CSV
    output_filename = f"{symbol}_hourly_data.csv"
    df.to_csv(output_filename, index=False)
    print(f"\nData saved to {output_filename}")
    
    # Save to Excel if openpyxl is available
    try:
        excel_filename = f"{symbol}_hourly_data.xlsx"
        df.to_excel(excel_filename, index=False, engine='openpyxl')
        print(f"Data also saved to {excel_filename}")
    except ImportError:
        print("openpyxl not available, Excel export skipped")
        
else:
    print("No data was fetched. Check your API credentials and symbol.")
