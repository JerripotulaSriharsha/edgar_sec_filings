#!/usr/bin/env python3
"""
SEC Filing Polling and Monitoring Tool

This script monitors SEC filings for specified companies and filing types,
stores the data in a local database, and provides analysis capabilities.
"""

import sqlite3
import time
import logging
from datetime import datetime, timedelta
from pathlib import Path
import os
import sys
import pandas as pd

# Add the current directory to Python path for imports
sys.path.append(str(Path(__file__).parent))

try:
    from edgar import Company, set_identity
    EDGAR_AVAILABLE = True
except ImportError:
    print("Warning: edgar library not available. Install with: pip install edgar")
    EDGAR_AVAILABLE = False

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('sec_polling.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class SECFilingMonitor:
    """Monitor SEC filings for specified companies and filing types"""
    
    def __init__(self, db_path="filings.db"):
        self.db_path = db_path
        self.init_database()
        
        # Default companies to monitor
        self.default_companies = [
            "AAPL",  # Apple Inc.
            "MSFT",  # Microsoft Corporation
            "GOOGL", # Alphabet Inc.
            "AMZN",  # Amazon.com Inc.
            "TSLA",  # Tesla Inc.
        ]
        
        # Default filing types to monitor
        self.default_filing_types = [
            "10-K",    # Annual Report
            "10-Q",    # Quarterly Report
            "8-K",     # Current Report
            "13F-HR",  # Institutional Investment Manager Holdings
            "4",        # Statement of Changes in Beneficial Ownership
            "5",        # Annual Statement of Changes in Beneficial Ownership
        ]
    
    def init_database(self):
        """Initialize SQLite database for storing filing data"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Create filings table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS filings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                company_ticker TEXT NOT NULL,
                company_name TEXT,
                form_type TEXT NOT NULL,
                filing_date TEXT NOT NULL,
                accession_number TEXT UNIQUE NOT NULL,
                description TEXT,
                file_size INTEGER,
                url TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Create financial_data table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS financial_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                filing_id INTEGER NOT NULL,
                metric_name TEXT NOT NULL,
                metric_value REAL,
                period TEXT,
                units TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (filing_id) REFERENCES filings (id)
            )
        ''')
        
        # Create insider_trades table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS insider_trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                filing_id INTEGER NOT NULL,
                insider_name TEXT,
                title TEXT,
                transaction_date TEXT,
                transaction_code TEXT,
                shares_traded INTEGER,
                shares_owned_after INTEGER,
                price_per_share REAL,
                total_value REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (filing_id) REFERENCES filings (id)
            )
        ''')
        
        # Create institutional_holdings table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS institutional_holdings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                filing_id INTEGER NOT NULL,
                issuer_name TEXT,
                cusip TEXT,
                shares_held INTEGER,
                market_value REAL,
                investment_discretion TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (filing_id) REFERENCES filings (id)
            )
        ''')
        
        # Create indexes for better performance
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_filings_company ON filings(company_ticker)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_filings_date ON filings(filing_date)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_filings_form ON filings(form_type)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_financial_data_filing ON financial_data(filing_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_insider_trades_filing ON insider_trades(filing_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_institutional_holdings_filing ON institutional_holdings(filing_id)')
        
        conn.commit()
        conn.close()
        logger.info("Database initialized successfully")
    
    def set_identity(self, email):
        """Set SEC identity for API access"""
        if not EDGAR_AVAILABLE:
            logger.error("edgar library not available")
            return False
        
        try:
            set_identity(email)
            logger.info(f"SEC identity set to: {email}")
            return True
        except Exception as e:
            logger.error(f"Failed to set SEC identity: {e}")
            return False
    
    def get_company_filings(self, ticker, form_type=None, limit=50):
        """Get recent filings for a specific company"""
        if not EDGAR_AVAILABLE:
            logger.error("edgar library not available")
            return []
        
        try:
            company = Company(ticker)
            
            if form_type:
                filings = company.get_filings(form=form_type, limit=limit)
            else:
                filings = company.recent_filings(limit=limit)
            
            logger.info(f"Retrieved {len(filings)} filings for {ticker}")
            return filings
            
        except Exception as e:
            logger.error(f"Error getting filings for {ticker}: {e}")
            return []
    
    def store_filing(self, ticker, filing):
        """Store filing metadata in database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # Check if filing already exists
            cursor.execute('''
                SELECT id FROM filings WHERE accession_number = ?
            ''', (filing.accession_number,))
            
            existing = cursor.fetchone()
            
            if existing:
                # Update existing filing
                cursor.execute('''
                    UPDATE filings SET
                        company_name = ?,
                        description = ?,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE accession_number = ?
                ''', (
                    getattr(filing, 'company_name', ''),
                    filing.description,
                    filing.accession_number
                ))
                filing_id = existing[0]
                logger.debug(f"Updated existing filing: {filing.accession_number}")
            else:
                # Insert new filing
                cursor.execute('''
                    INSERT INTO filings 
                    (company_ticker, company_name, form_type, filing_date, 
                     accession_number, description, url)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (
                    ticker,
                    getattr(filing, 'company_name', ''),
                    filing.form,
                    filing.filing_date,
                    filing.accession_number,
                    filing.description,
                    getattr(filing, 'url', '')
                ))
                filing_id = cursor.lastrowid
                logger.info(f"Stored new filing: {filing.accession_number} for {ticker}")
            
            conn.commit()
            return filing_id
            
        except Exception as e:
            logger.error(f"Error storing filing {filing.accession_number}: {e}")
            conn.rollback()
            return None
        finally:
            conn.close()
    
    def store_financial_data(self, filing_id, financial_data):
        """Store extracted financial data"""
        if not financial_data or financial_data.empty:
            return
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            for metric_name, values in financial_data.items():
                for period, value in values.items():
                    if pd.notna(value) and value != 0:
                        cursor.execute('''
                            INSERT OR REPLACE INTO financial_data 
                            (filing_id, metric_name, metric_value, period)
                            VALUES (?, ?, ?, ?)
                        ''', (filing_id, metric_name, value, str(period)))
            
            conn.commit()
            logger.info(f"Stored financial data for filing {filing_id}")
            
        except Exception as e:
            logger.error(f"Error storing financial data: {e}")
            conn.rollback()
        finally:
            conn.close()
    
    def monitor_filings(self, companies=None, filing_types=None, days_back=30):
        """Monitor filings for specified companies and filing types"""
        if companies is None:
            companies = self.default_companies
        
        if filing_types is None:
            filing_types = self.default_filing_types
        
        logger.info(f"Starting monitoring for {len(companies)} companies")
        logger.info(f"Monitoring filing types: {filing_types}")
        
        total_new_filings = 0
        
        for ticker in companies:
            logger.info(f"Processing company: {ticker}")
            
            for form_type in filing_types:
                try:
                    filings = self.get_company_filings(ticker, form_type, limit=20)
                    
                    for filing in filings:
                        # Check if filing is within our time window
                        filing_date = datetime.strptime(filing.filing_date, '%Y-%m-%d')
                        if filing_date >= datetime.now() - timedelta(days=days_back):
                            filing_id = self.store_filing(ticker, filing)
                            if filing_id:
                                total_new_filings += 1
                                
                                # Try to extract and store financial data for 10-K and 10-Q
                                if form_type in ['10-K', '10-Q']:
                                    try:
                                        xbrl = filing.xbrl()
                                        if xbrl and hasattr(xbrl, 'statements'):
                                            statements = xbrl.statements
                                            
                                            # Extract balance sheet
                                            if hasattr(statements, 'balance_sheet'):
                                                balance_sheet = statements.balance_sheet()
                                                self.store_financial_data(filing_id, balance_sheet)
                                            
                                            # Extract income statement
                                            if hasattr(statements, 'income_statement'):
                                                income_statement = statements.income_statement()
                                                self.store_financial_data(filing_id, income_statement)
                                            
                                            # Extract cash flow statement
                                            if hasattr(statements, 'cashflow_statement'):
                                                cash_flow = statements.cashflow_statement()
                                                self.store_financial_data(filing_id, cash_flow)
                                                
                                    except Exception as e:
                                        logger.warning(f"Could not extract XBRL data for {filing.accession_number}: {e}")
                    
                    # Rate limiting - be respectful to SEC servers
                    time.sleep(1)
                    
                except Exception as e:
                    logger.error(f"Error processing {form_type} for {ticker}: {e}")
                    continue
        
        logger.info(f"Monitoring completed. Found {total_new_filings} new filings")
        return total_new_filings
    
    def get_filing_summary(self, days=30):
        """Get summary of recent filings"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # Get filings in the last N days
            cutoff_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
            
            cursor.execute('''
                SELECT 
                    company_ticker,
                    form_type,
                    COUNT(*) as filing_count,
                    MAX(filing_date) as latest_filing
                FROM filings 
                WHERE filing_date >= ?
                GROUP BY company_ticker, form_type
                ORDER BY company_ticker, form_type
            ''', (cutoff_date,))
            
            results = cursor.fetchall()
            
            if results:
                print(f"\nFiling Summary (Last {days} days):")
                print("=" * 60)
                print(f"{'Company':<8} {'Form':<8} {'Count':<6} {'Latest Filing':<15}")
                print("-" * 60)
                
                for row in results:
                    ticker, form_type, count, latest = row
                    print(f"{ticker:<8} {form_type:<8} {count:<6} {latest:<15}")
            else:
                print(f"No filings found in the last {days} days")
                
        except Exception as e:
            logger.error(f"Error getting filing summary: {e}")
        finally:
            conn.close()
    
    def search_filings(self, company_ticker=None, form_type=None, date_from=None, date_to=None):
        """Search filings with various filters"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            query = "SELECT * FROM filings WHERE 1=1"
            params = []
            
            if company_ticker:
                query += " AND company_ticker = ?"
                params.append(company_ticker)
            
            if form_type:
                query += " AND form_type = ?"
                params.append(form_type)
            
            if date_from:
                query += " AND filing_date >= ?"
                params.append(date_from)
            
            if date_to:
                query += " AND filing_date <= ?"
                params.append(date_to)
            
            query += " ORDER BY filing_date DESC LIMIT 100"
            
            cursor.execute(query, params)
            results = cursor.fetchall()
            
            if results:
                print(f"\nSearch Results ({len(results)} filings):")
                print("=" * 100)
                print(f"{'Date':<12} {'Company':<8} {'Form':<8} {'Accession':<20} {'Description':<50}")
                print("-" * 100)
                
                for row in results:
                    filing_date, ticker, form, accession, description = row[4], row[1], row[3], row[5], row[6]
                    desc_short = (description[:47] + '...') if len(description) > 50 else description
                    print(f"{filing_date:<12} {ticker:<8} {form:<8} {accession:<20} {desc_short:<50}")
            else:
                print("No filings found matching the search criteria")
                
        except Exception as e:
            logger.error(f"Error searching filings: {e}")
        finally:
            conn.close()

def main():
    """Main function for command-line usage"""
    import argparse
    
    parser = argparse.ArgumentParser(description='SEC Filing Monitor')
    parser.add_argument('--companies', nargs='+', help='Company tickers to monitor')
    parser.add_argument('--forms', nargs='+', help='Filing forms to monitor')
    parser.add_argument('--days', type=int, default=30, help='Days back to monitor')
    parser.add_argument('--summary', action='store_true', help='Show filing summary')
    parser.add_argument('--search', action='store_true', help='Search existing filings')
    parser.add_argument('--identity', help='SEC identity email')
    
    args = parser.parse_args()
    
    # Initialize monitor
    monitor = SECFilingMonitor()
    
    # Set identity if provided
    if args.identity:
        if not monitor.set_identity(args.identity):
            print("Failed to set SEC identity. Exiting.")
            return
    
    # Show summary if requested
    if args.summary:
        monitor.get_filing_summary(args.days)
        return
    
    # Search if requested
    if args.search:
        monitor.search_filings()
        return
    
    # Monitor filings
    companies = args.companies if args.companies else None
    forms = args.forms if args.forms else None
    
    print("Starting SEC filing monitoring...")
    new_filings = monitor.monitor_filings(companies, forms, args.days)
    print(f"Monitoring completed. Found {new_filings} new filings.")

if __name__ == "__main__":
    main()
