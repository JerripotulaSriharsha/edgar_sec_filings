"""
Main processor that coordinates the SEC filing pipeline
Connects poller → extractors → signal generators
"""

import sqlite3
from typing import Dict, Any, List
from extractors.form_10k_extractor import Form10KExtractor
from signals.signal_generator import SignalGenerator


class FilingProcessor:
    """Main processor for SEC filings"""
    
    def __init__(self, db_path: str = "filings.db"):
        self.db_path = db_path
        self.signal_generator = SignalGenerator()
    
    def get_pending_filings(self) -> List[Dict[str, Any]]:
        """Get filings from dispatch_queue that haven't been processed"""
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        
        cur.execute("""
            SELECT adsh, form, cik, url, enqueued_ts 
            FROM dispatch_queue 
            WHERE processed = 0
        """)
        
        filings = []
        for row in cur.fetchall():
            filings.append({
                'adsh': row[0],
                'form': row[1],
                'cik': row[2],
                'url': row[3],
                'enqueued_ts': row[4]
            })
        
        conn.close()
        return filings
    
    def process_filing(self, filing: Dict[str, Any]) -> Dict[str, Any]:
        """Process a single filing through extraction and signal generation"""
        
        # TODO: Download and parse the actual filing content
        # For now, just use the basic filing info
        
        # Extract data based on form type
        if filing['form'] == '10-K':
            extractor = Form10KExtractor(filing)
        else:
            # TODO: Add other form extractors
            return {'error': f'No extractor for form {filing["form"]}'}
        
        extracted_data = extractor.extract()
        
        # Generate signals
        signal = self.signal_generator.process_filing(extracted_data)
        
        # Mark as processed
        self.mark_filing_processed(filing['adsh'])
        
        return {
            'filing': filing,
            'extracted_data': extracted_data,
            'signal': signal
        }
    
    def mark_filing_processed(self, adsh: str):
        """Mark a filing as processed in the database"""
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        
        cur.execute("""
            UPDATE dispatch_queue 
            SET processed = 1 
            WHERE adsh = ?
        """, (adsh,))
        
        conn.commit()
        conn.close()
    
    def run_pipeline(self):
        """Run the complete pipeline: get filings → process → generate signals"""
        print("Starting SEC filing processing pipeline...")
        
        filings = self.get_pending_filings()
        print(f"Found {len(filings)} pending filings")
        
        for filing in filings:
            try:
                result = self.process_filing(filing)
                print(f"Processed {filing['adsh']} ({filing['form']})")
            except Exception as e:
                print(f"Error processing {filing['adsh']}: {e}")
        
        print("Pipeline completed!")
        return self.signal_generator.get_all_signals()
