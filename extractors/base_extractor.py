"""
Base extractor class for SEC filings
All form-specific extractors should inherit from this
"""

from abc import ABC, abstractmethod
from typing import Dict, Any


class BaseExtractor(ABC):
    """Base class for extracting data from SEC filings"""
    
    def __init__(self, filing_data: Dict[str, Any]):
        self.filing_data = filing_data
    
    @abstractmethod
    def extract(self) -> Dict[str, Any]:
        """Extract relevant data from the filing"""
        pass
    
    def get_form_type(self) -> str:
        """Get the form type from filing data"""
        return self.filing_data.get('form', '')
    
    def get_cik(self) -> str:
        """Get the CIK from filing data"""
        return self.filing_data.get('cik', '')
