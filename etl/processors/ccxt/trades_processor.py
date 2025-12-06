"""CCXT Trades Processor."""
from typing import Any, Dict, List, Union, Optional
from ..base_processor import BaseProcessor


class CcxtTradesProcessor(BaseProcessor):
    """
    Process CCXT trades data.
    """
    
    def process(self, data: Any) -> Any:
        """
        Process trades data.
        
        Args:
            data: Single record or list of records
            
        Returns:
            Processed record(s)
        """
        if isinstance(data, list):
            return [self._process_one(d) for d in data if d]
        return self._process_one(data)

    def _process_one(self, record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Process a single trade record."""
        if not record:
            return None
            
        try:
            # Add derived fields if needed
            # For now, just pass through
            
            self.stats["records_output"] += 1
            return record
            
        except Exception:
            self.stats["errors"] += 1
            return None
