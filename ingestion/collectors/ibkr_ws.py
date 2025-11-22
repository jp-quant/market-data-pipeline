"""Interactive Brokers WebSocket collector stub."""
import asyncio
import logging
from typing import List, Optional

from .base_collector import BaseCollector


logger = logging.getLogger(__name__)


class IBKRCollector(BaseCollector):
    """
    Interactive Brokers Pro WebSocket collector stub.
    
    TODO: Implement connection to IBKR Client Portal API
    Reference: https://www.interactivebrokers.com/api/doc.html
    
    IBKR provides:
    - Real-time stocks, options, futures, forex
    - Market depth
    - Account data
    
    Note: IBKR requires Client Portal Gateway running locally
    """
    
    def __init__(
        self,
        log_writer,
        gateway_url: str = "https://localhost:5000",
        account_id: Optional[str] = None,
        contracts: Optional[List[dict]] = None,
        **kwargs
    ):
        """
        Initialize IBKR collector.
        
        Args:
            log_writer: LogWriter instance
            gateway_url: URL of IBKR Client Portal Gateway
            account_id: IBKR account ID
            contracts: List of contract definitions to subscribe to
            **kwargs: Passed to BaseCollector
        """
        super().__init__(source_name="ibkr", log_writer=log_writer, **kwargs)
        
        self.gateway_url = gateway_url
        self.account_id = account_id
        self.contracts = contracts or []
        
        logger.info(
            f"[IBKRCollector] Initialized (STUB): gateway={gateway_url}, "
            f"account={account_id}"
        )
    
    async def _collect(self):
        """
        TODO: Implement IBKR Client Portal API connection.
        
        Steps:
        1. Authenticate with Client Portal Gateway
        2. Subscribe to market data for contracts
        3. Poll or stream updates
        4. Push to log_writer
        
        Note: IBKR uses REST + WebSocket hybrid approach
        """
        logger.warning(
            "[IBKRCollector] STUB implementation - not collecting data"
        )
        
        # Placeholder: sleep until shutdown
        while not self._shutdown.is_set():
            await asyncio.sleep(1)
        
        logger.info("[IBKRCollector] Stub collector stopped")
