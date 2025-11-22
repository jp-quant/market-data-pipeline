"""Databento WebSocket collector stub - for equities/ETFs/options."""
import asyncio
import logging
from typing import List, Optional

from .base_collector import BaseCollector


logger = logging.getLogger(__name__)


class DatabentoCollector(BaseCollector):
    """
    Databento WebSocket collector stub.
    
    TODO: Implement connection to Databento Live WebSocket API
    Reference: https://docs.databento.com/api-reference-live/
    
    Databento provides:
    - Real-time equities, ETFs, options
    - Level 1 (MBO), Level 2 (MBP), trades
    - Multiple exchanges and datasets
    """
    
    def __init__(
        self,
        log_writer,
        api_key: str,
        dataset: str,
        symbols: List[str],
        schema: str = "trades",
        **kwargs
    ):
        """
        Initialize Databento collector.
        
        Args:
            log_writer: LogWriter instance
            api_key: Databento API key
            dataset: Dataset identifier (e.g., 'XNAS.ITCH')
            symbols: List of symbols to subscribe to
            schema: Data schema (trades, mbp-1, mbp-10, mbo, etc.)
            **kwargs: Passed to BaseCollector
        """
        super().__init__(source_name="databento", log_writer=log_writer, **kwargs)
        
        self.api_key = api_key
        self.dataset = dataset
        self.symbols = symbols
        self.schema = schema
        
        logger.info(
            f"[DatabentoCollector] Initialized (STUB): dataset={dataset}, "
            f"symbols={symbols}, schema={schema}"
        )
    
    async def _collect(self):
        """
        TODO: Implement Databento WebSocket connection.
        
        Steps:
        1. Connect to wss://live.databento.com/ws
        2. Authenticate with API key
        3. Subscribe to dataset/schema/symbols
        4. Stream messages and push to log_writer
        """
        logger.warning(
            "[DatabentoCollector] STUB implementation - not collecting data"
        )
        
        # Placeholder: sleep until shutdown
        while not self._shutdown.is_set():
            await asyncio.sleep(1)
        
        logger.info("[DatabentoCollector] Stub collector stopped")
