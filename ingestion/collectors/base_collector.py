"""Base collector interface."""
import asyncio
import logging
from abc import ABC, abstractmethod
from typing import Optional

from ..writers.log_writer import LogWriter


logger = logging.getLogger(__name__)


class BaseCollector(ABC):
    """
    Abstract base class for all market data collectors.
    
    Collectors are I/O-only:
    - Connect to websocket
    - Capture raw messages
    - Push to LogWriter queue
    - Zero processing/transformation
    """
    
    def __init__(
        self,
        source_name: str,
        log_writer: LogWriter,
        auto_reconnect: bool = True,
        max_reconnect_attempts: int = 10,
        reconnect_delay: float = 5.0,
    ):
        """
        Initialize base collector.
        
        Args:
            source_name: Identifier for this data source
            log_writer: LogWriter instance to push messages to
            auto_reconnect: Whether to automatically reconnect on disconnect
            max_reconnect_attempts: Maximum reconnection attempts
            reconnect_delay: Seconds to wait between reconnect attempts
        """
        self.source_name = source_name
        self.log_writer = log_writer
        self.auto_reconnect = auto_reconnect
        self.max_reconnect_attempts = max_reconnect_attempts
        self.reconnect_delay = reconnect_delay
        
        self._collector_task: Optional[asyncio.Task] = None
        self._shutdown = asyncio.Event()
        
        self.stats = {
            "messages_received": 0,
            "connection_attempts": 0,
            "disconnections": 0,
            "errors": 0,
        }
        
        logger.info(
            f"[{self.source_name}] Collector initialized: "
            f"auto_reconnect={auto_reconnect}, max_attempts={max_reconnect_attempts}"
        )
    
    async def start(self):
        """Start the collector task."""
        if self._collector_task is not None:
            logger.warning(f"[{self.source_name}] Collector already running")
            return
        
        self._shutdown.clear()
        self._collector_task = asyncio.create_task(self._run())
        logger.info(f"[{self.source_name}] Collector started")
    
    async def stop(self):
        """Stop the collector task."""
        if self._collector_task is None:
            return
        
        logger.info(f"[{self.source_name}] Stopping collector")
        self._shutdown.set()
        
        try:
            await asyncio.wait_for(self._collector_task, timeout=10.0)
        except asyncio.TimeoutError:
            logger.error(f"[{self.source_name}] Collector did not stop within timeout")
            self._collector_task.cancel()
        
        logger.info(f"[{self.source_name}] Collector stopped. Stats: {self.stats}")
    
    async def _run(self):
        """Main collector loop with reconnection logic."""
        attempt = 0
        
        while not self._shutdown.is_set():
            try:
                self.stats["connection_attempts"] += 1
                attempt += 1
                
                logger.info(
                    f"[{self.source_name}] Connection attempt {attempt}/"
                    f"{self.max_reconnect_attempts if self.auto_reconnect else 1}"
                )
                
                # Call subclass-specific collection logic
                await self._collect()
                
                # If we get here, connection closed normally
                self.stats["disconnections"] += 1
                logger.info(f"[{self.source_name}] Connection closed")
                
                if not self.auto_reconnect:
                    break
                
                if attempt >= self.max_reconnect_attempts:
                    logger.error(
                        f"[{self.source_name}] Max reconnection attempts reached"
                    )
                    break
                
                # Wait before reconnecting
                logger.info(
                    f"[{self.source_name}] Reconnecting in {self.reconnect_delay}s..."
                )
                await asyncio.sleep(self.reconnect_delay)
            
            except asyncio.CancelledError:
                logger.info(f"[{self.source_name}] Collector task cancelled")
                break
            
            except Exception as e:
                self.stats["errors"] += 1
                logger.error(
                    f"[{self.source_name}] Error in collector: {e}",
                    exc_info=True
                )
                
                if not self.auto_reconnect:
                    break
                
                if attempt >= self.max_reconnect_attempts:
                    logger.error(
                        f"[{self.source_name}] Max reconnection attempts reached after error"
                    )
                    break
                
                await asyncio.sleep(self.reconnect_delay)
    
    @abstractmethod
    async def _collect(self):
        """
        Subclass implements actual websocket connection and message handling.
        
        This method should:
        1. Connect to websocket
        2. Subscribe to channels
        3. Read messages in a loop
        4. Push raw messages to log_writer
        5. Handle disconnections gracefully
        """
        pass
    
    def get_stats(self):
        """Get collector statistics."""
        return self.stats.copy()
