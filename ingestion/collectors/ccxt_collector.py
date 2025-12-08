"""CCXT Pro Collector for streaming market data."""
import asyncio
import json
import logging
import ccxt.pro as ccxtpro
from typing import List, Optional, Dict, Any

from .base_collector import BaseCollector
from ..utils.time import utc_now

logger = logging.getLogger(__name__)


class CcxtCollector(BaseCollector):
    """
    CCXT Pro collector that streams market data via WebSocket.
    
    Supports multiple channels (methods) per exchange.
    """
    
    def __init__(
        self,
        log_writer,
        exchange_id: str,
        channels: Dict[str, List[str]],
        api_key: str = "",
        api_secret: str = "",
        password: str = "",
        options: Dict[str, Any] = None,
        **kwargs
    ):
        """
        Initialize CCXT collector.
        
        Args:
            log_writer: LogWriter instance
            exchange_id: CCXT exchange ID (e.g., 'binance', 'kraken')
            channels: Map of method name to symbols (e.g. {'watchTicker': ['BTC/USDT']})
            api_key: API Key (optional)
            api_secret: API Secret (optional)
            password: API Password (optional)
            options: Extra exchange options
        """
        super().__init__(
            source_name=f"ccxt_{exchange_id}",
            log_writer=log_writer,
            **kwargs
        )
        self.exchange_id = exchange_id
        self.channels = channels
        self.api_key = api_key
        self.api_secret = api_secret
        self.password = password
        self.options = options or {}
        
        self.exchange = None
        self._tasks = []

    async def _collect(self):
        """
        Main collection loop.
        Initializes exchange, spawns channel loops, and handles cleanup.
        """
        try:
            exchange_class = getattr(ccxtpro, self.exchange_id)
        except AttributeError:
            logger.error(f"Exchange {self.exchange_id} not found in ccxt.pro")
            return

        # Initialize exchange
        # enableRateLimit: True is recommended by CCXT to handle the initial REST handshake 
        # and internal throttling to avoid 429s. It generally does not affect WebSocket 
        # throughput once connected, but ensures connection stability.
        config = {
            'apiKey': self.api_key,
            'secret': self.api_secret,
            'password': self.password,
            'enableRateLimit': self.options.get('enableRateLimit', True),
            'options': self.options
        }
        self.exchange = exchange_class(config)
        logger.info(f"Initialized CCXT exchange {self.exchange_id}")

        loops = []
        
        for method, symbols in self.channels.items():
            if not hasattr(self.exchange, method):
                logger.warning(f"Exchange {self.exchange_id} does not support {method}")
                continue
                
            if symbols:
                # Create a loop for each symbol if the method requires it
                for symbol in symbols:
                    loops.append(self._symbol_loop(method, symbol))
            else:
                # Method without symbols (e.g. watchBalance)
                loops.append(self._method_loop(method))
        
        if not loops:
            logger.warning(f"No valid channels configured for {self.exchange_id}")
            return

        # Run all loops concurrently
        try:
            await asyncio.gather(*loops)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"Error in CCXT collector main loop for {self.exchange_id}: {e}")
        finally:
            # Close exchange connection
            if self.exchange:
                await self.exchange.close()
                self.exchange = None
            logger.info(f"Closed CCXT exchange {self.exchange_id}")

    async def _symbol_loop(self, method: str, symbol: str):
        """Loop for a specific symbol and method."""
        logger.info(f"Starting {self.exchange_id} {method} {symbol}")
        while not self._shutdown.is_set():
            try:
                # Call the watch method
                # e.g. await exchange.watchTicker(symbol)
                response = await getattr(self.exchange, method)(symbol)
                
                # Capture timestamp immediately after receipt
                capture_ts = utc_now()

                # Standardize and write
                msg = {
                    "type": method.replace("watch", "").lower(), # e.g. ticker, trades
                    "exchange": self.exchange_id,
                    "symbol": symbol,
                    "method": method,
                    "data": response,
                    "collected_at": self.exchange.milliseconds(),
                    "capture_ts": capture_ts.isoformat()
                }
                
                # Write to log writer (non-blocking to avoid stalling the websocket loop)
                try:
                    await self.log_writer.write(msg, block=False)
                except asyncio.QueueFull:
                    logger.warning(f"[{self.source_name}] Queue full, dropping message")
                
            except Exception as e:
                if self._shutdown.is_set():
                    break
                logger.error(f"Error in {self.exchange_id} {method} {symbol}: {e}")
                # Simple backoff
                await asyncio.sleep(self.reconnect_delay)

    async def _method_loop(self, method: str):
        """Loop for a method without symbol arguments."""
        logger.info(f"Starting {self.exchange_id} {method}")
        while not self._shutdown.is_set():
            try:
                response = await getattr(self.exchange, method)()
                
                # Capture timestamp immediately after receipt
                capture_ts = utc_now()
                
                msg = {
                    "type": method.replace("watch", "").lower(),
                    "exchange": self.exchange_id,
                    "method": method,
                    "data": response,
                    "collected_at": self.exchange.milliseconds(),
                    "capture_ts": capture_ts.isoformat()
                }
                
                # Write to log writer (non-blocking)
                try:
                    await self.log_writer.write(msg, block=False)
                except asyncio.QueueFull:
                    logger.warning(f"[{self.source_name}] Queue full, dropping message")
                
            except Exception as e:
                if self._shutdown.is_set():
                    break
                logger.error(f"Error in {self.exchange_id} {method}: {e}")
                await asyncio.sleep(self.reconnect_delay)
