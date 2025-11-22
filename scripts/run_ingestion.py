"""Run FluxForge ingestion pipeline."""
import asyncio
import logging
import signal
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from config import load_config
from ingestion.collectors.coinbase_ws import CoinbaseCollector
from ingestion.collectors.databento_ws import DatabentoCollector
from ingestion.collectors.ibkr_ws import IBKRCollector
from ingestion.writers.log_writer import LogWriter


logger = logging.getLogger(__name__)


class IngestionPipeline:
    """Orchestrates ingestion from multiple data sources."""
    
    def __init__(self, config_path: str = None):
        """
        Initialize ingestion pipeline.
        
        Args:
            config_path: Path to config file (optional)
        """
        self.config = load_config(config_path)
        
        # Configure logging
        logging.basicConfig(
            level=getattr(logging, self.config.log_level),
            format=self.config.log_format
        )
        
        # Collectors and writers
        self.writers = {}
        self.collectors = []
        
        self._shutdown_event = asyncio.Event()
    
    async def start(self):
        """Start all configured collectors."""
        logger.info("=" * 80)
        logger.info("FluxForge Ingestion Pipeline Starting")
        logger.info("=" * 80)
        
        # Start Coinbase if configured
        if self.config.coinbase and self.config.coinbase.api_key:
            logger.info("Initializing Coinbase collector...")
            
            # Create log writer for Coinbase
            coinbase_writer = LogWriter(
                output_dir=self.config.ingestion.output_dir,
                source_name="coinbase",
                batch_size=self.config.ingestion.batch_size,
                flush_interval_seconds=self.config.ingestion.flush_interval_seconds,
                queue_maxsize=self.config.ingestion.queue_maxsize,
                enable_fsync=self.config.ingestion.enable_fsync,
                segment_max_mb=self.config.ingestion.segment_max_mb,
            )
            await coinbase_writer.start()
            self.writers["coinbase"] = coinbase_writer
            
            # Create collector
            coinbase_collector = CoinbaseCollector(
                log_writer=coinbase_writer,
                api_key=self.config.coinbase.api_key,
                api_secret=self.config.coinbase.api_secret,
                product_ids=self.config.coinbase.product_ids,
                channels=self.config.coinbase.channels,
                ws_url=self.config.coinbase.ws_url,
                level2_batch_size=getattr(self.config.coinbase, 'level2_batch_size', 10),
                auto_reconnect=self.config.ingestion.auto_reconnect,
                max_reconnect_attempts=self.config.ingestion.max_reconnect_attempts,
                reconnect_delay=self.config.ingestion.reconnect_delay,
            )
            await coinbase_collector.start()
            self.collectors.append(coinbase_collector)
            
            logger.info("✓ Coinbase collector started")
        
        # # Start Databento if configured (stub for now)
        # if self.config.databento and self.config.databento.api_key:
        #     logger.info("Initializing Databento collector (STUB)...")
            
            databento_writer = LogWriter(
                output_dir=self.config.ingestion.output_dir,
                source_name="databento",
                batch_size=self.config.ingestion.batch_size,
                flush_interval_seconds=self.config.ingestion.flush_interval_seconds,
                queue_maxsize=self.config.ingestion.queue_maxsize,
                enable_fsync=self.config.ingestion.enable_fsync,
                segment_max_mb=self.config.ingestion.segment_max_mb,
            )
        #     await databento_writer.start()
        #     self.writers["databento"] = databento_writer
            
        #     databento_collector = DatabentoCollector(
        #         log_writer=databento_writer,
        #         api_key=self.config.databento.api_key,
        #         dataset=self.config.databento.dataset,
        #         symbols=self.config.databento.symbols,
        #         schema=self.config.databento.schema,
        #     )
        #     await databento_collector.start()
        #     self.collectors.append(databento_collector)
            
        #     logger.info("✓ Databento collector started (STUB)")
        
        # # Start IBKR if configured (stub for now)
        # if self.config.ibkr and self.config.ibkr.account_id:
        #     logger.info("Initializing IBKR collector (STUB)...")
            
            ibkr_writer = LogWriter(
                output_dir=self.config.ingestion.output_dir,
                source_name="ibkr",
                batch_size=self.config.ingestion.batch_size,
                flush_interval_seconds=self.config.ingestion.flush_interval_seconds,
                queue_maxsize=self.config.ingestion.queue_maxsize,
                enable_fsync=self.config.ingestion.enable_fsync,
                segment_max_mb=self.config.ingestion.segment_max_mb,
            )
        #     await ibkr_writer.start()
        #     self.writers["ibkr"] = ibkr_writer
            
        #     ibkr_collector = IBKRCollector(
        #         log_writer=ibkr_writer,
        #         gateway_url=self.config.ibkr.gateway_url,
        #         account_id=self.config.ibkr.account_id,
        #         contracts=self.config.ibkr.contracts,
        #     )
        #     await ibkr_collector.start()
        #     self.collectors.append(ibkr_collector)
            
        #     logger.info("✓ IBKR collector started (STUB)")
        
        logger.info("=" * 80)
        logger.info(f"Ingestion pipeline running with {len(self.collectors)} collector(s)")
        logger.info("Press Ctrl+C to stop")
        logger.info("=" * 80)
        
        # Wait for shutdown signal
        await self._shutdown_event.wait()
    
    async def stop(self):
        """Stop all collectors and writers."""
        logger.info("Shutting down ingestion pipeline...")
        
        # Stop collectors
        for collector in self.collectors:
            await collector.stop()
        
        # Stop writers
        for writer in self.writers.values():
            await writer.stop()
        
        logger.info("Ingestion pipeline stopped")
    
    async def print_stats(self):
        """Print statistics for all collectors and writers."""
        logger.info("=" * 80)
        logger.info("Pipeline Statistics")
        logger.info("=" * 80)
        
        for collector in self.collectors:
            stats = collector.get_stats()
            logger.info(f"{collector.source_name}: {stats}")
        
        for name, writer in self.writers.items():
            stats = writer.get_stats()
            logger.info(f"{name} writer: {stats}")
        
        logger.info("=" * 80)


async def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="FluxForge Ingestion Pipeline")
    parser.add_argument(
        "--config",
        type=str,
        help="Path to config file (default: config/config.yaml)"
    )
    parser.add_argument(
        "--stats-interval",
        type=int,
        default=60,
        help="Print stats every N seconds (default: 60)"
    )
    
    args = parser.parse_args()
    
    # Create pipeline
    pipeline = IngestionPipeline(config_path=args.config)
    
    # Setup signal handlers
    def signal_handler(sig, frame):
        logger.info("Received shutdown signal")
        pipeline._shutdown_event.set()
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Start stats printer
    async def stats_printer():
        while not pipeline._shutdown_event.is_set():
            await asyncio.sleep(args.stats_interval)
            if not pipeline._shutdown_event.is_set():
                await pipeline.print_stats()
    
    stats_task = asyncio.create_task(stats_printer())
    
    try:
        # Start pipeline
        await pipeline.start()
    finally:
        # Cleanup
        stats_task.cancel()
        await pipeline.stop()


if __name__ == "__main__":
    asyncio.run(main())
