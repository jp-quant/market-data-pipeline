"""CCXT-specific segment pipeline for processing NDJSON segments."""
import logging
from pathlib import Path
from typing import Optional, Dict, List

from storage.base import StorageBackend
from etl.readers.ndjson_reader import NDJSONReader
from etl.processors.raw_parser import RawParser
from etl.processors.ccxt.ticker_processor import CcxtTickerProcessor
from etl.processors.ccxt.trades_processor import CcxtTradesProcessor
from etl.processors.ccxt.orderbook_processor import CcxtOrderbookProcessor
from etl.writers.parquet_writer import ParquetWriter
from .pipeline import ETLPipeline

logger = logging.getLogger(__name__)


class CcxtSegmentPipeline:
    """
    CCXT-specific pipeline for processing NDJSON segments.
    """
    
    def __init__(
        self,
        storage: StorageBackend,
        output_base_path: str,
        channel_config: Optional[Dict[str, Dict]] = None,
    ):
        self.storage = storage
        self.source = "ccxt"
        self.output_base_path = output_base_path
        self.channel_config = channel_config or self._get_default_config()
        
        # Create channel-specific pipelines
        self.pipelines = self._create_pipelines()
        
        logger.info(
            f"[CcxtSegmentPipeline] Initialized, "
            f"storage={storage.backend_type}, "
            f"channels={list(self.pipelines.keys())}"
        )
    
    def _get_default_config(self) -> Dict[str, Dict]:
        """Get default configuration per channel."""
        return {
            "ticker": {
                "partition_cols": ["exchange", "symbol", "date"],
                "processor_options": {}
            },
            "trades": {
                "partition_cols": ["exchange", "symbol", "date"],
                "processor_options": {}
            },
            "orderbook": {
                "partition_cols": ["exchange", "symbol", "date"],
                "processor_options": {}
            }
        }
    
    def _create_pipelines(self) -> Dict[str, Dict]:
        """Create channel-specific pipelines."""
        pipelines = {}
        
        for channel, config in self.channel_config.items():
            processor_class = self._get_processor_class(channel)
            if not processor_class:
                logger.warning(f"No processor for channel: {channel}")
                continue
                
            processor = processor_class()
            
            # Create pipeline
            pipeline = ETLPipeline(
                reader=NDJSONReader(self.storage),
                processors=[
                    RawParser(source=self.source, channel=channel),
                    processor,
                ],
                writer=ParquetWriter(
                    storage=self.storage,
                    compression="snappy"
                )
            )
            
            # Store pipeline and its configuration
            pipelines[channel] = {
                "pipeline": pipeline,
                "output_path": f"{self.output_base_path}/{channel}",
                "partition_cols": config.get("partition_cols", ["exchange", "symbol", "date"])
            }
            
        return pipelines

    def _get_processor_class(self, channel: str):
        processors = {
            "ticker": CcxtTickerProcessor,
            "trades": CcxtTradesProcessor,
            "orderbook": CcxtOrderbookProcessor,
        }
        return processors.get(channel)

    def process_segment(self, segment_path: Path, channels: Optional[List[str]] = None):
        """Process a single segment file."""
        segment_path = Path(segment_path)
        logger.info(f"[CcxtSegmentPipeline] Processing segment: {segment_path.name}")
        
        results = {}
        
        target_channels = channels or self.pipelines.keys()
        
        for channel in target_channels:
            if channel not in self.pipelines:
                continue
                
            pipeline_config = self.pipelines[channel]
            pipeline = pipeline_config["pipeline"]
            output_path = pipeline_config["output_path"]
            partition_cols = pipeline_config["partition_cols"]
            
            try:
                # Run pipeline
                pipeline.execute(
                    input_path=str(segment_path),
                    output_path=output_path,
                    partition_cols=partition_cols
                )
                
                # Get stats
                stats = pipeline.writer.get_stats()
                results[channel] = stats
                logger.info(f"  Channel {channel}: {stats.get('records_written', 0)} records")
                
                # Reset stats for next run
                pipeline.reset_stats()
                
            except Exception as e:
                logger.error(f"  Channel {channel} failed: {e}")
                results[channel] = {"error": str(e)}
                
        return results
