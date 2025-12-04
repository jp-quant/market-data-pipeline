"""S3-compatible log writer for ingestion layer.

Writes NDJSON segments directly to S3 with same semantics as LogWriter:
- Batched writes
- Size-based segment rotation
- active/ready segregation (using S3 prefixes)
- Durability guarantees
"""
import asyncio
import logging
from datetime import datetime
from typing import Optional, Dict, Any
from collections import deque
import io

from ..utils.time import utc_now
from ..utils.serialization import to_ndjson
from storage.s3.uploader import S3StorageManager


logger = logging.getLogger(__name__)


class S3LogWriter:
    """
    S3-compatible batched log writer with segment rotation.
    
    Same design as LogWriter but writes to S3 instead of local disk.
    
    S3 structure:
        s3://bucket/prefix/
            raw/
                active/coinbase/     # ONE open segment being written (in memory)
                ready/coinbase/      # Closed segments ready for ETL
    """
    
    def __init__(
        self,
        s3_manager: S3StorageManager,
        source_name: str,
        batch_size: int = 100,
        flush_interval_seconds: float = 5.0,
        queue_maxsize: int = 10000,
        segment_max_mb: int = 100,
    ):
        """
        Initialize S3 log writer.
        
        Args:
            s3_manager: S3StorageManager instance
            source_name: Data source identifier (e.g., "coinbase", "databento")
            batch_size: Number of records to batch before writing
            flush_interval_seconds: Maximum time between flushes
            queue_maxsize: Maximum queue size
            segment_max_mb: Max segment size in MB before rotation
        """
        self.s3 = s3_manager
        self.source_name = source_name
        self.batch_size = batch_size
        self.flush_interval = flush_interval_seconds
        self.segment_max_bytes = segment_max_mb * 1024 * 1024
        
        # Bounded queue
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=queue_maxsize)
        
        # In-memory buffer
        self.buffer: deque = deque()
        self.current_segment_buffer = io.BytesIO()  # In-memory segment buffer
        
        # Segment management
        self.current_date_hour: str = ""
        self.current_hour_counter: int = 0
        self.current_segment_key: Optional[str] = None
        self.current_segment_size: int = 0
        
        # S3 prefixes
        self.active_prefix = f"raw/active/{self.source_name}"
        self.ready_prefix = f"raw/ready/{self.source_name}"
        
        # Statistics
        self.stats = {
            "messages_received": 0,
            "messages_written": 0,
            "flushes": 0,
            "rotations": 0,
            "queue_full_events": 0,
            "errors": 0,
            "s3_uploads": 0,
        }
        
        # Writer task
        self._writer_task: Optional[asyncio.Task] = None
        self._shutdown = asyncio.Event()
        
        logger.info(
            f"[S3LogWriter] Initialized: source={source_name}, "
            f"s3://{s3_manager.bucket}/{s3_manager.prefix}, "
            f"batch_size={batch_size}, segment_max_mb={segment_max_mb}"
        )
    
    async def start(self):
        """Start the writer task and initialize first segment."""
        if self._writer_task is not None:
            logger.warning("[S3LogWriter] Writer task already running")
            return
        
        self._shutdown.clear()
        
        # Initialize first segment
        await self._initialize_first_segment()
        
        self._writer_task = asyncio.create_task(self._writer_loop())
        logger.info(f"[S3LogWriter] Writer task started for source={self.source_name}")
    
    async def stop(self):
        """Stop the writer task and flush remaining data."""
        if self._writer_task is None:
            return
        
        logger.info(f"[S3LogWriter] Stopping writer for source={self.source_name}")
        self._shutdown.set()
        
        # Wait for writer task to finish
        try:
            await asyncio.wait_for(self._writer_task, timeout=10.0)
        except asyncio.TimeoutError:
            logger.error("[S3LogWriter] Writer task did not stop within timeout")
            self._writer_task.cancel()
        
        # Final flush
        await self._flush()
        
        # Close current segment and move to ready
        if self.current_segment_key:
            await self._close_and_move_to_ready()
        
        logger.info(f"[S3LogWriter] Stopped. Stats: {self.stats}")
    
    async def write(self, record: Dict[str, Any], block: bool = True):
        """
        Write a record to the queue.
        
        Args:
            record: Dictionary record to write
            block: If True, blocks when queue is full
        """
        self.stats["messages_received"] += 1
        
        try:
            if block:
                await self.queue.put(record)
            else:
                self.queue.put_nowait(record)
        except asyncio.QueueFull:
            self.stats["queue_full_events"] += 1
            logger.warning(
                f"[S3LogWriter] Queue full (size={self.queue.qsize()}) - "
                "backpressure active"
            )
            raise
    
    async def _writer_loop(self):
        """Main writer loop that batches and flushes records."""
        last_flush_time = asyncio.get_event_loop().time()
        
        while not self._shutdown.is_set():
            try:
                # Wait for record or timeout
                try:
                    record = await asyncio.wait_for(
                        self.queue.get(),
                        timeout=self.flush_interval
                    )
                    self.buffer.append(record)
                except asyncio.TimeoutError:
                    pass
                
                current_time = asyncio.get_event_loop().time()
                time_since_flush = current_time - last_flush_time
                
                # Flush conditions
                should_flush = (
                    len(self.buffer) >= self.batch_size or
                    (len(self.buffer) > 0 and time_since_flush >= self.flush_interval)
                )
                
                if should_flush:
                    await self._flush()
                    last_flush_time = current_time
                    
                    # Check if rotation needed after flush
                    if self.current_segment_size >= self.segment_max_bytes:
                        await self._rotate_segment()
            
            except Exception as e:
                self.stats["errors"] += 1
                logger.error(f"[S3LogWriter] Error in writer loop: {e}", exc_info=True)
                await asyncio.sleep(1)
        
        # Final flush on shutdown
        if self.buffer:
            await self._flush()
    
    async def _flush(self):
        """Flush buffer to in-memory segment."""
        if not self.buffer:
            return
        
        try:
            records_to_write = list(self.buffer)
            self.buffer.clear()
            
            # Write to in-memory buffer
            bytes_written = 0
            for record in records_to_write:
                line = to_ndjson(record)
                line_bytes = line.encode('utf-8')
                self.current_segment_buffer.write(line_bytes)
                bytes_written += len(line_bytes)
            
            self.current_segment_size += bytes_written
            self.stats["messages_written"] += len(records_to_write)
            self.stats["flushes"] += 1
            
            logger.debug(
                f"[S3LogWriter] Flushed {len(records_to_write)} records "
                f"({bytes_written} bytes) to in-memory buffer, "
                f"total size: {self.current_segment_size / 1024 / 1024:.2f} MB"
            )
        
        except Exception as e:
            self.stats["errors"] += 1
            logger.error(f"[S3LogWriter] Error flushing buffer: {e}", exc_info=True)
    
    async def _initialize_first_segment(self):
        """Initialize the first segment."""
        now = utc_now()
        self.current_date_hour = now.strftime("%Y%m%dT%H")
        
        # Find existing segments to determine next counter
        existing_active = self.s3.list_objects(
            prefix=f"{self.active_prefix}/segment_{self.current_date_hour}_",
            suffix=".ndjson"
        )
        existing_ready = self.s3.list_objects(
            prefix=f"{self.ready_prefix}/segment_{self.current_date_hour}_",
            suffix=".ndjson"
        )
        
        max_counter = 0
        for obj in existing_active + existing_ready:
            try:
                # Extract counter from key
                parts = obj["Key"].split("_")
                if len(parts) >= 3:
                    counter = int(parts[2].split(".")[0])
                    max_counter = max(max_counter, counter)
            except (ValueError, IndexError):
                continue
        
        self.current_hour_counter = max_counter + 1
        
        # Create segment key
        filename = f"segment_{self.current_date_hour}_{self.current_hour_counter:05d}.ndjson"
        self.current_segment_key = f"{self.active_prefix}/{filename}"
        
        # Reset in-memory buffer
        self.current_segment_buffer = io.BytesIO()
        self.current_segment_size = 0
        
        logger.info(
            f"[S3LogWriter] Initialized segment {self.current_date_hour}_{self.current_hour_counter:05d}: "
            f"{self.current_segment_key}"
        )
    
    async def _rotate_segment(self):
        """Rotate the current segment."""
        if not self.current_segment_key:
            logger.warning("[S3LogWriter] No active segment to rotate")
            return
        
        old_key = self.current_segment_key
        old_date_hour = self.current_date_hour
        old_counter = self.current_hour_counter
        old_size_mb = self.current_segment_size / 1024 / 1024
        
        # Upload current segment buffer to S3 (active)
        try:
            segment_data = self.current_segment_buffer.getvalue()
            await asyncio.get_event_loop().run_in_executor(
                None,
                self.s3.write_bytes,
                segment_data,
                old_key,
                None,  # metadata
                "application/x-ndjson"  # content_type
            )
            self.stats["s3_uploads"] += 1
            
            logger.info(
                f"[S3LogWriter] Uploaded segment to S3: {old_key} "
                f"({old_size_mb:.2f} MB)"
            )
        except Exception as e:
            logger.error(f"[S3LogWriter] Failed to upload segment: {e}")
            raise
        
        # Move from active to ready (copy + delete)
        try:
            filename = old_key.split("/")[-1]
            ready_key = f"{self.ready_prefix}/{filename}"
            
            # Copy to ready
            await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: self.s3.s3_client.copy_object(
                    Bucket=self.s3.bucket,
                    CopySource={"Bucket": self.s3.bucket, "Key": self.s3.get_full_key(old_key)},
                    Key=self.s3.get_full_key(ready_key)
                )
            )
            
            # Delete from active
            await asyncio.get_event_loop().run_in_executor(
                None,
                self.s3.delete,
                old_key
            )
            
            logger.info(
                f"[S3LogWriter] Rotated segment {old_date_hour}_{old_counter:05d} → ready/"
            )
        except Exception as e:
            logger.error(f"[S3LogWriter] Failed to move segment to ready/: {e}")
            raise
        
        # Get current date-hour
        now = utc_now()
        new_date_hour = now.strftime("%Y%m%dT%H")
        
        # Check if hour changed
        if new_date_hour != self.current_date_hour:
            self.current_date_hour = new_date_hour
            self.current_hour_counter = 1
            logger.info(
                f"[S3LogWriter] Hour changed: {old_date_hour} → {new_date_hour}, "
                "counter reset to 1"
            )
        else:
            self.current_hour_counter += 1
        
        # Create new segment
        filename = f"segment_{self.current_date_hour}_{self.current_hour_counter:05d}.ndjson"
        self.current_segment_key = f"{self.active_prefix}/{filename}"
        
        # Reset in-memory buffer
        self.current_segment_buffer = io.BytesIO()
        self.current_segment_size = 0
        self.stats["rotations"] += 1
        
        logger.info(
            f"[S3LogWriter] New segment {self.current_date_hour}_{self.current_hour_counter:05d}: "
            f"{self.current_segment_key}"
        )
    
    async def _close_and_move_to_ready(self):
        """Close current segment and move to ready on shutdown."""
        if not self.current_segment_key:
            return
        
        # Upload current segment buffer to S3 (active)
        try:
            segment_data = self.current_segment_buffer.getvalue()
            if len(segment_data) > 0:  # Only upload if there's data
                await asyncio.get_event_loop().run_in_executor(
                    None,
                    self.s3.write_bytes,
                    segment_data,
                    self.current_segment_key,
                    None,
                    "application/x-ndjson"
                )
                self.stats["s3_uploads"] += 1
                
                # Move to ready
                filename = self.current_segment_key.split("/")[-1]
                ready_key = f"{self.ready_prefix}/{filename}"
                
                await asyncio.get_event_loop().run_in_executor(
                    None,
                    lambda: self.s3.s3_client.copy_object(
                        Bucket=self.s3.bucket,
                        CopySource={"Bucket": self.s3.bucket, "Key": self.s3.get_full_key(self.current_segment_key)},
                        Key=self.s3.get_full_key(ready_key)
                    )
                )
                
                await asyncio.get_event_loop().run_in_executor(
                    None,
                    self.s3.delete,
                    self.current_segment_key
                )
                
                logger.info(f"[S3LogWriter] Final segment moved to ready/: {ready_key}")
        except Exception as e:
            logger.error(f"[S3LogWriter] Failed to finalize segment: {e}")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get current statistics."""
        return {
            **self.stats,
            "queue_size": self.queue.qsize(),
            "buffer_size": len(self.buffer),
            "source_name": self.source_name,
            "current_date_hour": self.current_date_hour,
            "current_hour_counter": self.current_hour_counter,
            "current_segment_size_mb": round(self.current_segment_size / 1024 / 1024, 2),
            "current_segment_key": self.current_segment_key,
        }
