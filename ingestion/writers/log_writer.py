"""
Unified log writer for durable, append-only NDJSON storage with size-based rotation.

Works with any StorageBackend (local filesystem or S3).
"""
import asyncio
import io
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any, BinaryIO
from collections import deque

from storage.base import StorageBackend
from ingestion.utils.time import utc_now
from ingestion.utils.serialization import to_ndjson

logger = logging.getLogger(__name__)


class LogWriter:
    """
    Batched log writer with size-based segment rotation.
    
    Design:
    - Bounded queue with backpressure
    - Batch writes to reduce I/O overhead
    - Size-based segment rotation (prevents unbounded growth)
    - Durability guarantees (fsync on flush for local, immediate S3 upload)
    - Active/ready directory segregation (ETL never touches active files)
    - Works with any StorageBackend (local or S3)
    
    Directory structure (relative to storage root):
        {active_path}/segment_*.ndjson     # Open segment being written
        {ready_path}/segment_*.ndjson      # Closed segments ready for ETL
    
    Example paths:
        Local: F:/raw/active/coinbase/segment_20251203T14_00001.ndjson
        S3:    s3://bucket/raw/active/coinbase/segment_20251203T14_00001.ndjson
    """
    
    def __init__(
        self,
        storage: StorageBackend,
        active_path: str,
        ready_path: str,
        source_name: str,
        batch_size: int = 100,
        flush_interval_seconds: float = 5.0,
        queue_maxsize: int = 10000,
        enable_fsync: bool = True,
        segment_max_mb: int = 100,
    ):
        """
        Initialize log writer.
        
        Args:
            storage: Storage backend instance
            active_path: Path for actively writing segments (relative to storage root)
            ready_path: Path for ready segments (relative to storage root)
            source_name: Data source identifier (e.g., "coinbase")
            batch_size: Records to batch before writing
            flush_interval_seconds: Maximum time between flushes
            queue_maxsize: Maximum queue size (backpressure when full)
            enable_fsync: Call fsync after flush (local only)
            segment_max_mb: Max segment size in MB before rotation
        """
        self.storage = storage
        self.active_path = active_path
        self.ready_path = ready_path
        self.source_name = source_name
        self.batch_size = batch_size
        self.flush_interval = flush_interval_seconds
        self.enable_fsync = enable_fsync
        self.segment_max_bytes = segment_max_mb * 1024 * 1024
        
        # Bounded queue for backpressure
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=queue_maxsize)
        
        # In-memory buffer for batching
        self.buffer: deque = deque()
        
        # Segment management
        self.current_date_hour: str = ""  # Format: YYYYMMDDTHH
        self.current_hour_counter: int = 0
        self.current_segment_name: Optional[str] = None
        self.current_segment_buffer: Optional[io.BytesIO] = None
        self.current_segment_size: int = 0
        
        # For local storage only - file handle
        self.current_segment_handle: Optional[BinaryIO] = None
        
        # Statistics
        self.stats = {
            "messages_received": 0,
            "messages_written": 0,
            "flushes": 0,
            "rotations": 0,
            "queue_full_events": 0,
            "errors": 0,
        }
        
        # Writer task
        self._writer_task: Optional[asyncio.Task] = None
        self._shutdown = asyncio.Event()
        
        # Ensure directories exist (local only, no-op for S3)
        self.storage.mkdir(self.active_path)
        self.storage.mkdir(self.ready_path)
        
        logger.info(
            f"[LogWriter] Initialized: source={source_name}, storage={storage.backend_type}, "
            f"batch_size={batch_size}, segment_max_mb={segment_max_mb}"
        )
        logger.info(f"[LogWriter] Active: {active_path}")
        logger.info(f"[LogWriter] Ready:  {ready_path}")
    
    async def start(self):
        """Start the writer background task."""
        if self._writer_task is not None:
            logger.warning("[LogWriter] Already started")
            return
        
        # Initialize first segment
        await asyncio.get_event_loop().run_in_executor(
            None,
            self._initialize_first_segment
        )
        
        # Start writer loop
        self._writer_task = asyncio.create_task(self._writer_loop())
        logger.info("[LogWriter] Started")
    
    async def stop(self):
        """Stop the writer and flush pending data."""
        if self._writer_task is None:
            logger.warning("[LogWriter] Not started")
            return
        
        logger.info("[LogWriter] Stopping...")
        self._shutdown.set()
        
        # Wait for writer task with timeout
        try:
            await asyncio.wait_for(self._writer_task, timeout=30.0)
        except asyncio.TimeoutError:
            logger.error("[LogWriter] Writer task did not stop gracefully")
            self._writer_task.cancel()
        
        # Close current segment and move to ready
        await asyncio.get_event_loop().run_in_executor(
            None,
            self._close_and_move_to_ready
        )
        
        logger.info(
            f"[LogWriter] Stopped: {self.stats['messages_written']} messages written, "
            f"{self.stats['rotations']} rotations"
        )
    
    async def write(self, record: Dict[str, Any], block: bool = True):
        """
        Write a record to the queue.
        
        Args:
            record: Dictionary to write
            block: If True, blocks when queue is full (backpressure).
                   If False, raises QueueFull immediately.
        """
        try:
            if block:
                await self.queue.put(record)
            else:
                self.queue.put_nowait(record)
            
            self.stats["messages_received"] += 1
        
        except asyncio.QueueFull:
            self.stats["queue_full_events"] += 1
            raise
    
    async def _writer_loop(self):
        """Background task that batches and writes records."""
        last_flush_time = asyncio.get_event_loop().time()
        
        while not self._shutdown.is_set() or not self.queue.empty():
            try:
                # Drain queue into buffer (up to batch_size)
                while len(self.buffer) < self.batch_size:
                    try:
                        record = await asyncio.wait_for(
                            self.queue.get(),
                            timeout=0.1
                        )
                        self.buffer.append(record)
                    except asyncio.TimeoutError:
                        break
                
                # Check if flush needed
                current_time = asyncio.get_event_loop().time()
                time_since_flush = current_time - last_flush_time
                
                should_flush = (
                    len(self.buffer) >= self.batch_size
                    or (self.buffer and time_since_flush >= self.flush_interval)
                )
                
                if should_flush:
                    await self._flush()
                    last_flush_time = current_time
                    
                    # Check if rotation needed
                    if self.current_segment_size >= self.segment_max_bytes:
                        await asyncio.get_event_loop().run_in_executor(
                            None,
                            self._rotate_segment
                        )
            
            except Exception as e:
                self.stats["errors"] += 1
                logger.error(f"[LogWriter] Error in writer loop: {e}", exc_info=True)
                await asyncio.sleep(1)
        
        # Final flush on shutdown
        if self.buffer:
            await self._flush()
    
    async def _flush(self):
        """Flush buffer to storage."""
        if not self.buffer:
            return
        
        try:
            records_to_write = list(self.buffer)
            self.buffer.clear()
            
            # Use executor to avoid blocking event loop
            bytes_written = await asyncio.get_event_loop().run_in_executor(
                None,
                self._write_to_segment,
                records_to_write
            )
            
            self.current_segment_size += bytes_written
            self.stats["messages_written"] += len(records_to_write)
            self.stats["flushes"] += 1
            
            logger.debug(
                f"[LogWriter] Flushed {len(records_to_write)} records "
                f"({bytes_written} bytes), total: {self.current_segment_size / 1024 / 1024:.2f} MB"
            )
        
        except Exception as e:
            self.stats["errors"] += 1
            logger.error(f"[LogWriter] Error flushing: {e}", exc_info=True)
    
    def _initialize_first_segment(self):
        """Initialize first segment (synchronous, runs in executor)."""
        now = utc_now()
        self.current_date_hour = now.strftime("%Y%m%dT%H")
        
        # Find existing segments to determine next counter
        existing_segments = []
        
        # List from active path
        active_files = self.storage.list_files(
            path=self.active_path,
            pattern=f"segment_{self.current_date_hour}_*.ndjson"
        )
        existing_segments.extend([f["path"] for f in active_files])
        
        # List from ready path
        ready_files = self.storage.list_files(
            path=self.ready_path,
            pattern=f"segment_{self.current_date_hour}_*.ndjson"
        )
        existing_segments.extend([f["path"] for f in ready_files])
        
        # Extract max counter
        max_counter = 0
        for seg_path in existing_segments:
            try:
                # Extract filename from path
                filename = seg_path.split("/")[-1] if "/" in seg_path else Path(seg_path).name
                parts = filename.replace(".ndjson", "").split('_')
                if len(parts) >= 3:
                    counter = int(parts[2])
                    max_counter = max(max_counter, counter)
            except (ValueError, IndexError):
                continue
        
        self.current_hour_counter = max_counter + 1
        
        # Create segment filename
        self.current_segment_name = (
            f"segment_{self.current_date_hour}_{self.current_hour_counter:05d}.ndjson"
        )
        
        # Initialize based on storage type
        if self.storage.backend_type == "local":
            # Local: open file handle
            full_path = Path(self.storage.get_full_path(
                f"{self.active_path}/{self.current_segment_name}"
            ))
            self.current_segment_handle = open(full_path, 'ab', buffering=0)
        else:
            # S3: use in-memory buffer
            self.current_segment_buffer = io.BytesIO()
        
        self.current_segment_size = 0
        
        logger.info(
            f"[LogWriter] Initialized segment: {self.current_segment_name}"
        )
    
    def _write_to_segment(self, records: list) -> int:
        """Write records to segment (synchronous, runs in executor)."""
        bytes_written = 0
        
        for record in records:
            line = to_ndjson(record)
            line_bytes = line.encode('utf-8')
            
            if self.storage.backend_type == "local":
                # Write to file handle
                self.current_segment_handle.write(line_bytes)
            else:
                # Write to buffer
                self.current_segment_buffer.write(line_bytes)
            
            bytes_written += len(line_bytes)
        
        # Sync for local storage
        if self.storage.backend_type == "local" and self.enable_fsync:
            self.current_segment_handle.flush()
            os.fsync(self.current_segment_handle.fileno())
        
        return bytes_written
    
    def _rotate_segment(self):
        """Rotate segment (synchronous, runs in executor)."""
        if not self.current_segment_name:
            logger.warning("[LogWriter] No active segment to rotate")
            return
        
        old_name = self.current_segment_name
        old_date_hour = self.current_date_hour
        old_size_mb = self.current_segment_size / 1024 / 1024
        
        # Close/upload current segment
        active_segment_path = f"{self.active_path}/{self.current_segment_name}"
        ready_segment_path = f"{self.ready_path}/{self.current_segment_name}"
        
        if self.storage.backend_type == "local":
            # Close file handle
            self.current_segment_handle.close()
            self.current_segment_handle = None
            
            # Move to ready (atomic rename)
            try:
                src = Path(self.storage.get_full_path(active_segment_path))
                dst = Path(self.storage.get_full_path(ready_segment_path))
                src.rename(dst)
            except Exception as e:
                logger.error(f"[LogWriter] Failed to move segment: {e}")
                # Try to reopen
                self.current_segment_handle = open(
                    self.storage.get_full_path(active_segment_path),
                    'ab',
                    buffering=0
                )
                return
        
        else:
            # S3: upload buffer to ready path directly
            try:
                self.storage.write_bytes(
                    self.current_segment_buffer.getvalue(),
                    ready_segment_path
                )
                self.current_segment_buffer = None
            except Exception as e:
                logger.error(f"[LogWriter] Failed to upload segment: {e}")
                return
        
        logger.info(
            f"[LogWriter] Rotated segment {old_name} ({old_size_mb:.2f} MB) → ready/"
        )
        
        # Check if hour changed
        now = utc_now()
        new_date_hour = now.strftime("%Y%m%dT%H")
        
        if new_date_hour != self.current_date_hour:
            self.current_date_hour = new_date_hour
            self.current_hour_counter = 1
        else:
            self.current_hour_counter += 1
        
        # Create new segment
        self.current_segment_name = (
            f"segment_{self.current_date_hour}_{self.current_hour_counter:05d}.ndjson"
        )
        
        if self.storage.backend_type == "local":
            full_path = Path(self.storage.get_full_path(
                f"{self.active_path}/{self.current_segment_name}"
            ))
            self.current_segment_handle = open(full_path, 'ab', buffering=0)
        else:
            self.current_segment_buffer = io.BytesIO()
        
        self.current_segment_size = 0
        self.stats["rotations"] += 1
        
        logger.info(f"[LogWriter] New segment: {self.current_segment_name}")
    
    def _close_and_move_to_ready(self):
        """Close and move final segment to ready (synchronous)."""
        if not self.current_segment_name:
            return
        
        active_segment_path = f"{self.active_path}/{self.current_segment_name}"
        ready_segment_path = f"{self.ready_path}/{self.current_segment_name}"
        
        if self.storage.backend_type == "local":
            if self.current_segment_handle:
                self.current_segment_handle.close()
                self.current_segment_handle = None
            
            # Move to ready
            try:
                src = Path(self.storage.get_full_path(active_segment_path))
                dst = Path(self.storage.get_full_path(ready_segment_path))
                if src.exists():
                    src.rename(dst)
                    logger.info(f"[LogWriter] Moved final segment to ready/: {self.current_segment_name}")
            except Exception as e:
                logger.error(f"[LogWriter] Failed to move final segment: {e}")
        
        else:
            # S3: upload buffer
            if self.current_segment_buffer:
                try:
                    self.storage.write_bytes(
                        self.current_segment_buffer.getvalue(),
                        ready_segment_path
                    )
                    self.current_segment_buffer = None
                    logger.info(f"[LogWriter] Uploaded final segment to ready/: {self.current_segment_name}")
                except Exception as e:
                    logger.error(f"[LogWriter] Failed to upload final segment: {e}")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get writer statistics."""
        return {
            **self.stats,
            "queue_size": self.queue.qsize(),
            "buffer_size": len(self.buffer),
            "current_segment": self.current_segment_name,
            "current_segment_size_mb": self.current_segment_size / 1024 / 1024,
        }
