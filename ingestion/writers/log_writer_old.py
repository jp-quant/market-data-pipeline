"""Batched log writer for durable, append-only NDJSON storage with size-based rotation."""
import asyncio
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any, BinaryIO
from collections import deque

from ..utils.time import utc_now
from ..utils.serialization import to_ndjson


logger = logging.getLogger(__name__)


class LogWriter:
    """
    Batched log writer with size-based segment rotation.
    
    Design goals:
    - Bounded queue with backpressure
    - Batch writes to reduce I/O overhead
    - Size-based segment rotation (prevents unbounded growth)
    - Durability guarantees (fsync on flush)
    - Active/ready directory segregation (ETL never touches active files)
    - No blocking of the ingestion event loop
    - Works on low-power hardware (Raspberry Pi)
    
    Directory structure:
        raw/
            active/     # ONE open segment being written
            ready/      # Closed segments ready for ETL
    """
    
    def __init__(
        self,
        output_dir: str,
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
            output_dir: Base directory for log files
            source_name: Data source identifier (e.g., "coinbase", "databento")
            batch_size: Number of records to batch before writing
            flush_interval_seconds: Maximum time between flushes
            queue_maxsize: Maximum queue size (backpressure kicks in when full)
            enable_fsync: Whether to call fsync after each flush
            segment_max_mb: Max segment size in MB before rotation
        """
        self.output_dir = Path(output_dir)
        self.source_name = source_name
        self.batch_size = batch_size
        self.flush_interval = flush_interval_seconds
        self.enable_fsync = enable_fsync
        self.segment_max_bytes = segment_max_mb * 1024 * 1024  # Convert MB to bytes
        
        # Bounded queue for backpressure
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=queue_maxsize)
        
        # In-memory buffer for batching
        self.buffer: deque = deque()
        
        # Segment management
        self.current_date_hour: str = ""  # Format: YYYYMMDDTHH
        self.current_hour_counter: int = 0  # Resets each hour
        self.current_segment_path: Optional[Path] = None
        self.current_segment_handle: Optional[BinaryIO] = None
        self.current_segment_size: int = 0
        
        # Directories
        self.active_dir = self.output_dir / "active" / self.source_name
        self.ready_dir = self.output_dir / "ready" / self.source_name
        
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
        
        # Ensure directories exist
        self.active_dir.mkdir(parents=True, exist_ok=True)
        self.ready_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info(
            f"[LogWriter] Initialized: source={source_name}, "
            f"batch_size={batch_size}, flush_interval={flush_interval_seconds}s, "
            f"queue_maxsize={queue_maxsize}, fsync={enable_fsync}, "
            f"segment_max_mb={segment_max_mb}"
        )
    
    async def start(self):
        """Start the writer task and initialize first segment."""
        if self._writer_task is not None:
            logger.warning("[LogWriter] Writer task already running")
            return
        
        self._shutdown.clear()
        
        # Initialize first segment
        await asyncio.get_event_loop().run_in_executor(
            None,
            self._initialize_first_segment
        )
        
        self._writer_task = asyncio.create_task(self._writer_loop())
        logger.info(f"[LogWriter] Writer task started for source={self.source_name}")
    
    async def stop(self):
        """Stop the writer task and flush remaining data."""
        if self._writer_task is None:
            return
        
        logger.info(f"[LogWriter] Stopping writer for source={self.source_name}")
        self._shutdown.set()
        
        # Wait for writer task to finish
        try:
            await asyncio.wait_for(self._writer_task, timeout=10.0)
        except asyncio.TimeoutError:
            logger.error("[LogWriter] Writer task did not stop within timeout")
            self._writer_task.cancel()
        
        # Final flush
        await self._flush()
        
        # Close current segment and move to ready
        if self.current_segment_handle:
            await asyncio.get_event_loop().run_in_executor(
                None,
                self._close_and_move_to_ready
            )
        
        logger.info(
            f"[LogWriter] Stopped. Stats: {self.stats}"
        )
    
    async def write(self, record: Dict[str, Any], block: bool = True):
        """
        Write a record to the queue.
        
        Args:
            record: Dictionary record to write
            block: If True, blocks when queue is full. If False, raises QueueFull.
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
                f"[LogWriter] Queue full (size={self.queue.qsize()}) - "
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
                    # Timeout - flush if buffer has data
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
                        await asyncio.get_event_loop().run_in_executor(
                            None,
                            self._rotate_segment
                        )
            
            except Exception as e:
                self.stats["errors"] += 1
                logger.error(f"[LogWriter] Error in writer loop: {e}", exc_info=True)
                await asyncio.sleep(1)  # Brief pause on error
        
        # Final flush on shutdown
        if self.buffer:
            await self._flush()
    
    async def _flush(self):
        """Flush buffer to disk."""
        if not self.buffer:
            return
        
        try:
            # Write buffer to current segment
            records_to_write = list(self.buffer)
            self.buffer.clear()
            
            # Use run_in_executor to avoid blocking event loop
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
                f"({bytes_written} bytes) to {self.current_segment_path.name}, "
                f"total size: {self.current_segment_size / 1024 / 1024:.2f} MB"
            )
        
        except Exception as e:
            self.stats["errors"] += 1
            logger.error(f"[LogWriter] Error flushing buffer: {e}", exc_info=True)
    
    def _initialize_first_segment(self):
        """
        Initialize the first segment file (synchronous, runs in executor).
        Finds the next segment counter for current date-hour by scanning existing files.
        """
        now = utc_now()
        self.current_date_hour = now.strftime("%Y%m%dT%H")
        
        # Find existing segments for this date-hour to determine next counter
        existing_segments = list(self.active_dir.glob(f"segment_{self.current_date_hour}_*.ndjson"))
        existing_segments.extend(self.ready_dir.glob(f"segment_{self.current_date_hour}_*.ndjson"))
        
        max_counter = 0
        for seg in existing_segments:
            try:
                # Extract counter from filename: segment_20251120T14_00042.ndjson
                parts = seg.stem.split('_')
                if len(parts) >= 3:
                    counter = int(parts[2])
                    max_counter = max(max_counter, counter)
            except (ValueError, IndexError):
                continue
        
        self.current_hour_counter = max_counter + 1
        
        # Create new segment
        filename = f"segment_{self.current_date_hour}_{self.current_hour_counter:05d}.ndjson"
        self.current_segment_path = self.active_dir / filename
        
        # Open file handle
        self.current_segment_handle = open(
            self.current_segment_path, 'ab', buffering=0
        )
        self.current_segment_size = 0
        
        logger.info(
            f"[LogWriter] Initialized segment {self.current_date_hour}_{self.current_hour_counter:05d}: "
            f"{self.current_segment_path.name}"
        )
    
    def _write_to_segment(self, records: list) -> int:
        """
        Synchronous write to current segment (runs in executor).
        
        Args:
            records: List of records to write
            
        Returns:
            Number of bytes written
        """
        if not self.current_segment_handle:
            raise RuntimeError("No active segment handle")
        
        bytes_written = 0
        for record in records:
            line = to_ndjson(record)
            line_bytes = line.encode('utf-8')
            self.current_segment_handle.write(line_bytes)
            bytes_written += len(line_bytes)
        
        # Ensure data is written to disk
        if self.enable_fsync:
            self.current_segment_handle.flush()
            os.fsync(self.current_segment_handle.fileno())
        
        return bytes_written
    
    def _rotate_segment(self):
        """
        Rotate the current segment (synchronous, runs in executor).
        
        1. Close current segment handle
        2. Move file from active/ to ready/
        3. Check if date-hour changed (reset counter if so)
        4. Increment counter within current hour
        5. Create new segment in active/
        """
        if not self.current_segment_handle:
            logger.warning("[LogWriter] No active segment to rotate")
            return
        
        old_path = self.current_segment_path
        old_date_hour = self.current_date_hour
        old_counter = self.current_hour_counter
        old_size_mb = self.current_segment_size / 1024 / 1024
        
        # Close current handle
        self.current_segment_handle.close()
        self.current_segment_handle = None
        
        # Move to ready/ (atomic rename on same filesystem)
        ready_path = self.ready_dir / old_path.name
        try:
            os.rename(old_path, ready_path)
            logger.info(
                f"[LogWriter] Rotated segment {old_date_hour}_{old_counter:05d} ({old_size_mb:.2f} MB): "
                f"{old_path.name} → ready/"
            )
        except Exception as e:
            logger.error(f"[LogWriter] Failed to move segment to ready/: {e}")
            # Try to recover by reopening the file
            self.current_segment_handle = open(old_path, 'ab', buffering=0)
            return
        
        # Get current date-hour
        now = utc_now()
        new_date_hour = now.strftime("%Y%m%dT%H")
        
        # Check if hour changed - reset counter if so
        if new_date_hour != self.current_date_hour:
            self.current_date_hour = new_date_hour
            self.current_hour_counter = 1
            logger.info(
                f"[LogWriter] Hour changed: {old_date_hour} → {new_date_hour}, "
                "counter reset to 1"
            )
        else:
            # Same hour, increment counter
            self.current_hour_counter += 1
        
        # Create new segment
        filename = f"segment_{self.current_date_hour}_{self.current_hour_counter:05d}.ndjson"
        self.current_segment_path = self.active_dir / filename
        
        # Open new file handle
        self.current_segment_handle = open(
            self.current_segment_path, 'ab', buffering=0
        )
        self.current_segment_size = 0
        self.stats["rotations"] += 1
        
        logger.info(
            f"[LogWriter] New segment {self.current_date_hour}_{self.current_hour_counter:05d}: "
            f"{self.current_segment_path.name}"
        )
    
    def _close_and_move_to_ready(self):
        """
        Close current segment and move to ready/ on shutdown.
        """
        if not self.current_segment_handle:
            return
        
        # Close handle
        self.current_segment_handle.close()
        self.current_segment_handle = None
        
        # Move to ready/
        if self.current_segment_path and self.current_segment_path.exists():
            ready_path = self.ready_dir / self.current_segment_path.name
            try:
                os.rename(self.current_segment_path, ready_path)
                logger.info(
                    f"[LogWriter] Final segment moved to ready/: "
                    f"{self.current_segment_path.name}"
                )
            except Exception as e:
                logger.error(f"[LogWriter] Failed to move final segment: {e}")
    
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
            "current_segment_file": self.current_segment_path.name if self.current_segment_path else None,
        }
