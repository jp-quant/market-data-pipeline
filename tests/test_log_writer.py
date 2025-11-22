"""Tests for log writer."""
import pytest
import asyncio
from pathlib import Path
import sys

# Add parent to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from ingestion.writers.log_writer import LogWriter
from ingestion.utils.time import utc_now


@pytest.mark.asyncio
async def test_log_writer_basic(temp_dir):
    """Test basic log writer functionality."""
    writer = LogWriter(
        output_dir=str(temp_dir),
        source_name="test",
        batch_size=5,
        flush_interval_seconds=1.0,
        queue_maxsize=100,
    )
    
    await writer.start()
    
    # Write some records
    for i in range(10):
        record = {
            "id": i,
            "timestamp": utc_now().isoformat(),
            "data": f"test-{i}",
        }
        await writer.write(record)
    
    # Wait for flush
    await asyncio.sleep(2)
    
    # Stop writer
    await writer.stop()
    
    # Check file was created
    log_file = temp_dir / "test" / f"{utc_now().strftime('%Y-%m-%d')}.ndjson"
    assert log_file.exists()
    
    # Check records
    lines = log_file.read_text().strip().split('\n')
    assert len(lines) == 10


@pytest.mark.asyncio
async def test_log_writer_backpressure(temp_dir):
    """Test backpressure handling."""
    writer = LogWriter(
        output_dir=str(temp_dir),
        source_name="test",
        batch_size=100,
        flush_interval_seconds=10.0,
        queue_maxsize=10,  # Small queue
    )
    
    await writer.start()
    
    # Fill queue
    for i in range(10):
        await writer.write({"id": i})
    
    # Next write should raise QueueFull with block=False
    with pytest.raises(asyncio.QueueFull):
        await writer.write({"id": 11}, block=False)
    
    await writer.stop()


@pytest.mark.asyncio
async def test_log_writer_stats(temp_dir):
    """Test statistics tracking."""
    writer = LogWriter(
        output_dir=str(temp_dir),
        source_name="test",
        batch_size=5,
        flush_interval_seconds=1.0,
    )
    
    await writer.start()
    
    # Write records
    for i in range(12):
        await writer.write({"id": i})
    
    await asyncio.sleep(2)
    await writer.stop()
    
    stats = writer.get_stats()
    assert stats["messages_received"] == 12
    assert stats["messages_written"] == 12
    assert stats["flushes"] >= 2  # At least 2 flushes for 12 records with batch_size=5
