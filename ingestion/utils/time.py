"""Time utilities for timestamping ingested data."""
import time
from datetime import datetime, timezone
from typing import Union


def utc_now() -> datetime:
    """Get current UTC time as timezone-aware datetime."""
    return datetime.now(timezone.utc)


def utc_timestamp() -> float:
    """Get current UTC timestamp as float (seconds since epoch)."""
    return time.time()


def utc_timestamp_ms() -> int:
    """Get current UTC timestamp in milliseconds."""
    return int(time.time() * 1000)


def utc_timestamp_ns() -> int:
    """Get current UTC timestamp in nanoseconds."""
    return time.time_ns()


def parse_timestamp(ts: Union[str, int, float, datetime]) -> datetime:
    """
    Parse various timestamp formats into timezone-aware datetime.
    
    Args:
        ts: Timestamp in various formats (ISO string, unix epoch, datetime)
        
    Returns:
        Timezone-aware datetime object in UTC
    """
    if isinstance(ts, datetime):
        if ts.tzinfo is None:
            return ts.replace(tzinfo=timezone.utc)
        return ts.astimezone(timezone.utc)
    
    if isinstance(ts, str):
        try:
            # Try ISO format
            dt = datetime.fromisoformat(ts.replace('Z', '+00:00'))
            if dt.tzinfo is None:
                return dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except ValueError:
            # Try parsing as unix timestamp
            try:
                return datetime.fromtimestamp(float(ts), tz=timezone.utc)
            except (ValueError, OSError):
                raise ValueError(f"Unable to parse timestamp: {ts}")
    
    if isinstance(ts, (int, float)):
        # Handle both seconds and milliseconds
        if ts > 1e12:  # Likely milliseconds
            ts = ts / 1000
        return datetime.fromtimestamp(ts, tz=timezone.utc)
    
    raise TypeError(f"Unsupported timestamp type: {type(ts)}")


def format_timestamp_iso(dt: datetime) -> str:
    """Format datetime as ISO 8601 string."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.isoformat()
