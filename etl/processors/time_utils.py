"""Timestamp parsing utilities for ETL processors."""
import logging
from datetime import datetime
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

# Note: We use Python's built-in datetime over alternatives like dateutil.parser or pandas
# because:
# 1. datetime.fromisoformat() is FAST (C-optimized, ~10x faster than dateutil.parser)
# 2. No external dependencies (dateutil is heavy, pandas overkill for just parsing)
# 3. ISO8601 is the standard for financial data (Coinbase, DataBento, IBKR all use it)
# 4. We handle edge cases manually (nanoseconds, Z vs +00:00) for specific needs
#
# For truly arbitrary formats (rare in production financial data), consider:
# - dateutil.parser.parse() - slower but handles almost any format
# - pandas.to_datetime() - good for vectorized operations on DataFrames
#
# Current approach: Fast, minimal dependencies, handles 99.9% of real-world cases


def parse_timestamp_fields(timestamp_str: Optional[str]) -> Dict[str, Any]:
    """
    Parse ISO8601 timestamp string and extract all time components.
    
    Handles multiple formats:
    - 2025-11-26T00:02:40.498898Z
    - 2025-11-26T00:02:40.644503+00:00
    - 2025-11-26T00:02:40Z
    - 2025-11-26T00:02:40
    - pandas.Timestamp objects
    - datetime objects
    
    Args:
        timestamp_str: ISO8601 timestamp string, pandas.Timestamp, or datetime object
        
    Returns:
        Dictionary with time fields: year, month, day, hour, minute, second, 
        microsecond, date, datetime_obj. Returns empty dict on parse failure.
    """
    if not timestamp_str:
        return {}
    
    try:
        # Handle pandas Timestamp objects (from parquet files)
        if hasattr(timestamp_str, 'to_pydatetime'):
            dt = timestamp_str.to_pydatetime()
        # Handle datetime objects directly
        elif isinstance(timestamp_str, datetime):
            dt = timestamp_str
        # Handle string timestamps
        elif isinstance(timestamp_str, str):
            # Normalize timezone format (Z -> +00:00)
            normalized = timestamp_str.replace('Z', '+00:00')
            
            # Handle nanosecond precision (Python datetime only supports microseconds)
            # Example: 2025-11-27T00:20:02.069533736+00:00 -> 2025-11-27T00:20:02.069533+00:00
            if '.' in normalized:
                # Split on decimal point
                date_part, frac_and_tz = normalized.split('.', 1)
                # Extract fractional seconds and timezone
                if '+' in frac_and_tz:
                    frac, tz = frac_and_tz.split('+', 1)
                    # Truncate to 6 digits (microseconds)
                    frac = frac[:6]
                    normalized = f"{date_part}.{frac}+{tz}"
                elif '-' in frac_and_tz[1:]:  # Skip first char in case of negative timezone
                    frac, tz = frac_and_tz.split('-', 1)
                    frac = frac[:6]
                    normalized = f"{date_part}.{frac}-{tz}"
            
            dt = datetime.fromisoformat(normalized)
        else:
            logger.warning(f"Unsupported timestamp type: {type(timestamp_str)}")
            return {}
        
        return {
            "year": dt.year,
            "month": dt.month,
            "day": dt.day,
            "hour": dt.hour,
            "minute": dt.minute,
            "second": dt.second,
            "microsecond": dt.microsecond,
            "date": dt.strftime("%Y-%m-%d"),  # For partitioning
            "datetime": dt.isoformat(),  # Full ISO8601 string
        }
    
    except (ValueError, AttributeError) as e:
        logger.debug(f"Failed to parse timestamp '{timestamp_str}': {e}")
        return {}


def add_time_fields(
    record: Dict[str, Any],
    timestamp_field: str,
    fallback_field: Optional[str] = None,
    prefix: str = "",
) -> Dict[str, Any]:
    """
    Add derived time fields to a record based on timestamp field.
    
    Args:
        record: Record to enhance with time fields
        timestamp_field: Primary timestamp field name to parse
        fallback_field: Optional fallback timestamp field if primary is missing
        prefix: Optional prefix for generated field names (e.g., "event_")
        
    Returns:
        Record with added time fields (year, month, day, hour, minute, second, etc.)
    
    Example:
        record = {"server_timestamp": "2025-11-26T00:02:40.498898Z"}
        add_time_fields(record, "server_timestamp")
        # Adds: year, month, day, hour, minute, second, microsecond, date
    """
    # Get timestamp value from primary or fallback field
    timestamp_str = record.get(timestamp_field)
    if not timestamp_str and fallback_field:
        timestamp_str = record.get(fallback_field)
    
    if not timestamp_str:
        logger.info(f"No timestamp found in fields: {timestamp_field}, {fallback_field}")
        return record
    
    # Parse and add time fields
    time_fields = parse_timestamp_fields(timestamp_str)
    
    # Add with optional prefix
    for key, value in time_fields.items():
        field_name = f"{prefix}{key}" if prefix else key
        record[field_name] = value
    
    return record
