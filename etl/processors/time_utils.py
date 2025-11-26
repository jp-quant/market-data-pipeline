"""Timestamp parsing utilities for ETL processors."""
import logging
from datetime import datetime
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)


def parse_timestamp_fields(timestamp_str: Optional[str]) -> Dict[str, Any]:
    """
    Parse ISO8601 timestamp string and extract all time components.
    
    Handles multiple formats:
    - 2025-11-26T00:02:40.498898Z
    - 2025-11-26T00:02:40.644503+00:00
    - 2025-11-26T00:02:40Z
    - 2025-11-26T00:02:40
    
    Args:
        timestamp_str: ISO8601 timestamp string
        
    Returns:
        Dictionary with time fields: year, month, day, hour, minute, second, 
        microsecond, date, datetime_obj. Returns empty dict on parse failure.
    """
    if not timestamp_str:
        return {}
    
    try:
        # Normalize timezone format (Z -> +00:00)
        dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
        
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
        logger.debug(f"No timestamp found in fields: {timestamp_field}, {fallback_field}")
        return record
    
    # Parse and add time fields
    time_fields = parse_timestamp_fields(timestamp_str)
    
    # Add with optional prefix
    for key, value in time_fields.items():
        field_name = f"{prefix}{key}" if prefix else key
        record[field_name] = value
    
    return record
