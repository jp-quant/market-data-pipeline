"""Serialization utilities for raw message handling."""
import json
from typing import Any, Dict
from datetime import datetime


def serialize_raw_message(raw_data: bytes, timestamp: datetime, metadata: Dict[str, Any] = None) -> Dict[str, Any]:
    """
    Serialize a raw websocket message with minimal processing.
    
    Args:
        raw_data: Raw bytes from websocket
        timestamp: Capture timestamp
        metadata: Optional metadata (source, channel, etc.)
        
    Returns:
        Dictionary ready for NDJSON serialization
    """
    record = {
        "capture_ts": timestamp.isoformat(),
        "raw_bytes": raw_data.hex() if isinstance(raw_data, bytes) else str(raw_data),
    }
    
    if metadata:
        record["metadata"] = metadata
    
    return record


def serialize_json_message(data: Dict[str, Any], timestamp: datetime, metadata: Dict[str, Any] = None) -> Dict[str, Any]:
    """
    Serialize a pre-parsed JSON message with capture timestamp.
    
    Args:
        data: Parsed JSON data
        timestamp: Capture timestamp
        metadata: Optional metadata (source, channel, etc.)
        
    Returns:
        Dictionary ready for NDJSON serialization
    """
    record = {
        "capture_ts": timestamp.isoformat(),
        "data": data,
    }
    
    if metadata:
        record["metadata"] = metadata
    
    return record


def to_ndjson(record: Dict[str, Any]) -> str:
    """
    Convert record to NDJSON line (newline-delimited JSON).
    
    Args:
        record: Dictionary to serialize
        
    Returns:
        JSON string with newline
    """
    return json.dumps(record, separators=(',', ':'), default=str) + '\n'


def from_ndjson(line: str) -> Dict[str, Any]:
    """
    Parse NDJSON line back to dictionary.
    
    Args:
        line: JSON string (with or without newline)
        
    Returns:
        Parsed dictionary
    """
    return json.loads(line.strip())
