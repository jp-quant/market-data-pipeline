"""Tests for time utilities."""
import pytest
from datetime import datetime, timezone
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from ingestion.utils.time import (
    utc_now,
    utc_timestamp,
    parse_timestamp,
    format_timestamp_iso,
)


def test_utc_now():
    """Test UTC now."""
    now = utc_now()
    assert now.tzinfo == timezone.utc


def test_utc_timestamp():
    """Test UTC timestamp."""
    ts = utc_timestamp()
    assert isinstance(ts, float)
    assert ts > 0


def test_parse_timestamp_iso():
    """Test parsing ISO timestamp."""
    ts_str = "2025-11-20T12:00:00Z"
    dt = parse_timestamp(ts_str)
    assert dt.year == 2025
    assert dt.month == 11
    assert dt.day == 20
    assert dt.hour == 12


def test_parse_timestamp_unix():
    """Test parsing Unix timestamp."""
    ts = 1700481600.0  # 2023-11-20 12:00:00 UTC
    dt = parse_timestamp(ts)
    assert dt.year == 2023
    assert dt.month == 11
    assert dt.day == 20


def test_format_timestamp_iso():
    """Test formatting timestamp."""
    dt = datetime(2025, 11, 20, 12, 0, 0, tzinfo=timezone.utc)
    ts_str = format_timestamp_iso(dt)
    assert "2025-11-20" in ts_str
    assert "12:00:00" in ts_str
