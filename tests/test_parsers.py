"""Tests for Coinbase parser."""
import pytest
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from etl.parsers.coinbase_parser import CoinbaseParser


def test_parse_ticker(sample_coinbase_message):
    """Test parsing ticker messages."""
    parser = CoinbaseParser()
    
    record = {
        "capture_ts": "2025-11-20T12:00:00Z",
        "data": sample_coinbase_message,
        "metadata": {"source": "coinbase"},
    }
    
    parsed = parser.parse_record(record)
    
    assert parsed is not None
    assert len(parsed) == 1
    assert parsed[0]["channel"] == "ticker"
    assert parsed[0]["product_id"] == "BTC-USD"
    assert parsed[0]["price"] == 50000.00


def test_parse_unknown_channel():
    """Test parsing unknown channel."""
    parser = CoinbaseParser()
    
    record = {
        "capture_ts": "2025-11-20T12:00:00Z",
        "data": {"channel": "unknown", "events": []},
        "metadata": {"source": "coinbase"},
    }
    
    parsed = parser.parse_record(record)
    assert parsed is None


def test_parser_stats(sample_coinbase_message):
    """Test parser statistics."""
    parser = CoinbaseParser()
    
    record = {
        "capture_ts": "2025-11-20T12:00:00Z",
        "data": sample_coinbase_message,
        "metadata": {"source": "coinbase"},
    }
    
    parser.parse_record(record)
    parser.parse_record(record)
    
    stats = parser.get_stats()
    assert stats["parsed"] == 2
    assert stats["by_channel"]["ticker"] == 2
