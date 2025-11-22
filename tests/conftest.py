"""Test configuration fixtures."""
import pytest
from pathlib import Path
import tempfile
import shutil


@pytest.fixture
def temp_dir():
    """Create a temporary directory for tests."""
    temp_path = tempfile.mkdtemp()
    yield Path(temp_path)
    shutil.rmtree(temp_path)


@pytest.fixture
def sample_config():
    """Sample configuration for testing."""
    return {
        "coinbase": {
            "api_key": "test-key",
            "api_secret": "test-secret",
            "product_ids": ["BTC-USD"],
            "channels": ["ticker"],
        },
        "ingestion": {
            "output_dir": "./test_data/raw",
            "batch_size": 10,
            "flush_interval_seconds": 1.0,
            "queue_maxsize": 100,
        },
        "etl": {
            "input_dir": "./test_data/raw",
            "output_dir": "./test_data/processed",
        },
        "log_level": "DEBUG",
    }


@pytest.fixture
def sample_coinbase_message():
    """Sample Coinbase WebSocket message."""
    return {
        "channel": "ticker",
        "timestamp": "2025-11-20T12:00:00Z",
        "events": [
            {
                "type": "snapshot",
                "tickers": [
                    {
                        "product_id": "BTC-USD",
                        "price": "50000.00",
                        "volume_24_h": "1000.5",
                        "best_bid": "49999.00",
                        "best_ask": "50001.00",
                    }
                ],
            }
        ],
    }
