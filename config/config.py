"""Configuration management for FluxForge."""
import os
import yaml
from pathlib import Path
from typing import Optional, Dict, Any, Literal
from pydantic import BaseModel, Field


class CoinbaseConfig(BaseModel):
    """Coinbase Advanced Trade API configuration."""
    api_key: str = Field(..., description="Coinbase Advanced Trade API key")
    api_secret: str = Field(..., description="Coinbase Advanced Trade API secret (PEM format)")
    product_ids: list[str] = Field(default_factory=lambda: ["BTC-USD", "ETH-USD"])
    channels: list[str] = Field(default_factory=lambda: ["ticker", "level2", "market_trades"])
    ws_url: str = "wss://advanced-trade-ws.coinbase.com"


class DatabentoConfig(BaseModel):
    """Databento API configuration."""
    api_key: str = ""
    dataset: str = "XNAS.ITCH"
    symbols: list[str] = Field(default_factory=list)
    schema: str = "trades"


class IBKRConfig(BaseModel):
    """Interactive Brokers configuration."""
    gateway_url: str = "https://localhost:5000"
    account_id: str = ""
    contracts: list[dict] = Field(default_factory=list)


class S3Config(BaseModel):
    """S3-specific configuration."""
    bucket: str = ""
    region: Optional[str] = None  # Auto-detected if None
    aws_access_key_id: Optional[str] = None  # Uses environment/IAM role if None
    aws_secret_access_key: Optional[str] = None
    aws_session_token: Optional[str] = None
    endpoint_url: Optional[str] = None  # For S3-compatible services (MinIO, etc.)


class PathConfig(BaseModel):
    """Customizable path structure for data organization."""
    # Ingestion layer paths (relative to storage root)
    raw_dir: str = "raw"  # Base directory for raw ingestion data
    active_subdir: str = "active"  # Subdirectory for actively writing segments
    ready_subdir: str = "ready"  # Subdirectory for segments ready for ETL
    processing_subdir: str = "processing"  # Subdirectory for ETL in-progress
    
    # ETL layer paths (relative to storage root)
    processed_dir: str = "processed"  # Base directory for processed/transformed data


class StorageLayerConfig(BaseModel):
    """Storage configuration for a specific layer (ingestion or ETL)."""
    backend: Literal["local", "s3"] = "local"
    base_dir: str = "./data"
    s3: Optional[S3Config] = Field(default_factory=S3Config)


class StorageConfig(BaseModel):
    """
    Explicit storage configuration per layer.
    
    Each layer (ingestion, ETL input, ETL output) has its own storage backend.
    This provides maximum flexibility:
    - All local: Fast development/testing
    - All S3: Fully cloud-native
    - Hybrid: Ingest to local (fast), ETL to S3 (durable)
    - Multi-bucket: Different S3 buckets for raw vs processed
    
    All paths are relative to base_dir (local) or bucket (S3).
    """
    # Ingestion layer storage (where raw segments are written)
    ingestion_storage: StorageLayerConfig = Field(
        default_factory=lambda: StorageLayerConfig(backend="local", base_dir="./data")
    )
    
    # ETL input storage (where ETL reads raw segments from)
    etl_storage_input: StorageLayerConfig = Field(
        default_factory=lambda: StorageLayerConfig(backend="local", base_dir="./data")
    )
    
    # ETL output storage (where processed data is written)
    etl_storage_output: StorageLayerConfig = Field(
        default_factory=lambda: StorageLayerConfig(backend="local", base_dir="./data")
    )
    
    # Path structure configuration (applies to all storage backends)
    paths: PathConfig = Field(default_factory=PathConfig)


class IngestionConfig(BaseModel):
    """Ingestion layer configuration."""
    batch_size: int = 100
    flush_interval_seconds: float = 5.0
    queue_maxsize: int = 10000
    enable_fsync: bool = True
    auto_reconnect: bool = True
    max_reconnect_attempts: int = 10
    reconnect_delay: float = 5.0
    segment_max_mb: int = 100  # Max size in MB before rotating segment


class ChannelETLConfig(BaseModel):
    """Per-channel ETL configuration."""
    enabled: bool = True
    partition_cols: Optional[list[str]] = None  # e.g., ["product_id", "date"]
    processor_options: Dict[str, Any] = Field(default_factory=dict)


class ETLConfig(BaseModel):
    """ETL layer configuration."""
    compression: str = "snappy"
    schedule_cron: Optional[str] = None  # e.g., "0 * * * *" for hourly
    delete_after_processing: bool = True  # Delete raw segments after ETL
    
    # Channel-specific configuration
    channels: Dict[str, ChannelETLConfig] = Field(default_factory=lambda: {
        "level2": ChannelETLConfig(
            partition_cols=["product_id", "date"],
            processor_options={"add_derived_fields": True}
        ),
        "market_trades": ChannelETLConfig(
            partition_cols=["product_id", "date"],
            processor_options={"add_derived_fields": True}
        ),
        "ticker": ChannelETLConfig(
            partition_cols=["date", "hour"],
            processor_options={"add_derived_fields": True}
        ),
    })


class FluxForgeConfig(BaseModel):
    """Root configuration for FluxForge."""
    # Data sources
    coinbase: Optional[CoinbaseConfig] = None
    databento: Optional[DatabentoConfig] = None
    ibkr: Optional[IBKRConfig] = None
    
    # Storage backend
    storage: StorageConfig = Field(default_factory=StorageConfig)
    
    # Ingestion settings
    ingestion: IngestionConfig = Field(default_factory=IngestionConfig)
    
    # ETL settings
    etl: ETLConfig = Field(default_factory=ETLConfig)
    
    # Logging
    log_level: str = "INFO"
    log_format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"


def load_config(config_path: Optional[str] = None) -> FluxForgeConfig:
    """
    Load configuration from YAML file.
    
    Args:
        config_path: Path to config file. If None, looks for:
            1. FLUXFORGE_CONFIG environment variable
            2. ./config/config.yaml
            3. ~/.fluxforge/config.yaml
    
    Returns:
        FluxForgeConfig instance
    """
    if config_path is None:
        # Check environment variable
        config_path = os.environ.get("FLUXFORGE_CONFIG")
        
        if config_path is None:
            # Check default locations
            candidates = [
                Path("./config/config.yaml"),
                Path.home() / ".fluxforge" / "config.yaml",
            ]
            for candidate in candidates:
                if candidate.exists():
                    config_path = str(candidate)
                    break
    
    if config_path is None:
        raise FileNotFoundError(
            "No config file found. Set FLUXFORGE_CONFIG or create config/config.yaml"
        )
    
    config_path = Path(config_path)
    
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    
    with open(config_path, 'r') as f:
        yaml_data = yaml.safe_load(f)
    
    return FluxForgeConfig(**yaml_data)


def save_example_config(output_path: str = "./config/config.example.yaml"):
    """
    Save an example configuration file.
    
    Args:
        output_path: Where to save the example config
    """
    example = {
        "coinbase": {
            "api_key": "organizations/xxx/apiKeys/xxx",
            "api_secret": "-----BEGIN EC PRIVATE KEY-----\\n...\\n-----END EC PRIVATE KEY-----\\n",
            "product_ids": ["BTC-USD", "ETH-USD"],
            "channels": ["ticker", "level2", "market_trades"],
        },
        "databento": {
            "api_key": "your-databento-api-key",
            "dataset": "XNAS.ITCH",
            "symbols": ["AAPL", "MSFT"],
            "schema": "trades",
        },
        "ibkr": {
            "gateway_url": "https://localhost:5000",
            "account_id": "your-account-id",
        },
        "ingestion": {
            "output_dir": "./data/raw",
            "batch_size": 100,
            "flush_interval_seconds": 5.0,
            "queue_maxsize": 10000,
            "enable_fsync": True,
        },
        "etl": {
            "input_dir": "./data/raw",
            "output_dir": "./data/processed",
            "compression": "snappy",
        },
        "log_level": "INFO",
    }
    
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, 'w') as f:
        yaml.dump(example, f, default_flow_style=False, sort_keys=False)
    
    print(f"Example config saved to {output_path}")
