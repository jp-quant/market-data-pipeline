"""FluxForge - A modular, high-throughput market data ingestion + ETL pipeline."""

__version__ = "0.1.0"
__author__ = "FluxForge Contributors"
__license__ = "MIT"

from config import FluxForgeConfig, load_config

__all__ = ["FluxForgeConfig", "load_config", "__version__"]
