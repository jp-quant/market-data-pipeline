"""
Unified Parquet writer for ETL output with scalable partitioning.

Works with any StorageBackend (local filesystem or S3).
"""
import io
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional, Union
from datetime import datetime
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import uuid

from storage.base import StorageBackend
from .base_writer import BaseWriter

logger = logging.getLogger(__name__)


class ParquetWriter(BaseWriter):
    """
    Write processed records to Parquet files with scalable partitioning.
    
    Design:
    - Partitions by date AND hour (keeps files small ~100MB each)
    - Micro-batches within hour (UUID-based filenames, no overwrites)
    - Schema evolution via PyArrow schema merging
    - Works with any StorageBackend (local or S3)
    
    Directory structure (relative to storage root):
        {base_path}/ticker/
          product_id=BTC-USD/
            date=2025-11-20/
              part-20251120T14-abc123.parquet
              part-20251120T14-def456.parquet
          product_id=ETH-USD/
            date=2025-11-21/
              part-20251121T09-ghi789.parquet
    
    Query examples:
    - DuckDB: SELECT * FROM '.../**/*.parquet' WHERE date='2025-11-20'
    - Spark: spark.read.parquet("...").filter("date='2025-11-20'")
    - Polars: pl.scan_parquet(".../**/*.parquet")
    """
    
    def __init__(
        self,
        storage: StorageBackend,
        compression: str = "zstd"
    ):
        """
        Initialize Parquet writer.
        
        Args:
            storage: Storage backend instance
            compression: Compression codec (zstd, snappy, gzip, etc.)
        """
        super().__init__()
        self.storage = storage
        self.compression = compression
        
        logger.info(
            f"[ParquetWriter] Initialized: storage={storage.backend_type}, "
            f"compression={compression}"
        )
    
    def write(
        self,
        data: Union[List[Dict[str, Any]], pd.DataFrame],
        output_path: str,
        partition_cols: Optional[List[str]] = None,
        **kwargs
    ):
        """
        Write records to Parquet with flexible partitioning.
        
        Strategy:
        - Auto-detects data format (list[dict] or DataFrame)
        - If partition_cols specified: creates Hive-style partitions
        - Otherwise: flat write with UUID filename
        - NO loading of existing files (append-only, scales to TB+)
        
        Args:
            data: Records as list[dict] or DataFrame
            output_path: Path to write to (relative to storage root)
            partition_cols: Columns to partition by (e.g., ['product_id', 'date'])
                          Creates directory structure: product_id=BTC-USD/date=2025-11-22/
            **kwargs: Additional options (unused, for future extensibility)
        
        Example:
            # Flat write
            writer.write(records, "processed/coinbase/ticker")
            
            # Partitioned write
            writer.write(records, "processed/coinbase/level2", 
                        partition_cols=["product_id", "date"])
        """
        if data is None:
            logger.warning("[ParquetWriter] No data to write")
            return
        
        # Convert to DataFrame if needed
        if isinstance(data, list):
            df = pd.DataFrame(data)
        elif isinstance(data, pd.DataFrame):
            df = data
        else:
            raise ValueError(f"Unsupported data type: {type(data)}")
        
        if df.empty:
            logger.warning("[ParquetWriter] Empty DataFrame, skipping write")
            return
        
        try:
            if partition_cols:
                self._write_partitioned(df, output_path, partition_cols)
            else:
                self._write_flat(df, output_path)
        
        except Exception as e:
            self.stats["errors"] += 1
            logger.error(f"[ParquetWriter] Error writing to Parquet: {e}", exc_info=True)
            raise
    
    def _write_partitioned(
        self,
        df: pd.DataFrame,
        output_path: str,
        partition_cols: List[str]
    ):
        """
        Write with Hive-style partitioning.
        
        Creates directory structure like:
          output_path/
            product_id=BTC-USD/
              date=2025-11-22/
                part-uuid.parquet
            product_id=ETH-USD/
              date=2025-11-22/
                part-uuid.parquet
        """
        # Validate partition columns exist
        missing_cols = set(partition_cols) - set(df.columns)
        if missing_cols:
            raise ValueError(
                f"Partition columns {missing_cols} not found in data. "
                f"Available columns: {list(df.columns)}"
            )
        
        # Group by partition columns
        grouped = df.groupby(partition_cols, dropna=False)
        
        for partition_values, partition_df in grouped:
            # Ensure partition_values is iterable
            if not isinstance(partition_values, tuple):
                partition_values = (partition_values,)
            
            # Build partition path
            partition_path = output_path
            for col, val in zip(partition_cols, partition_values):
                # Sanitize partition value (e.g. replace '/' with '-')
                safe_val = str(val).replace("/", "-")
                partition_path = self.storage.join_path(partition_path, f"{col}={safe_val}")
            
            # Ensure directory exists (local only, no-op for S3)
            self.storage.mkdir(partition_path)
            
            # Keep partition columns in data (NOT dropped - preserved for data integrity)
            # Directory structure provides partition pruning optimization
            # But columns remain in Parquet for standalone querying
            # data_df = partition_df.drop(columns=partition_cols, errors='ignore')
            data_df = partition_df
            
            # Generate unique filename
            filename = self._generate_unique_filename()
            file_path = self.storage.join_path(partition_path, filename)
            
            # Write to storage
            self._write_parquet_to_storage(data_df, file_path)
            
            self.stats["records_written"] += len(partition_df)
            self.stats["files_written"] += 1
            
            logger.info(
                f"[ParquetWriter] Wrote {len(partition_df)} records to "
                f"{file_path} "
                f"({data_df.memory_usage(deep=True).sum() / 1024:.1f} KB)"
            )
        
        logger.info(
            f"[ParquetWriter] Total: {self.stats['records_written']} records written "
            f"across {self.stats['files_written']} files"
        )
    
    def _write_flat(
        self,
        df: pd.DataFrame,
        output_path: str
    ):
        """
        Write without partitioning (single file with UUID name).
        """
        # Ensure directory exists
        self.storage.mkdir(output_path)
        
        # Generate unique filename
        filename = self._generate_unique_filename()
        file_path = self.storage.join_path(output_path, filename)
        
        # Write to storage
        self._write_parquet_to_storage(df, file_path)
        
        self.stats["records_written"] += len(df)
        self.stats["files_written"] += 1
        
        logger.info(
            f"[ParquetWriter] Wrote {len(df)} records to {file_path} "
            f"({df.memory_usage(deep=True).sum() / 1024:.1f} KB)"
        )
    
    def _write_parquet_to_storage(self, df: pd.DataFrame, file_path: str):
        """
        Write DataFrame as Parquet to storage backend.
        
        Args:
            df: DataFrame to write
            file_path: Relative path from storage root
        """
        # Convert to PyArrow table
        table = pa.Table.from_pandas(df)
        
        if self.storage.backend_type == "local":
            # Local: write directly to file
            full_path = self.storage.get_full_path(file_path)
            pq.write_table(
                table,
                full_path,
                compression=self.compression
            )
        
        else:
            # S3: write to buffer then upload
            buffer = io.BytesIO()
            pq.write_table(
                table,
                buffer,
                compression=self.compression
            )
            
            # Upload to S3
            self.storage.write_bytes(buffer.getvalue(), file_path)
    
    def _generate_unique_filename(self) -> str:
        """Generate unique filename with timestamp and UUID."""
        timestamp = datetime.now().strftime("%Y%m%dT%H")
        unique_id = str(uuid.uuid4())[:8]
        return f"part_{timestamp}_{unique_id}.parquet"
