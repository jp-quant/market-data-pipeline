"""S3-compatible Parquet writer for ETL output.

Writes Parquet files directly to S3 with same partitioning strategy as ParquetWriter.
"""
import logging
from typing import List, Dict, Any, Optional, Union
from datetime import datetime
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import uuid
import io

from .base_writer import BaseWriter
from storage.s3.uploader import S3StorageManager

logger = logging.getLogger(__name__)


class S3ParquetWriter(BaseWriter):
    """
    Write processed records to Parquet files on S3 with scalable partitioning.
    
    Same strategy as ParquetWriter but writes to S3:
    - Partitions by specified columns (e.g., product_id, date, hour)
    - Micro-batches with UUID-based filenames
    - Uses Polars/s3fs for seamless S3 integration
    
    S3 structure:
    s3://bucket/prefix/
      processed/
        coinbase/
          ticker/
            product_id=BTC-USD/
              date=2025-11-20/
                hour=14/
                  part-20251120T14-abc123.parquet
    
    Example:
        s3_writer = S3ParquetWriter(
            s3_manager=s3,
            base_prefix="processed/coinbase"
        )
        
        s3_writer.write(
            data=df,
            output_path="ticker",
            partition_cols=["product_id", "date", "hour"]
        )
    """
    
    def __init__(
        self,
        s3_manager: S3StorageManager,
        base_prefix: str = "processed",
        compression: str = "snappy"
    ):
        """
        Initialize S3 Parquet writer.
        
        Args:
            s3_manager: S3StorageManager instance
            base_prefix: Base prefix for all processed data (e.g., "processed" or "processed/coinbase")
            compression: Compression codec
        """
        super().__init__()
        self.s3 = s3_manager
        self.base_prefix = base_prefix.strip("/") if base_prefix else ""
        self.compression = compression
        
        logger.info(
            f"[S3ParquetWriter] Initialized: s3://{s3_manager.bucket}/{s3_manager.prefix}/{base_prefix}, "
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
        Write records to S3 Parquet with flexible partitioning.
        
        Args:
            data: Records as list[dict] or DataFrame
            output_path: Relative path under base_prefix (e.g., "ticker" or "coinbase/ticker")
            partition_cols: Columns to partition by
            **kwargs: Additional options
        
        Example:
            # Writes to: s3://bucket/prefix/processed/coinbase/ticker/product_id=BTC-USD/...
            writer.write(
                data=df,
                output_path="coinbase/ticker",
                partition_cols=["product_id", "date", "hour"]
            )
        """
        if data is None:
            logger.warning("[S3ParquetWriter] No data to write")
            return
        
        # Convert to DataFrame if needed
        if isinstance(data, list):
            df = pd.DataFrame(data)
        elif isinstance(data, pd.DataFrame):
            df = data
        else:
            raise ValueError(f"Unsupported data type: {type(data)}")
        
        if df.empty:
            logger.warning("[S3ParquetWriter] Empty DataFrame, skipping write")
            return
        
        # Build full S3 prefix
        if self.base_prefix:
            full_prefix = f"{self.base_prefix}/{output_path.strip('/')}"
        else:
            full_prefix = output_path.strip("/")
        
        try:
            if partition_cols:
                self._write_partitioned(df, full_prefix, partition_cols)
            else:
                self._write_flat(df, full_prefix)
        
        except Exception as e:
            self.stats["errors"] += 1
            logger.error(f"[S3ParquetWriter] Error writing to S3 Parquet: {e}", exc_info=True)
            raise
    
    def _write_partitioned(
        self,
        df: pd.DataFrame,
        base_key: str,
        partition_cols: List[str]
    ):
        """Write with Hive-style partitioning to S3."""
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
            
            # Build partition key
            partition_key = base_key
            for col, val in zip(partition_cols, partition_values):
                partition_key = f"{partition_key}/{col}={val}"
            
            # Keep partition columns in data (same as ParquetWriter)
            data_df = partition_df
            
            # Generate unique filename
            filename = self._generate_unique_filename()
            full_key = f"{partition_key}/{filename}"
            
            # Write to S3 using PyArrow + in-memory buffer
            table = pa.Table.from_pandas(data_df)
            
            # Write to in-memory buffer
            buffer = io.BytesIO()
            pq.write_table(
                table,
                buffer,
                compression=self.compression
            )
            
            # Upload to S3
            buffer.seek(0)
            self.s3.write_bytes(
                buffer.getvalue(),
                full_key,
                content_type="application/octet-stream"
            )
            
            self.stats["records_written"] += len(partition_df)
            self.stats["files_written"] += 1
            
            logger.info(
                f"[S3ParquetWriter] Wrote {len(partition_df)} records to "
                f"s3://{self.s3.bucket}/{self.s3.get_full_key(full_key)} "
                f"({data_df.memory_usage(deep=True).sum() / 1024:.1f} KB)"
            )
        
        logger.info(
            f"[S3ParquetWriter] Total: {self.stats['records_written']} records written "
            f"across {self.stats['files_written']} files"
        )
    
    def _write_flat(
        self,
        df: pd.DataFrame,
        base_key: str
    ):
        """Write without partitioning (single file with UUID name) to S3."""
        # Generate unique filename
        filename = self._generate_unique_filename()
        full_key = f"{base_key}/{filename}"
        
        # Write to in-memory buffer
        table = pa.Table.from_pandas(df)
        buffer = io.BytesIO()
        pq.write_table(
            table,
            buffer,
            compression=self.compression
        )
        
        # Upload to S3
        buffer.seek(0)
        self.s3.write_bytes(
            buffer.getvalue(),
            full_key,
            content_type="application/octet-stream"
        )
        
        self.stats["records_written"] += len(df)
        self.stats["files_written"] += 1
        
        logger.info(
            f"[S3ParquetWriter] Wrote {len(df)} records to "
            f"s3://{self.s3.bucket}/{self.s3.get_full_key(full_key)} "
            f"({df.memory_usage(deep=True).sum() / 1024:.1f} KB)"
        )
    
    def _generate_unique_filename(self) -> str:
        """Generate unique filename with timestamp and UUID."""
        timestamp = datetime.now().strftime("%Y%m%dT%H")
        unique_id = str(uuid.uuid4())[:8]
        return f"part_{timestamp}_{unique_id}.parquet"
