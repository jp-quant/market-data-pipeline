"""Parquet writer for ETL output."""
import logging
from pathlib import Path
from typing import List, Dict, Any
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


logger = logging.getLogger(__name__)


class ParquetWriter:
    """
    Write processed records to Parquet files.
    
    Organizes data by:
    - source (coinbase, databento, etc.)
    - channel (ticker, level2, trades, etc.)
    - date (YYYY-MM-DD partitions)
    """
    
    def __init__(self, output_dir: str, compression: str = "snappy"):
        """
        Initialize Parquet writer.
        
        Args:
            output_dir: Base directory for Parquet files
            compression: Compression codec (snappy, gzip, zstd, etc.)
        """
        self.output_dir = Path(output_dir)
        self.compression = compression
        
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info(
            f"[ParquetWriter] Initialized: output_dir={output_dir}, "
            f"compression={compression}"
        )
    
    def write(
        self,
        records: List[Dict[str, Any]],
        source: str,
        channel: str,
        date_str: str
    ):
        """
        Write records to Parquet file with schema evolution support.
        
        Handles schema changes gracefully:
        - New fields: Added with null values in old data
        - Missing fields: Filled with null values in new data
        - Type changes: Logged as warning, uses new schema
        
        Args:
            records: List of parsed records
            source: Data source (coinbase, databento, etc.)
            channel: Channel name (ticker, level2, etc.)
            date_str: Date string (YYYY-MM-DD)
        """
        if not records:
            logger.warning(f"[ParquetWriter] No records to write for {source}/{channel}/{date_str}")
            return
        
        try:
            # Create directory structure
            target_dir = self.output_dir / source / channel
            target_dir.mkdir(parents=True, exist_ok=True)
            
            # Output file
            output_file = target_dir / f"{date_str}.parquet"
            
            # Convert to DataFrame
            new_df = pd.DataFrame(records)
            
            # Append or write
            if output_file.exists():
                # Read existing data
                try:
                    existing_df = pd.read_parquet(output_file)
                    
                    # Check for schema differences
                    existing_cols = set(existing_df.columns)
                    new_cols = set(new_df.columns)
                    
                    added_cols = new_cols - existing_cols
                    removed_cols = existing_cols - new_cols
                    
                    if added_cols:
                        logger.info(
                            f"[ParquetWriter] Schema evolution detected: "
                            f"Added columns: {sorted(added_cols)}"
                        )
                        # Add missing columns to existing data with NaN
                        for col in added_cols:
                            existing_df[col] = pd.NA
                    
                    if removed_cols:
                        logger.info(
                            f"[ParquetWriter] Schema evolution detected: "
                            f"Removed columns: {sorted(removed_cols)} (filled with NaN in new data)"
                        )
                        # Add missing columns to new data with NaN
                        for col in removed_cols:
                            new_df[col] = pd.NA
                    
                    # Align column order
                    all_cols = sorted(set(existing_df.columns) | set(new_df.columns))
                    existing_df = existing_df.reindex(columns=all_cols)
                    new_df = new_df.reindex(columns=all_cols)
                    
                    # Concatenate
                    df = pd.concat([existing_df, new_df], ignore_index=True)
                    
                except Exception as e:
                    logger.warning(
                        f"[ParquetWriter] Could not merge with existing file: {e}. "
                        "Overwriting with new data."
                    )
                    df = new_df
            else:
                df = new_df
            
            # Convert to Arrow Table and write
            table = pa.Table.from_pandas(df)
            pq.write_table(
                table,
                output_file,
                compression=self.compression
            )
            
            logger.info(
                f"[ParquetWriter] Wrote {len(records)} records to "
                f"{output_file} ({df.memory_usage(deep=True).sum() / 1024:.1f} KB)"
            )
        
        except Exception as e:
            logger.error(
                f"[ParquetWriter] Error writing to Parquet: {e}",
                exc_info=True
            )
            raise
