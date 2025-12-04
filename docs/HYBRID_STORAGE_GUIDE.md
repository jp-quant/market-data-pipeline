# Hybrid Storage Architecture Guide

## Overview

FluxForge uses **explicit storage configuration** where each layer (ingestion, ETL input, ETL output) can have its own storage backend. This provides maximum flexibility to optimize for performance, cost, and durability.

## Architecture

### Explicit Configuration

All storage is configured per-layer:

```yaml
storage:
  # Where ingestion writes raw segments
  ingestion_storage:
    backend: "local"  # or "s3"
    base_dir: "F:/"   # or bucket name
  
  # Where ETL reads raw segments from
  etl_storage_input:
    backend: "local"
    base_dir: "F:/"
  
  # Where ETL writes processed data
  etl_storage_output:
    backend: "local"
    base_dir: "F:/"
```

This explicit approach eliminates confusion and gives full control.

## Common Storage Patterns

### Pattern 1: All Local (Development/Testing)

**Use Case:** Fast development, testing, local analysis

**Configuration:**
```yaml
storage:
  ingestion_storage:
    backend: "local"
    base_dir: "F:/"
  
  etl_storage_input:
    backend: "local"
    base_dir: "F:/"
  
  etl_storage_output:
    backend: "local"
    base_dir: "F:/"
```

**Benefits:**
- ✅ No cloud costs
- ✅ Fast performance
- ✅ Easy debugging
- ✅ No network dependency

**Drawbacks:**
- ❌ Limited by disk space
- ❌ No durability guarantees
- ❌ Single machine only

### Pattern 2: Ingest Local → ETL to S3 (RECOMMENDED)

**Use Case:** Production - fast ingestion, durable long-term storage

**Configuration:**
```yaml
storage:
  ingestion_storage:
    backend: "local"
- ❌ Manual data management if disk fills up

### Pattern 3: All S3 (Fully Cloud)fills up

### Pattern 2: Ingest S3 → ETL Local (Not Recommended)

**Use Case:** Centralized ingestion, fast ETL processing

**Configuration:**
```yaml
storage:
  ingestion_storage:
    backend: "s3"
    base_dir: "market-data-vault"
    s3:
      bucket: "market-data-vault"
  
  etl_storage_input:
    backend: "s3"
    base_dir: "market-data-vault"
    s3:
      bucket: "market-data-vault"
  
  etl_storage_output:
    backend: "local"
    base_dir: "F:/"
```

**Why Not Recommended:**
- Adds network latency to ingestion (bad for low-latency feeds)
- Higher S3 costs (many small PUT requests)
- Processed data not durable (local only)

**Use Case:** Distributed systems, serverless, multi-region

**Configuration:**
```yaml
storage:
  ingestion_storage:
    backend: "s3"
    base_dir: "market-data-vault"
    s3:
      bucket: "market-data-vault"
  
  etl_storage_input:
    backend: "s3"
    base_dir: "market-data-vault"
    s3:
      bucket: "market-data-vault"
  
  etl_storage_output:
    backend: "s3"
    base_dir: "market-data-vault"
    s3:
      bucket: "market-data-vault"
```3:
    bucket: "market-data-vault"
```

**Benefits:**
- ✅ Fully cloud-native
- ✅ Can run ingestion/ETL on different machines
- ✅ Multi-region support
- ✅ No local disk requirements

**Drawbacks:**
- ❌ Network latency on ingestion
- ❌ Higher S3 costs (PUT requests for raw segments)

## Implementation Details

### Code Changes

The hybrid mode implementation automatically handles:

1. **Separate Storage Backends:**
   - `create_ingestion_storage()` - For ingestion layer
   - `create_etl_storage_input()` - For reading raw data
   - `create_etl_storage_output()` - For writing processed data
### Example Usage

**Start Ingestion:**
```powershell
python scripts/run_ingestion.py
```

**Start ETL:**
```powershell
python scripts/run_etl_watcher.py
```

Configuration is read from `config/config.yaml` automatically. explicit configuration approach works seamlessly:
   - `get_etl_output_path()` uses ETL output config

### Example Usage

**Start Ingestion (writes to local):**
```powershell
python scripts/run_ingestion.py --config config/config.hybrid.yaml
```

**Start ETL (reads from local, writes to S3):**
```powershell
python scripts/run_etl_watcher.py --config config/config.hybrid.yaml
3. **Path Resolution:**
   - All path helpers work with explicit storage configs
   - `get_ingestion_path()` uses ingestion_storage config
   - `get_etl_input_path()` uses etl_storage_input config
   - `get_etl_output_path()` uses etl_storage_output config
[ingestion] Active: F:/raw/active/coinbase/
[ingestion] Ready:  F:/raw/ready/coinbase/

[etl] Storage Input:  local @ F:/
[etl] Storage Output: s3 @ market-data-vault
[etl] Reading from F:/raw/ready/coinbase/
[etl] Writing to s3://market-data-vault/processed/coinbase/
```

## Directory Structure

### Local (Ingestion)
```
F:/
├── raw/
│   ├── active/
│   │   └── coinbase/
│   │       └── segment_20251203T14_00001.ndjson
│   ├── ready/
│   │   └── coinbase/
│   │       └── segment_20251203T14_00002.ndjson
│   └── processing/
│       └── coinbase/
│           └── segment_20251203T14_00003.ndjson
```

### S3 (Processed Data)
```
s3://market-data-vault/
└── processed/
    └── coinbase/
        ├── ticker/
        │   └── product_id=BTC-USD/
        │       └── date=2025-12-03/
        │           └── part_20251203T14_abc12345.parquet
        ├── level2/
        │   └── product_id=BTC-USD/
        │       └── date=2025-12-03/
        │           └── part_20251203T14_def67890.parquet
        └── market_trades/
            └── product_id=BTC-USD/
                └── date=2025-12-03/
                    └── part_20251203T14_ghi78901.parquet
```

## Disk Management

When using local ingestion, monitor disk usage:

```powershell
# Check disk usage
Get-PSDrive F | Select-Object Used, Free, @{Name="UsedGB";Expression={[math]::Round($_.Used/1GB,2)}}, @{Name="FreeGB";Expression={[math]::Round($_.Free/1GB,2)}}

# Check raw data size
Get-ChildItem F:\raw -Recurse | Measure-Object -Property Length -Sum | Select-Object @{Name="SizeGB";Expression={[math]::Round($_.Sum/1GB,2)}}
```

### Cleanup Strategy

After successful ETL (when `delete_after_processing: true`):
- Processed segments automatically deleted from `ready/`
- `active/` segments move to `ready/` on rotation
- Only actively writing segment remains in `active/`

If disk space is low:
```powershell
# Delete old processed segments (if needed)
Remove-Item F:\raw\processing\coinbase\*.ndjson -Force

# Or clear everything (if already in S3)
Remove-Item F:\raw\ready\coinbase\*.ndjson -Force
```

## Cost Analysis

### Hybrid (Local Ingest → S3 ETL)

**Assumptions:**
- 100 crypto products
- 1GB/hour raw data ingestion
- 10:1 compression ratio after ETL
- 730 hours/month

**Costs:**
- Local storage: Free (existing disk)
- S3 storage: ~$2.30/month (100GB processed @ $0.023/GB)
- S3 PUT requests: ~$0.50/month (100k files @ $0.005/1000)
- **Total: ~$3/month**

### All-S3

**Same assumptions:**

**Costs:**
- S3 storage: ~$16/month (700GB raw + processed @ $0.023/GB)
- S3 PUT requests: ~$3.65/month (730k raw segments @ $0.005/1000)
- **Total: ~$20/month**

**Savings with hybrid: ~85%**

## Monitoring

### Ingestion Health
```powershell
# Check if ingestion is writing
Get-ChildItem F:\raw\active\coinbase\ -Name | Select-Object -Last 5

# Check segment sizes
Get-ChildItem F:\raw\active\coinbase\ | Select-Object Name, @{Name="SizeMB";Expression={[math]::Round($_.Length/1MB,2)}}
```

### ETL Health
```powershell
# Check if ETL is processing
Get-ChildItem F:\raw\ready\coinbase\ -Name | Select-Object -First 5

# Check S3 processed data
aws s3 ls s3://market-data-vault/processed/coinbase/ticker/ --recursive --human-readable | Select-Object -Last 10
```

### Logs
```powershell
# Ingestion logs show local writes
[LogWriter] Flushed 100 records (45 KB) to F:/raw/active/coinbase/segment_*.ndjson

# ETL logs show mixed storage
[ETLJob] Reading from F:/raw/ready/coinbase/segment_*.ndjson (local)
[ParquetWriter] Wrote 1000 records to s3://market-data-vault/processed/coinbase/ticker/...
```

## Troubleshooting

### Issue: "Disk full during ingestion"
## Migration

### Switching Storage Patterns

Simply update the storage config blocks in `config/config.yaml`:
3. **Restart services:**
   ```powershell
   # Stop ingestion (Ctrl+C)
   # Stop ETL (Ctrl+C)
   
   # Restart with new config
   python scripts/run_ingestion.py --config config/config.hybrid.yaml
   python scripts/run_etl_watcher.py --config config/config.hybrid.yaml
   ```

### From Hybrid to All-S3

1. **Ensure all local segments processed**
2. **Update config to unified S3**
3. **Restart services**

## Best Practices

1. **Monitor disk space** when using local ingestion
2. **Set `delete_after_processing: true`** to auto-cleanup
3. **Use separate S3 buckets** for different environments (dev/prod)
4. **Enable S3 lifecycle policies** for old data archival
5. **Test failover** - What happens if local disk fills?

## Summary

**Hybrid storage is ideal when:**
- ✅ You need low-latency ingestion
- ✅ You want durable long-term storage
- ✅ You want to minimize S3 costs
- ✅ You have local disk capacity

**Use all-S3 when:**
- ✅ Running distributed systems
- ✅ Need multi-region support
- ✅ Want zero local dependencies
- ✅ Cost is not primary concern
1. **Ensure all local segments are processed**
2. **Update storage configs in `config/config.yaml`**
3. **Restart services** (no code changes needed)

Example - switching from all-local to hybrid:

```yaml
# Change this:
etl_storage_output:
  backend: "local"
  base_dir: "F:/"

# To this:
etl_storage_output:
  backend: "s3"
  base_dir: "market-data-vault"
  s3:
    bucket: "market-data-vault"
```## Summary

**All-Local is ideal when:**
- ✅ Developing/testing
- ✅ Fast iteration needed
- ✅ No cloud costs desired

**Hybrid (Local→S3) is ideal when:**