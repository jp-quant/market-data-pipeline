"""Health check script for FluxForge segment-based system."""
import sys
from pathlib import Path
from datetime import datetime, timedelta

# Add parent to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from config import load_config


def check_health(config_path: str = None):
    """Check system health and report status."""
    config = load_config(config_path)
    
    print("=" * 80)
    print("FluxForge Health Check")
    print("=" * 80)
    
    # Check directories
    base_dir = Path(config.ingestion.output_dir)
    
    for source in ["coinbase"]:  # Add more sources as configured
        print(f"\n📊 Source: {source}")
        print("-" * 80)
        
        active_dir = base_dir / "active" / source
        ready_dir = base_dir / "ready" / source
        processing_dir = Path(config.etl.processing_dir) / source
        
        # Check active segments
        if active_dir.exists():
            active_segments = list(active_dir.glob("segment_*.ndjson"))
            print(f"✓ Active directory: {active_dir}")
            print(f"  Segments: {len(active_segments)}")
            
            if active_segments:
                for seg in active_segments:
                    size_mb = seg.stat().st_size / 1024 / 1024
                    age_sec = (datetime.now().timestamp() - seg.stat().st_mtime)
                    age_min = int(age_sec / 60)
                    print(f"  - {seg.name}: {size_mb:.2f} MB (age: {age_min} min)")
                    
                    if size_mb > config.ingestion.segment_max_mb * 0.9:
                        print(f"    ⚠️  Near rotation threshold!")
        else:
            print(f"❌ Active directory missing: {active_dir}")
        
        # Check ready segments
        if ready_dir.exists():
            ready_segments = list(ready_dir.glob("segment_*.ndjson"))
            print(f"\n✓ Ready directory: {ready_dir}")
            print(f"  Segments: {len(ready_segments)}")
            
            if ready_segments:
                total_size_mb = sum(s.stat().st_size for s in ready_segments) / 1024 / 1024
                oldest = min(ready_segments, key=lambda s: s.stat().st_mtime)
                oldest_age_sec = datetime.now().timestamp() - oldest.stat().st_mtime
                oldest_age_min = int(oldest_age_sec / 60)
                
                print(f"  Total size: {total_size_mb:.2f} MB")
                print(f"  Oldest: {oldest.name} ({oldest_age_min} min ago)")
                
                if len(ready_segments) > 50:
                    print(f"  ⚠️  Large backlog - ETL falling behind!")
                elif len(ready_segments) > 20:
                    print(f"  ⚡ Moderate backlog - consider faster ETL")
            else:
                print(f"  ✓ No backlog - ETL is caught up")
        else:
            print(f"❌ Ready directory missing: {ready_dir}")
        
        # Check processing segments
        if processing_dir.exists():
            processing_segments = list(processing_dir.glob("segment_*.ndjson"))
            print(f"\n✓ Processing directory: {processing_dir}")
            
            if processing_segments:
                print(f"  Segments: {len(processing_segments)}")
                for seg in processing_segments:
                    age_sec = datetime.now().timestamp() - seg.stat().st_mtime
                    age_min = int(age_sec / 60)
                    print(f"  - {seg.name} (age: {age_min} min)")
                    
                    if age_min > 10:
                        print(f"    ⚠️  Stuck in processing? ETL may have crashed")
            else:
                print(f"  ✓ No segments in processing")
        else:
            print(f"❌ Processing directory missing: {processing_dir}")
    
    # Configuration check
    print(f"\n⚙️  Configuration")
    print("-" * 80)
    print(f"Segment max size: {config.ingestion.segment_max_mb} MB")
    print(f"Batch size: {config.ingestion.batch_size}")
    print(f"Flush interval: {config.ingestion.flush_interval_seconds}s")
    print(f"Delete after processing: {config.etl.delete_after_processing}")
    
    # Recommendations
    print(f"\n💡 Recommendations")
    print("-" * 80)
    
    ready_dir = base_dir / "ready" / "coinbase"
    if ready_dir.exists():
        ready_count = len(list(ready_dir.glob("segment_*.ndjson")))
        
        if ready_count == 0:
            print("✓ System healthy - no backlog")
        elif ready_count < 10:
            print("✓ System healthy - small backlog")
        elif ready_count < 20:
            print("⚡ Consider running ETL watcher more frequently")
            print("  python scripts/run_etl_watcher.py --poll-interval 15")
        else:
            print("⚠️  Large backlog detected!")
            print("  1. Start continuous ETL watcher:")
            print("     python scripts/run_etl_watcher.py --poll-interval 10")
            print("  2. Or manually process backlog:")
            print("     python scripts/run_etl.py --source coinbase --mode all")
    
    print("\n" + "=" * 80)


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="FluxForge Health Check")
    parser.add_argument(
        "--config",
        type=str,
        help="Path to config file (default: config/config.yaml)"
    )
    
    args = parser.parse_args()
    check_health(args.config)
