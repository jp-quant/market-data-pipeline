"""Local storage utilities."""
import os
import shutil
from pathlib import Path
from typing import Dict, Any


def get_disk_usage(path: str) -> Dict[str, Any]:
    """
    Get disk usage statistics for a path.
    
    Args:
        path: Path to check
        
    Returns:
        Dictionary with total, used, free bytes and GB
    """
    usage = shutil.disk_usage(path)
    
    return {
        "total_bytes": usage.total,
        "used_bytes": usage.used,
        "free_bytes": usage.free,
        "total_gb": round(usage.total / (1024**3), 2),
        "used_gb": round(usage.used / (1024**3), 2),
        "free_gb": round(usage.free / (1024**3), 2),
        "percent_used": round((usage.used / usage.total) * 100, 1),
    }


def get_directory_size(path: str) -> int:
    """
    Get total size of a directory in bytes.
    
    Args:
        path: Directory path
        
    Returns:
        Total size in bytes
    """
    total = 0
    for root, dirs, files in os.walk(path):
        for file in files:
            file_path = os.path.join(root, file)
            if os.path.exists(file_path):
                total += os.path.getsize(file_path)
    return total


def ensure_directory(path: str):
    """
    Ensure directory exists, creating if necessary.
    
    Args:
        path: Directory path
    """
    Path(path).mkdir(parents=True, exist_ok=True)


def list_files_by_date(directory: str, extension: str = ".ndjson") -> list:
    """
    List files in directory sorted by date.
    
    Args:
        directory: Directory to search
        extension: File extension to filter
        
    Returns:
        List of file paths sorted by name (date)
    """
    path = Path(directory)
    if not path.exists():
        return []
    
    files = sorted(path.glob(f"*{extension}"))
    return [str(f) for f in files]
