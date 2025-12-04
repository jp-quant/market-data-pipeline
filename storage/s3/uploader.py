"""S3 storage manager for FluxForge.

Provides seamless S3 integration for both ingestion and ETL layers:
- Direct S3 writes using boto3
- S3FS integration for Polars/Pandas compatibility
- Atomic operations with temp prefixes
- Credential management (environment, role-based)
- Multi-region support
"""
import logging
import boto3
import s3fs
from pathlib import Path
from typing import Optional, Dict, Any, List, BinaryIO
from botocore.exceptions import ClientError, NoCredentialsError
import io

logger = logging.getLogger(__name__)


class S3StorageManager:
    """
    Unified S3 storage manager for FluxForge.
    
    Features:
    - boto3 client for management operations
    - s3fs for seamless Polars/Pandas integration
    - Automatic credential detection (environment, IAM role)
    - Multi-region support
    - Atomic writes with temp prefixes
    
    Example:
        # Initialize with auto-detected credentials
        s3 = S3StorageManager(
            bucket="my-data-lake",
            prefix="fluxforge"
        )
        
        # Write directly
        s3.write_bytes(b"data", "raw/coinbase/segment.ndjson")
        
        # Get s3fs filesystem for Polars/Pandas
        fs = s3.get_filesystem()
        df.write_parquet(f"s3://{s3.bucket}/{s3.prefix}/processed/ticker/data.parquet", 
                         storage_options=s3.get_storage_options())
    """
    
    def __init__(
        self,
        bucket: str,
        prefix: str = "",
        region: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        endpoint_url: Optional[str] = None,
        s3fs_kwargs: Optional[Dict[str, Any]] = None,
    ):
        """
        Initialize S3 storage manager.
        
        Args:
            bucket: S3 bucket name
            prefix: Base prefix for all operations (e.g., "fluxforge" or "prod/fluxforge")
            region: AWS region (auto-detected if None)
            aws_access_key_id: AWS access key (uses environment/role if None)
            aws_secret_access_key: AWS secret key (uses environment/role if None)
            aws_session_token: AWS session token (for temporary credentials)
            endpoint_url: Custom S3 endpoint (for S3-compatible services)
            s3fs_kwargs: Additional kwargs for s3fs.S3FileSystem
        """
        self.bucket = bucket
        self.prefix = prefix.strip("/") if prefix else ""
        self.region = region
        self.endpoint_url = endpoint_url
        
        # Build boto3 session kwargs
        session_kwargs = {}
        if region:
            session_kwargs["region_name"] = region
        
        # Build boto3 client kwargs
        client_kwargs = {}
        if aws_access_key_id and aws_secret_access_key:
            client_kwargs["aws_access_key_id"] = aws_access_key_id
            client_kwargs["aws_secret_access_key"] = aws_secret_access_key
            if aws_session_token:
                client_kwargs["aws_session_token"] = aws_session_token
        if endpoint_url:
            client_kwargs["endpoint_url"] = endpoint_url
        
        # Initialize boto3 session and client
        self.session = boto3.Session(**session_kwargs)
        self.s3_client = self.session.client("s3", **client_kwargs)
        self.s3_resource = self.session.resource("s3", **client_kwargs)
        
        # Build s3fs kwargs
        self.s3fs_kwargs = s3fs_kwargs or {}
        if aws_access_key_id and aws_secret_access_key:
            self.s3fs_kwargs["key"] = aws_access_key_id
            self.s3fs_kwargs["secret"] = aws_secret_access_key
            if aws_session_token:
                self.s3fs_kwargs["token"] = aws_session_token
        if endpoint_url:
            self.s3fs_kwargs["client_kwargs"] = {"endpoint_url": endpoint_url}
        
        # Initialize s3fs filesystem
        self.fs = s3fs.S3FileSystem(**self.s3fs_kwargs)
        
        # Test connection
        try:
            self.s3_client.head_bucket(Bucket=bucket)
            logger.info(
                f"[S3StorageManager] Connected to s3://{bucket}/{prefix} "
                f"(region: {self._get_bucket_region()})"
            )
        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code == "404":
                raise ValueError(f"Bucket '{bucket}' does not exist")
            elif error_code == "403":
                raise PermissionError(f"Access denied to bucket '{bucket}'")
            else:
                raise
        except NoCredentialsError:
            raise ValueError(
                "No AWS credentials found. Set AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY "
                "or configure IAM role."
            )
    
    def get_full_key(self, relative_key: str) -> str:
        """Get full S3 key including prefix."""
        relative_key = relative_key.strip("/")
        if self.prefix:
            return f"{self.prefix}/{relative_key}"
        return relative_key
    
    def get_full_path(self, relative_key: str) -> str:
        """Get full S3 path (s3://bucket/prefix/key)."""
        full_key = self.get_full_key(relative_key)
        return f"s3://{self.bucket}/{full_key}"
    
    def write_bytes(
        self,
        data: bytes,
        key: str,
        metadata: Optional[Dict[str, str]] = None,
        content_type: Optional[str] = None,
    ) -> str:
        """
        Write bytes to S3.
        
        Args:
            data: Bytes to write
            key: Relative key (prefix will be prepended)
            metadata: Optional metadata dict
            content_type: Optional content type
        
        Returns:
            Full S3 path (s3://bucket/prefix/key)
        """
        full_key = self.get_full_key(key)
        
        put_kwargs = {"Body": data}
        if metadata:
            put_kwargs["Metadata"] = metadata
        if content_type:
            put_kwargs["ContentType"] = content_type
        
        try:
            self.s3_client.put_object(
                Bucket=self.bucket,
                Key=full_key,
                **put_kwargs
            )
            
            full_path = self.get_full_path(key)
            logger.debug(
                f"[S3StorageManager] Wrote {len(data)} bytes to {full_path}"
            )
            return full_path
        
        except ClientError as e:
            logger.error(f"[S3StorageManager] Failed to write to S3: {e}")
            raise
    
    def write_file(
        self,
        local_path: Path,
        key: str,
        metadata: Optional[Dict[str, str]] = None,
    ) -> str:
        """
        Upload local file to S3.
        
        Args:
            local_path: Local file path
            key: Relative S3 key
            metadata: Optional metadata
        
        Returns:
            Full S3 path
        """
        full_key = self.get_full_key(key)
        
        try:
            extra_args = {}
            if metadata:
                extra_args["Metadata"] = metadata
            
            self.s3_client.upload_file(
                str(local_path),
                self.bucket,
                full_key,
                ExtraArgs=extra_args if extra_args else None
            )
            
            full_path = self.get_full_path(key)
            file_size = local_path.stat().st_size
            logger.info(
                f"[S3StorageManager] Uploaded {local_path.name} ({file_size / 1024:.1f} KB) "
                f"to {full_path}"
            )
            return full_path
        
        except ClientError as e:
            logger.error(f"[S3StorageManager] Failed to upload file: {e}")
            raise
    
    def read_bytes(self, key: str) -> bytes:
        """
        Read bytes from S3.
        
        Args:
            key: Relative S3 key
        
        Returns:
            File contents as bytes
        """
        full_key = self.get_full_key(key)
        
        try:
            response = self.s3_client.get_object(Bucket=self.bucket, Key=full_key)
            return response["Body"].read()
        
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                raise FileNotFoundError(f"S3 key not found: {full_key}")
            raise
    
    def exists(self, key: str) -> bool:
        """
        Check if S3 key exists.
        
        Args:
            key: Relative S3 key
        
        Returns:
            True if exists
        """
        full_key = self.get_full_key(key)
        
        try:
            self.s3_client.head_object(Bucket=self.bucket, Key=full_key)
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
            raise
    
    def delete(self, key: str):
        """
        Delete S3 object.
        
        Args:
            key: Relative S3 key
        """
        full_key = self.get_full_key(key)
        
        try:
            self.s3_client.delete_object(Bucket=self.bucket, Key=full_key)
            logger.debug(f"[S3StorageManager] Deleted {full_key}")
        except ClientError as e:
            logger.error(f"[S3StorageManager] Failed to delete: {e}")
            raise
    
    def list_objects(
        self,
        prefix: str = "",
        suffix: str = "",
        recursive: bool = True,
    ) -> List[Dict[str, Any]]:
        """
        List objects in S3.
        
        Args:
            prefix: Additional prefix to filter (appended to base prefix)
            suffix: File extension filter (e.g., ".ndjson")
            recursive: List recursively
        
        Returns:
            List of dicts with 'Key', 'Size', 'LastModified'
        """
        full_prefix = self.get_full_key(prefix) if prefix else self.prefix
        if full_prefix and not full_prefix.endswith("/"):
            full_prefix += "/"
        
        paginator = self.s3_client.get_paginator("list_objects_v2")
        
        kwargs = {"Bucket": self.bucket, "Prefix": full_prefix}
        if not recursive:
            kwargs["Delimiter"] = "/"
        
        objects = []
        for page in paginator.paginate(**kwargs):
            if "Contents" in page:
                for obj in page["Contents"]:
                    if suffix and not obj["Key"].endswith(suffix):
                        continue
                    objects.append({
                        "Key": obj["Key"],
                        "Size": obj["Size"],
                        "LastModified": obj["LastModified"],
                    })
        
        return objects
    
    def get_filesystem(self) -> s3fs.S3FileSystem:
        """
        Get s3fs filesystem for Polars/Pandas integration.
        
        Returns:
            s3fs.S3FileSystem instance
        
        Example:
            fs = s3.get_filesystem()
            df = pl.read_parquet(f"s3://{s3.bucket}/{s3.prefix}/data.parquet", 
                                storage_options={"client": fs})
        """
        return self.fs
    
    def get_storage_options(self) -> Dict[str, Any]:
        """
        Get storage options dict for Polars/Pandas.
        
        Returns:
            Storage options compatible with Polars/Pandas
        
        Example:
            df.write_parquet("s3://bucket/key", storage_options=s3.get_storage_options())
        """
        return self.s3fs_kwargs
    
    def open(
        self,
        key: str,
        mode: str = "rb",
        **kwargs
    ):
        """
        Open S3 file using s3fs (returns file-like object).
        
        Args:
            key: Relative S3 key
            mode: File mode ('rb', 'wb', etc.)
            **kwargs: Additional s3fs open kwargs
        
        Returns:
            File-like object
        
        Example:
            with s3.open("data/file.txt", "wb") as f:
                f.write(b"data")
        """
        full_path = self.get_full_path(key)
        return self.fs.open(full_path, mode, **kwargs)
    
    def _get_bucket_region(self) -> str:
        """Get bucket region."""
        try:
            response = self.s3_client.get_bucket_location(Bucket=self.bucket)
            location = response.get("LocationConstraint")
            return location if location else "us-east-1"
        except Exception:
            return "unknown"
    
    def get_stats(self) -> Dict[str, Any]:
        """Get storage statistics."""
        return {
            "bucket": self.bucket,
            "prefix": self.prefix,
            "region": self._get_bucket_region(),
            "full_path": f"s3://{self.bucket}/{self.prefix}" if self.prefix else f"s3://{self.bucket}",
        }
