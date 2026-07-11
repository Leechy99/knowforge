"""
MinIO S3-Compatible Storage Client
"""
import io
from datetime import datetime
from typing import Any

import boto3
from botocore.config import Config


class MinioFileStore:
    BUCKET_NAME = "knowforge"

    def __init__(
        self,
        endpoint: str = "localhost:9000",
        access_key: str = "",
        secret_key: str = "",
        secure: bool = False,
        bucket_name: str = BUCKET_NAME,
    ):
        self.bucket_name = bucket_name
        self.client = boto3.client(
            "s3",
            endpoint_url=f"{'https' if secure else 'http'}://{endpoint}",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=Config(signature_version="s3v4"),
        )

    def ensure_bucket(self):
        try:
            self.client.head_bucket(Bucket=self.bucket_name)
        except:
            self.client.create_bucket(Bucket=self.bucket_name)

    def upload_raw_file(self, key: str, content: bytes, metadata: dict[str, str] | None = None):
        self.client.put_object(
            Bucket=self.bucket_name,
            Key=f"raw/{key}",
            Body=content,
            Metadata=metadata or {},
        )

    def upload_processed_file(self, key: str, content: str, format: str):
        self.client.put_object(
            Bucket=self.bucket_name,
            Key=f"processed/{key}.{format}",
            Body=content.encode("utf-8"),
            ContentType=f"text/{format}",
        )

    def download_file(self, key: str) -> bytes:
        response = self.client.get_object(Bucket=self.bucket_name, Key=key)
        return response["Body"].read()

    def list_files(self, prefix: str = "", max_keys: int = 100) -> list[str]:
        response = self.client.list_objects_v2(
            Bucket=self.bucket_name,
            Prefix=prefix,
            MaxKeys=max_keys,
        )
        return [obj["Key"] for obj in response.get("Contents", [])]
