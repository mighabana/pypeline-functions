import os
import tempfile
import urllib.parse
from collections.abc import Generator
from datetime import UTC, datetime

import boto3
import polars as pl
import pyarrow.parquet as pq
from botocore.client import BaseClient

from infolio.utils.logger import get_logger

logger = get_logger(__name__)

class S3:
    """
    A wrapper around boto3 S3 client to simplify common operations.

    Parameters
    ----------
    aws_access_key_id : str | None
        AWS access key ID. If None, boto3 will use default credential resolution.
    aws_secret_access_key : str | None
        AWS secret access key. If None, boto3 will use default credential resolution.
    aws_session_token : str | None
        AWS session token for temporary credentials.
    region_name : str | None
        AWS region name.
    """

    def __init__(
        self,
        endpoint_url: str | None = None,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
        aws_session_token: str | None = None,
        region_name: str | None = None,
    ) -> None:
        self.client = boto3.client(
            "s3",
            endpoint_url=endpoint_url or os.getenv("CONNECTOR__S3__ENDPOINT_URL", None),
            aws_access_key_id=aws_access_key_id or os.getenv("CONNECTOR__S3__ACCESS_KEY_ID", None),
            aws_secret_access_key=aws_secret_access_key or os.getenv("CONNECTOR__S3__SECRET_ACCESS_KEY", None),
            aws_session_token=aws_session_token or os.getenv("CONNECTOR__S3__SESSION_TOKEN", None),
            region_name=region_name or os.getenv("CONNECTOR__S3__REGION_NAME", None),
        )

    def upload(
        self,
        data: Generator[pl.DataFrame, None, None],
        bucket_name: str,
        path_prefix: str,
        filename: str|None=None,
        tags: dict[str, str]|None=None
    ) -> str:
        """
        Upload a generator of Polars DataFrames to S3 as a single Parquet file.

        The method consumes the generator in sequence, writing each DataFrame
        to a temporary Parquet file. Once all partitions are written, the file is
        uploaded to the specified S3 bucket and path. A timestamp-based filename
        will be used if no custom filename is provided. The temporary file is
        automatically deleted after upload.

        Parameters
        ----------
        data : Generator[pl.DataFrame, None, None]
            Generator yielding Polars DataFrame partitions.
        bucket_name : str
            Name of the S3 bucket where the file will be uploaded.
        path_prefix : str
            Path prefix (folder) in the S3 bucket where the file will be stored.
        filename : str, optional
            Custom filename for the uploaded file. Must end with `.parquet`.
            If not provided, a UTC timestamp in the format ``%Y%m%dT%H%M%S.parquet``
            will be used.

        Returns
        -------
        str
            The S3 URI of the uploaded file in the format:
            ``s3://{bucket_name}/{path_prefix}/{filename}``

        Raises
        ------
        ValueError
            If a custom filename is provided that does not end with `.parquet`.
        StopIteration
            If the generator yields no data.

        Notes
        -----
        - The generator is fully consumed during this process.
        - The temporary Parquet file is removed after upload.
        """
        if filename is None:
            timestamp = datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%S")
            filename = f"{timestamp}.parquet"
        elif ".parquet" not in filename:
            logger.error("The default upload function expects the file to be uploaded as a .parquet file.")
            raise ValueError

        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp_file:
            tmp_file_name = tmp_file.name

        try:
            first_chunk = next(data)
            with pq.ParquetWriter(tmp_file_name, first_chunk.to_arrow().schema) as writer:
                writer.write_table(first_chunk.to_arrow())
                for chunk in data:
                    writer.write_table(chunk.to_arrow())

            s3_key = f"{path_prefix.strip('/')}/{filename}"
            if tags:
                encoded_tags = urllib.parse.urlencode(tags)
                self.client.upload_file(tmp_file_name, bucket_name, s3_key, ExtraArgs={"Tagging": encoded_tags})
            else:
                self.client.upload_file(tmp_file_name, bucket_name, s3_key)

            logger.info(f"Uploaded to S3: s3://{bucket_name}/{s3_key}")
            return f"s3://{bucket_name}/{s3_key}"
        finally:
            if os.path.exists(tmp_file_name):
                os.unlink(tmp_file_name)


    def upload_file(
        self,
        local_path: str,
        bucket: str,
        key: str | None = None,
        use_timestamp: bool = False,
        tags : dict[str, str] | None = None
    ) -> str:
        """
        Upload a local file to S3.

        Parameters
        ----------
        local_path : str
            Path to the local file to upload.
        bucket : str
            Target S3 bucket name.
        key : str | None
            Target S3 key. If None, uses the file name.
        use_timestamp : bool
            If True, prepends the current timestamp to the key.

        Returns
        -------
        str
            The S3 key of the uploaded file.
        """
        key = key or os.path.basename(local_path)
        if use_timestamp:
            timestamp = datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%S")
            key = f"{timestamp}_{key}"

        if tags:
            encoded_tags = urllib.parse.urlencode(tags)
            self.client.upload_file(local_path, bucket, key, ExtraArgs={"Tagging": encoded_tags})
        else:
            self.client.upload_file(local_path, bucket, key)

        return key

    def upload_bytes(
        self,
        data: bytes,
        bucket: str,
        key: str,
        use_timestamp: bool = False,
        tags : dict[str, str] | None = None
    ) -> str:
        """
        Upload bytes data to S3.

        Parameters
        ----------
        data : bytes
            Data to upload.
        bucket : str
            Target S3 bucket.
        key : str
            Target S3 key.
        use_timestamp : bool
            If True, prepends the current timestamp to the key.

        Returns
        -------
        str
            The S3 key of the uploaded object.
        """
        if use_timestamp:
            timestamp = datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%S")
            key = f"{timestamp}_{key}"

        if tags:
            encoded_tags = urllib.parse.urlencode(tags)
            self.client.put_object(Bucket=bucket, Key=key, Body=data, Tagging=encoded_tags)
        else:
            self.client.put_object(Bucket=bucket, Key=key, Body=data)
        return key

    def download_file(self, bucket: str, key: str, local_path: str) -> None:
        """Download a file from S3 to local path."""
        self.client.download_file(bucket, key, local_path)

    def list_objects(self, bucket: str, prefix: str | None = None) -> list[str]:
        """
        List object keys in a bucket optionally filtered by prefix.

        Parameters
        ----------
        bucket : str
            S3 bucket name.
        prefix : str | None
            Optional prefix filter.

        Returns
        -------
        List[str]
            List of object keys.
        """
        paginator = self.client.get_paginator("list_objects_v2")
        keys = []
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix or ""):
            contents = page.get("Contents", [])
            keys.extend([item["Key"] for item in contents])
        return keys

    def get_object_bytes(self, bucket: str, key: str) -> bytes:
        """
        Retrieve an object from S3 as bytes.

        Parameters
        ----------
        bucket : str
            S3 bucket name.
        key : str
            Object key.

        Returns
        -------
        bytes
            Object data.
        """
        response = self.client.get_object(Bucket=bucket, Key=key)
        return response["Body"].read()

    def s3_client(self) -> BaseClient:
        """Return the raw boto3 client for advanced operations."""
        return self.client
