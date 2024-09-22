"""Utility function for connecting to Google Cloud Storage (GCS) as a target destination.

This module provides:
- TargetGoogleCloudStorage: class containing helper functions for authenticating, uploading and managing files in GCS.
"""

import logging
import re
import io
import json
from zipfile import ZipFile, is_zipfile

from google.cloud.storage import Client, transfer_manager


class GoogleCloudStorage:
    """A data target helper class for authenticating, uploading and managing files in Google Cloud Storage.

    Attributes
    ----------
    name : str
        The name of the target for logging purposes.
    service_account_file : str
        The path to the service account credentials file.

    Methods
    -------
    authenticate() -> bool
        Authenticate the Google Cloud Storage client with the provided service account file.
    upload(soure_file:str, bucket:str, file_path:str) -> bool
        Upload a single file to Google Cloud Storage at the specified destination.
    get_list_blob_files(bucket_name:str, blob_name: str) -> list
        Fetch all blob files within a GCS bucket.
    upload_local_directory_to_gcs(directory_path:str, bucket_name:str, blob_name:str) -> None
        Upload a local directory and its contents to GCS at the specified destination.
    delete_files_in_blob(bucket_name:str, blob_name:str) -> None
        Delete files inside the specified blob directory in the GCS bucket.
    """

    def __init__(self) -> None:
        self.client = Client()

# ----------------------------------- Upload -----------------------------------
# inspiration: https://cloud.google.com/storage/docs/uploading-objects

    def upload(self, source_file:str, bucket_name:str, file_path:str) -> bool:
        """Upload a single file to Google Cloud Storage at the specified destination.

        Parameters
        ----------
        source_file : str
            The file path of the file to be uploaded.
        bucket_name : str
            The name of the bucket that the file will be uploaded in.
        file_path : str
            The file path where the `source_file` will be stored in the specified bucket.
        """
        bucket = self.client.bucket(bucket_name)
        blob = bucket.blob(file_path)
        blob.upload_from_filename(source_file)

    def upload_many_blobs_with_transfer_manager(self, bucket_name:str, filenames:list[str], source_directory:str="", workers=8) -> None:
        """Upload every file in a list to a bucket, concurrently in a process pool."""
        
        bucket = self.client.bucket(bucket_name)

        results = transfer_manager.upload_many_from_filenames(
            bucket, filenames, source_directory=source_directory, max_workers=workers
        )

        for name, result in zip(filenames, results):
            # The results list is either `None` or an exception for each filename in
            # the input list, in order.

            if isinstance(result, Exception):
                print("Failed to upload {} due to exception: {}".format(name, result))
            else:
                print("Uploaded {} to {}.".format(name, bucket.name))

    def upload_directory_with_transfer_manager(self, bucket_name, source_directory, workers=8) -> None:
        """Upload every file in a directory, including all files in subdirectories."""

        from pathlib import Path

        bucket = self.client.bucket(bucket_name)

        # recursively get all files in `directory` as Path objects.
        directory_as_path_obj = Path(source_directory)
        paths = directory_as_path_obj.rglob("*")

        # filter so the list only includes files, not directories themselves.
        file_paths = [path for path in paths if path.is_file()]

        # these paths are relative to the current working directory. Next, make them relative to `directory`
        relative_paths = [path.relative_to(source_directory) for path in file_paths]

        # convert them all to strings.
        string_paths = [str(path) for path in relative_paths]

        # TODO: plan how to do logging
        # print("Found {} files.".format(len(string_paths)))

        # Start the upload.
        results = transfer_manager.upload_many_from_filenames(
            bucket, string_paths, source_directory=source_directory, max_workers=workers
        )

        for name, result in zip(string_paths, results):
            # The results list is either `None` or an exception for each filename in the input list, in order.

            if isinstance(result, Exception):
                logging.error("Failed to upload {} due to exception: {}".format(name, result))
                raise ExceptionGroup("File upload failed", result)
            else:
                logging.info("Uploaded {} to {}.".format(name, bucket.name))

# ----------------------------------- Delete -----------------------------------

    def detele_file(self, bucket_name:str, blob_name:str) -> None:
        """Deletes a blob from the bucket.
        
        Parameters
        ----------
        bucket_name : str
            The name of the bucket that the file will be uploaded in.
        blob_name : str
            The name of the blob to search.
        """
        bucket = self.client.bucket(bucket_name)
        blob = bucket.blob(blob_name)

        blob.delete()
        logging.info(f"Deleted blob: {blob_name}")

    def delete_files_with_prefix(self, bucket_name: str, prefix: str=None) -> None:
        """Use a batch request to delete a list of objects with the given prefix in a bucket.

        Parameters
        ----------
        bucket_name : str
            The name of the bucket that the file will be uploaded in.
        prefix : str
            The prefix of the object paths
        """
        blobs_to_delete = self.list_blobs_with_prefix(bucket_name, prefix)
        with self.client.batch():
            for blob in blobs_to_delete:
                blob.delete()
                logging.info(f"Deleted blob: {blob.name}")
            logging.info("Files inside the specified GCS blob directory have been deleted successfully.")
            return True


# ----------------------------------- List -----------------------------------

    def list_blob_files(self, bucket_name: str) -> list:
        """Fetch all blob files within a GCS bucket.

        Parameters
        ----------
        bucket_name : str
            The name of the bucket that the file will be uploaded in.
        """
        try:
            blobs = self.client.list_blobs(bucket_name)
            return blobs

        # TODO: Improve exception handling.
        except Exception as e:
            logging.error(f"An error occurred while listing blobs: {e}")

    def list_blobs_with_prefix(self, bucket_name, prefix) -> list:
        """Lists all the blobs in the bucket that begin with the prefix.
        """

        # Note: Client.list_blobs requires at least package version 1.17.0.
        blobs = self.client.list_blobs(bucket_name, prefix=prefix)

        return blobs

# ----------------------------------- Misc. -----------------------------------

    def extract_zip_files(self, bucket_name:str, prefix_filter:str, landing_bucket_name:str, landing_prefix:str=None) -> list[str]:
        """Extracts all the .zip files from a bucket that begin with the prefix and save them to another bucket.
        """
        bucket = self.client.get_bucket(bucket_name)
        
        landing_bucket = self.client.get_bucket(landing_bucket_name)

        blobs = self.list_blobs_with_prefix(bucket_name, prefix_filter)
        blob_paths = [blob.name for blob in blobs]

        # NOTE: we can't use batching because the payload must be less than 10MB (https://cloud.google.com/storage/docs/batch#overview)
        for blob_path in blob_paths:
            blob = bucket.blob(blob_path)
            zipbytes = io.BytesIO(blob.download_as_string())
            
            if is_zipfile(zipbytes):
                with ZipFile(zipbytes, 'r') as myzip:
                    for contentfilename in myzip.namelist():
                        contentfile = myzip.read(contentfilename)
                        if landing_prefix:
                            blob = landing_bucket.blob(landing_prefix.removesuffix("/") + "/" + contentfilename)
                        else:
                            blob = landing_bucket.blob(blob_path.removesuffix('.zip') + "/" + contentfilename)
                        blob.upload_from_string(contentfile)

        return blob_paths # list of zip files extracted

    def convert_json_to_jsonl(self, bucket_name:str, prefix_filter:str) -> None:
    
        bucket = self.client.get_bucket(bucket_name)

        landing_blobs = self.list_blobs_with_prefix(bucket_name, prefix_filter)
        landing_blob_paths = [blob.name for blob in landing_blobs]
        json_blobs = [p for p in landing_blob_paths if re.search(r'\.json$', p) is not None]

        for json_blob in json_blobs:
            blob = bucket.blob(json_blob)
            blob_content = blob.download_as_string().decode('utf-8', 'replace')
            data = json.loads(blob_content)
            jsonl_blob = bucket.blob(json_blob+"l")
            if isinstance(data, dict):
                jsonl_blob.upload_from_string(json.dumps(data))
            elif isinstance(data, list):
                content = ""
                for datum in data:
                    content = content + json.dumps(datum) + "\n"
                jsonl_blob.upload_from_string(content)