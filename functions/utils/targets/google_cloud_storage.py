"""Utility function for connecting to Google Cloud Storage (GCS) as a target destination.

This module provides:
- TargetGoogleCloudStorage: class containing helper functions for authenticating, uploading and managing files in GCS.
"""

import glob
import logging
import os
import traceback

from google.cloud import storage
from google.oauth2 import service_account


class TargetGoogleCloudStorage:
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

    def __init__(self, name:str, service_account_file:str) -> None:
        self.name = f"Target: (Google Cloud Storage) {name}"
        self.service_account_file = service_account_file
        self.credentials = None


    def authenticate(self) -> bool:
        """Authenticate the Google Cloud Storage client with the provided service account file."""
        if self.credentials is None:
            try:
                self.credentials = service_account.Credentials.from_service_account_file(self.service_account_file)
                return True
            except:
                logging.error(f"Failed to connect to {self.name}")
                traceback.print_exc()
                raise

                # TODO: figure out a retry strategy to handle failed authentications
                return False
        else:
            logging.info(f"Connection is already established for {self.name}")
            return True

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
        try:
            client = storage.Client(credentials=self.credentials, project=self.credentials.project_id)

            bucket = client.bucket(bucket_name)
            blob = bucket.blob(file_path)
            blob.upload_from_filename(source_file)
            return True
        except:
            logging.error(f"Failed to upload {source_file} to Google Cloud Storage, with destination \
                {bucket}/{file_path}.")
            traceback.print_exc()
            raise

    def get_list_blob_files(self, bucket_name: str, blob_name: str) -> list:
        """Fetch all blob files within a GCS bucket.

        Parameters
        ----------
        bucket_name : str
            The name of the bucket that the file will be uploaded in.
        blob_name : str
            The name of the blob to search.
        """
        try:
            storage_client = storage.Client(credentials=self.credentials)
            blobs = storage_client.list_blobs(bucket_name)
            # NOTE: Review what the matching_blobs is doing.
            # not sure if its actually filtering or taking all blobs within a bucket.
            matching_blobs = [blob.name for blob in blobs if blob_name in blob.name and blob.name != blob_name]
            logging.info(f"GCS blobs: {matching_blobs}")
            return matching_blobs

        # TODO: Improve exception handling.
        except Exception as e:
            logging.error(f"An error occurred while listing blobs: {e}")

    def upload_local_directory_to_gcs(self, directory_path: str, bucket_name: str, blob_name: str) -> None:
        """Upload a local directory and its contents to GCS at the specified destination.

        Parameters
        ----------
        directory_path : str
            The local path to the directory to be uploaded.
        bucket_name : str
            The name of the bucket that the file will be uploaded in.
        blob_name : str
            The name of the blob where the files in the directory will be uploaded.
        """
        try:
            storage_client = storage.Client(credentials=self.credentials)
            bucket = storage_client.bucket(bucket_name)

            assert os.path.isdir(directory_path)
            for local_file in glob.glob(directory_path + "/**"):
                filename=local_file.split("/")[-1]
                blob = bucket.blob(blob_name+filename)
                blob.upload_from_filename(local_file)
            logging.info("Local files uploaded to GCS bucket successfully.")
        except Exception as e:
            logging.error(f"An error occurred during the upload: {e}")

    def delete_files_in_blob(self, bucket_name: str, blob_name: str) -> None:
        """Delete files inside the specified blob directory in the GCS bucket.

        Parameters
        ----------
        bucket_name : str
            The name of the bucket that the file will be uploaded in.
        blob_name : str
            The name of the blob to be deleted.
        """
        try:
            storage_client = storage.Client(credentials=self.credentials)
            bucket = storage_client.get_bucket(bucket_name)

            # List all objects in the specified blob directory
            blobs = bucket.list_blobs(prefix=blob_name)

            for blob in blobs:
                # Check if the blob is a file (not a directory)
                if not blob.name.endswith("/"):
                    blob.delete()
                    logging.info(f"Deleted blob: {blob.name}")
            logging.info("Files inside the specified GCS blob directory have been deleted successfully.")

        except Exception as e:
            logging.error(f"An error occurred while deleting files in the blob: {e}")
