"""Utility function for connecting to BigQuery as a target destination.

This module provides:
- TargetBigQuery: class containing helper functions for authenticating and replacing or appending data to BigQuery.
"""
from __future__ import annotations

import logging
import traceback

import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account


class TargetBigQuery:
    """A data target helper class for BigQuery.

    Attributes
    ----------
    name : str
        The name of the target for logging purposes.
    service_account_file : str
        The path to the service account credentials file.

    Methods
    -------
    authenticate() -> bool
        Authenticate the BigQuery client with the provided service account file.
    replace_from_gcs(source_bucket:str, source_file_path:str, target_schema:str, target_table:str) -> bool
        Replace data on the specified table using data from the specified source file.
    append_from_gcs(source_bucket:str, source_file_path:str, target_schema:str, target_table:str) -> bool
        Append data on the specified table using data from the specified source file.
    replace_from_df(data:pd.DataFrame, target_schema:str, target_table: str, load_job_schema:str=None) -> bool
        Replace data on the specified table using data from the given DataFrame.
    append_from_df(data:pd.DataFrame, target_schema:str, target_table: str, load_job_schema:str) -> bool
        Append data on the specified table using data from the given DataFrame.
    """

    def __init__(self, name:str, service_account_file:str) -> None:
        """Initialize a BigQuery Target with the specified name and service account file path."""
        self.name = f"Target: (BigQuery) {name}"
        self.service_account_file = service_account_file
        self.credentials = None

    def authenticate(self) -> bool:
        """Authenticate the BigQuery client with the provided service account file."""
        if self.credentials is None:
            try:
                self.credentials = service_account.Credentials.from_service_account_file(self.service_account_file)
                return True
            except:
                logging.error(f"Failed to connect to {self.name}")
                traceback.print_exc()
                raise

                # We can raise false when we figure out a retry strategy to handle failed authentications
                # return False
        else:
            logging.info(f"Connection is already established for {self.name}")
            return True

    def replace_from_gcs(self, source_bucket:str, source_file_path:str, target_schema:str, target_table:str) -> bool:
        """Replace data on the specified table using data from the specified source file.

        Parameters
        ----------
        source_bucket : str
            The name of the source Google Cloud Storage bucket.
        source_file_path : str
            The file path of the source data file.
        target_schema : str
            The name of the schema containing the table where the data will be dumped.
        target_table : str
            The name of the table where the data will be dumped.

        Raises
        ------
        Unknown
            To be improved...
        """
        try:
            # TODO: Add checks to ensure source file is a parquet file
            # TODO: Add flexibility for other source file formats (arrow, avro, pickle, etc.)
            client = bigquery.Client(credentials=self.credentials, project=self.credentials.project_id)

            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.PARQUET,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
            )

            uri = f"gs://{source_bucket}/{source_file_path}"
            table_id = f"{target_schema}.{target_table}"

            load_job = client.load_table_from_uri(
                uri, table_id, job_config=job_config
            )

            load_job.result()

            return True
        except:
            logging.error(f"Failed to replace data in table {target_schema}.{target_table} at {self.name}")
            traceback.print_exc()
            # TODO: Improve error handling
            raise

            # return False

    def append_from_gcs(self, source_bucket:str, source_file_path:str, target_schema:str, target_table:str) -> bool:
        """Append data on the specified table using data from the specified source file.

        Parameters
        ----------
        source_bucket : str
            The name of the source Google Cloud Storage bucket.
        source_file_path : str
            The file path of the source data file.
        target_schema : str
            The name of the schema containing the table where the data will be dumped.
        target_table : str
            The name of the table where the data will be dumped.

        Raises
        ------
        Unknown
            To be improved...
        """
        try:
            client = bigquery.Client(credentials=self.credentials, project=self.credentials.project_id)

            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.PARQUET,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND
            )

            uri = f"gs://{source_bucket}/{source_file_path}"
            table_id = f"{target_schema}.{target_table}"

            load_job = client.load_table_from_uri(
                uri, table_id, job_config=job_config
            )

            load_job.result()

            return True
        except:
            logging.error(f"Failed to replace data in table {target_schema}.{target_table} at {self.name}")
            traceback.print_exc()
            # TODO: Improve error handling
            raise

            # return False

    def replace_from_df(
        self,
        data:pd.DataFrame,
        target_schema:str,
        target_table: str,
        load_job_schema:str|None=None
    ) -> bool:
        """Replace data on the specified table using data from the given DataFrame.

        Parameters
        ----------
        data : pandas.DataFrame
            The source DataFrame to be ingested.
        target_schema : str
            The name of the schema containing the table where the data will be dumped.
        target_table : str
            The name of the table where the data will be dumped.
        load_job_schema: str, optional
            The schema of the DataFrame, is None by default.
            The schema is of the form "col_name:TYPE,col_name2:TYPE2."

        Raises
        ------
        Unknown
            To be improved...
        """
        try:
            client = bigquery.Client(credentials=self.credentials, project=self.credentials.project_id)

            job_config = bigquery.LoadJobConfig(
                # source_format=bigquery.SourceFormat.PARQUET,
                # skip_leading_rows=1,
                autodetect=True,
                write_disposition="WRITE_TRUNCATE",
                allow_quoted_newlines=True
            )

            if load_job_schema:
                columns = [ col.split(":") for col in load_job_schema.split(",")]
                schema = [bigquery.SchemaField(col_name, dtype) for col_name, dtype in columns]
                job_config.schema = schema

            table_id = f"{target_schema}.{target_table}"
            job = client.load_table_from_dataframe(dataframe=data, destination=table_id, job_config=job_config)

            job.result()

            return True
        except:
            logging.error(f"Failed to replace data in table {table_id} at {self.name}")
            traceback.print_exc()
            # TODO: Improve error handling
            raise

            # return False

    def append_from_df(self, data:pd.DataFrame, target_schema:str, target_table: str, load_job_schema:str) -> bool:
        """Append data on the specified table using data from the given DataFrame.

        Parameters
        ----------
        data : pandas.DataFrame
            The source DataFrame to be ingested.
        target_schema : str
            The name of the schema containing the table where the data will be dumped.
        target_table : str
            The name of the table where the data will be dumped.
        load_job_schema: str, optional
            The schema of the DataFrame, is None by default.
            The schema is of the form "col_name:TYPE,col_name2:TYPE2."

        Raises
        ------
        Unknown
            To be improved...
        """
        try:
            client = bigquery.Client(credentials=self.credentials, project=self.credentials.project_id)


            job_config = bigquery.LoadJobConfig(
                # source_format=bigquery.SourceFormat.PARQUET,
                # skip_leading_rows=1,
                autodetect=True,
                write_disposition="WRITE_APPEND"
            )

            if load_job_schema:
                columns = [ col.split(":") for col in load_job_schema.split(",")]
                schema = [bigquery.SchemaField(col_name, dtype) for col_name, dtype in columns]
                job_config.schema = schema
            table_id = f"{target_schema}.{target_table}"
            job = client.load_table_from_dataframe(dataframe=data, destination=table_id, job_config=job_config)

            job.result()
            return True
        except:
            logging.error(f"Failed to append data to table {table_id} at {self.name}")
            traceback.print_exc()
            # TODO: Improve error handling
            raise

            # return False
