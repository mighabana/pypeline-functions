#!/usr/bin/env python

"""Executes the pipeline to extract data from a parquet file stored on Google Cloud Storage and loads it onto the specified BigQuery table."""  # noqa: E501

from utils.targets.bigquery import TargetBigQuery


def execute(
        target_name:str,
        service_account_file:str,
        source_bucket:str,
        source_file_path:str,
        target_schema:str,
        target_table:str
    ) -> None:
    """Execute the Google Cloud Storage to BigQuery pipeline.

    Parameters
    ----------
    target_name : str
        The name of the target for logging purposes.
    service_account_file : str
        The path to the service account credentials file.
    source_bucket : str
        The name of the source Google Cloud Storage bucket.
    source_file_path : str
        The file path of the source data file.
    target_schema : str
        The name of the schema containing the table where the data will be dumped.
    target_table : str
        The name of the table where the data will be dumped.
    """
    bq = TargetBigQuery(
        name = target_name,
        service_account_file=service_account_file
    )

    bq.authenticate()

    bq.replace_from_gcs(
        source_bucket=source_bucket,
        source_file_path=source_file_path,
        target_schema=target_schema,
        target_table=target_table
    )

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Executes the pipeline to extract data from a parquet file stored on \
        Google Cloud Storage and loads it onto the specified BigQuery table")
    parser.add_argument("--target_name", nargs=None, type=str, required=True, help="custom name for the target to be \
        used for logging purposes")
    parser.add_argument("--service_account_file", nargs=None, type=str, required=True, help="file path to the service \
        account credentials file")
    parser.add_argument("--source_bucket", nargs=None, type=str, required=True, help="name of the source bucket on \
        Google Cloud Storage")
    parser.add_argument("--source_file_path", nargs=None, type=str, required=True, help="file path to the parquet file \
        to be loaded on Google Cloud Storage")
    parser.add_argument("--target_schema", nargs=None, type=str, required=True, help="name of the target schema for \
        the data to be saved onto")
    parser.add_argument("--target_table", nargs=None, type=str, required=True, help="name of the target table for the \
        data to be saved onto")
    args = parser.parse_args()

    execute(
        source_bucket=args.source_bucket,
        source_file_path=args.source_file_path,
        target_schema=args.target_schema,
        target_table=args.target_table
    )
