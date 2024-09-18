#!/usr/bin/env python

"""Executes the pipeline to extract data from MSSQL and uploads it as a parquet file in Google Cloud Storage."""

from __future__ import annotations

import os

import fastparquet as fp
import pandas as pd
from utils.sources.mssql import SourceMSSQL
from utils.targets.google_cloud_storage import TargetGoogleCloudStorage


def execute(
        source_name:str,
        host_name:str,
        db_name:str,
        username:str,
        password:str,
        port:int,
        source_table:str,
        target_bucket:str,
        target_file_path:str,
        source_fields:list|None=None,
        batch_size:int=10000,
        ssh_host:str="127.0.0.1",
        ssh_username:str="root",
        ssh_pkey:str="~/.ssh/id_ed25519",
        ssh_pkey_password:str=""
    ) -> None:
    """Execute the MSSQL to Google Cloud Storage pipeline.

    Parameters
    ----------
    source_name : str
        The name of the source for logging purposes.
    host_name : str
        The host address of the MSSQL database.
    db_name : str
        The database name of the MSSQL database to be accessed.
    username : str
        The username of the account to authenticate and access the specified databaase.
    password : str
        The password of the account to authenticate and access the specified database.
    port : int
        The port from which the database can be accessed.
    source_table : str
        The name of the table to be extracted.
    target_bucket : str
        The name of the bucket that the parquet file will be uploaded in.
    target_file_path : str
        The file path where the parquet file will be stored.
    source_fields : list, optional
        The list of fields to be extracted from the specified table, is ["*"] by default to select all fields.
    batch_size : int, optional
        The number of rows to be processed for each batch, is 10000 by default.
    ssh_host : str, optional
        The host address of the SSH tunnel, is "127.0.0.1" by default.
    ssh_username : str, optional
        The username to authenticate into the SSH tunnel, is "root" by default.
    ssh_pkey : str, optional
        The path of the ssh private key to authenticate into the SSH tunnel, is "~/.ssh/id_ed25519" by default.
    ssh_pkey_password : str, optional
        The password of the SSH private key being used to access the SSH tunnel, is "" by default.
    """
    # Added for B006 compliance (https://docs.astral.sh/ruff/rules/mutable-argument-default/)
    if source_fields is None:
        source_fields = ["*"]

    source = SourceMSSQL(
        name=source_name,
        host=host_name,
        db_name=db_name,
        username=username,
        password=password,
        port=port,
        driver="FreeTDS",
        tds_version="7.4",
        ssh_host=ssh_host,
        ssh_username=ssh_username,
        ssh_pkey=ssh_pkey,
        ssh_pkey_password=ssh_pkey_password
    )

    source.connect(tunnel=True)

    initial_iter = True
    schema = {}

    table_name= source_table if "." not in source_table else source_table.split(".")[1]
    file_name = f"{table_name}.parquet"
    while True:
        batch_list, schema = source.fetch(source_fields, source_table, batch_size)

        if len(batch_list) == 0:
            break

        df = pd.DataFrame(batch_list).astype(schema)

        if initial_iter:
            fp.write(
                f"../data/{file_name}",
                df,
                file_scheme="simple",
                write_index=False,
                times="int96",
                compression="SNAPPY",
                has_nulls=True
            )
            initial_iter = False
        else:
            fp.write(
                f"../data/{file_name}",
                df,
                file_scheme="simple",
                write_index=False,
                times="int96",
                compression="SNAPPY",
                has_nulls=True,
                append=True
            )

    source.disconnect()

    target = TargetGoogleCloudStorage(
        name=os.environ["TARGET_NAME"],
        service_account_file=os.environ["SERVICE_ACCOUNT_FILE"]
    )

    target.authenticate()
    target.upload(f"../data/{file_name}", target_bucket, target_file_path)

    os.remove(f"../data/{file_name}")

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Executes the pipeline to extract data from the MSSQL database and \
        saves it onto Google Cloud Storage as a parquet file")
    # TODO: Check how to reduce the number of arguments for this script
    # NOTE: `nargs=None` is redundant and uneccessary but I like explicitly including it to highlight its importance
    parser.add_argument("--source_name", nargs=None, type=str, required=True, help="custom name for the source to be \
        used for logging purposes")
    parser.add_argument("--host_name", nargs=None, type=str, required=True, help="host address of the MSSQL database")
    parser.add_argument("--db_name", nargs=None, type=str, required=True, help="database name of the MSSQL database to \
        be accessed")
    parser.add_argument("--username", nargs=None, type=str, required=True, help="username of the account to \
        authenticate and access the specified database")
    parser.add_argument("--password", nargs=None, type=str, required=True, help="password of the account to \
        authenticate and access the specified database")
    parser.add_argument("--port", nargs=None, type=int, required=True, help="port from which the database can be \
        accessed")
    parser.add_argument("--source_table", nargs=None, type=str, required=True, help="name of the source table")
    parser.add_argument("--target_bucket", nargs=None, type=str, required=True, help="name of the target Google Cloud \
        Storage bucket")
    parser.add_argument("--target_file_path", nargs=None, type=str, required=True, help="file path where the parquet \
        file will be saved")
    # optional arguments
    parser.add_argument("--source_fields", nargs="*", type=str, default=["*"], help="list of fields to be fetched from \
        the source table")
    parser.add_argument("--batch_size", nargs=None, type=int, default=10000, help="number of rows to be processed for \
        each batch")
    parser.add_argument("--ssh_host", nargs=None, type=str, default="127.0.0.1", help="host address of the SSH tunnel")
    parser.add_argument("--ssh_username", nargs=None, type=str, default="root", help="username to authenticate into \
        the SSH tunnel")
    parser.add_argument("--ssh_pkey", nargs=None, type=str, default="~/.ssh/id_ed25519", help="path of the ssh private \
        key to authenticate into the SSH tunnel")
    parser.add_argument("--ssh_pkey_password", nargs=None, type=str, default="", help="password of the SSH private key \
        being used to access the SSH tunnel")
    args = parser.parse_args()

    execute(
        source_name=args.source_name,
        host_name=args.host_name,
        db_name=args.db_name,
        username=args.username,
        password=args.password,
        port=args.port,
        source_table=args.source_table,
        target_bucket=args.target_bucket,
        target_file_path=args.target_file_path,
        source_fields=args.source_fields,
        batch_size=args.batch_size,
        ssh_host=args.ssh_host,
        ssh_username=args.ssh_username,
        ssh_pkey=args.ssh_pkey,
        ssh_pkey_password=args.ssh_pkey_password
    )
