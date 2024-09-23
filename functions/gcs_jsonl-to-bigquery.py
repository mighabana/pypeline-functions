#!/usr/bin/env python

import dlt
from dlt.sources.filesystem import filesystem, read_jsonl


def pipeline_run(
    bucket_name:str,
    prefix_filter:str,
    pipeline_name: str,
    table_name:str,
    dataset_name:str
) -> None:
    """Run the GCS-jsonl to bigquery pipeline."""
    # TODO: figure out how to handle merging multiple extracts and deduping data
    # probably a feature within dlt that I still have to learn
    files = filesystem(bucket_url=f"gs://{bucket_name}", file_glob=f"{prefix_filter}*.jsonl")
    # NOTE: probably better to filter the files to remove some files that are not as important

    reader = (files | read_jsonl()).with_name(table_name)
    pipeline = dlt.pipeline(pipeline_name=pipeline_name, dataset_name=dataset_name, destination="bigquery")

    # NOTE: learn how dlt manages the auto schema from jsonl...
    # the output tables are quite gross imo but it could also be user error
    info = pipeline.run(reader)
    print(info)

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Transfers data from a .jsonl file on GCS to BigQuery using dlt")
    parser.add_argument(
        "--bucket_name", type=str, required=True,
        help="name of the bucket where the .jsonl files are stored"
    )
    parser.add_argument(
        "--prefix_filter", type=str, required=True,
        help="prefix path location where the .jsonl files are stored"
    )
    parser.add_argument(
        "--pipeline_name", type=str, required=True,
        help="name of the pipeline"
    )
    parser.add_argument(
        "--table_name", type=str, required=True,
        help="name of the table where the data will be loaded"
    )
    parser.add_argument(
        "--dataset_name", type=str, required=True,
        help="name of the dataset where the data will be loaded"
    )

    args = parser.parse_args()

    pipeline_run(
        args.bucket_name,
        args.prefix_filter,
        args.pipeline_name,
        args.table_name,
        args.dataset_name
    )
