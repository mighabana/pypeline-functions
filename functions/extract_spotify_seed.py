#!/usr/bin/env python

from utils.google_cloud_storage import GoogleCloudStorage

if __name__ == "__main__":

    import argparse

    parser = argparse.ArgumentParser(
        description="Extracts the spotify data seed (.zip) files and dumps them into another bucket \
            with all .json files converted to .jsonl"
    )
    parser.add_argument(
        "--data_type", type=str, choices=["account_data", "streaming_history"], required=True,
        help="the data type to extract. either the account data or the extended streaming history"
    )
    parser.add_argument(
        "--landing_bucket_name", type=str, required=True,
        help="name of the bucket to store the extracted data seeds"
    )
    parser.add_argument(
        "--landing_prefix", nargs="?", type=str, default="",
        help="prefix path location where the extract will be stored. \
            if undeclared it will use the same prefix path as the source"
    )

    args = parser.parse_args()

    landing_bucket_name = args.landing_bucket_name
    landing_prefix = args.landing_prefix

    gcs = GoogleCloudStorage()

    prefix_filter = f"spotify/{args.data_type}"

    if landing_prefix == "":
        blob_paths = gcs.extract_zip_files("data-seeds", prefix_filter, args.landing_bucket_name)
        gcs.convert_json_to_jsonl(args.landing_bucket_name, prefix_filter)
    else:
        blob_paths = gcs.extract_zip_files("data-seeds", prefix_filter, args.landing_bucket_name, args.landing_prefix)
        gcs.convert_json_to_jsonl(args.landing_bucket_name, args.landing_prefix)
