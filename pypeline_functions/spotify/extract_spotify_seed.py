#!/usr/bin/env python

import argparse

from pypeline_functions.utils.storage import GoogleCloudStorage


def extract_spotify_seed(landing_bucket_name: str, landing_prefix: str, data_type: str) -> None:
    """Run the extraction pipeline for the Spotify data seed."""
    gcs = GoogleCloudStorage()

    prefix_filter = f"spotify/{data_type}"

    if landing_prefix == "":
        blob_paths = gcs.extract_zip_files("data-seeds", prefix_filter, landing_bucket_name)
    else:
        blob_paths = gcs.extract_zip_files("data-seeds", prefix_filter, landing_bucket_name, landing_prefix)

    print(blob_paths)


def main() -> None:  # noqa: D103
    parser = argparse.ArgumentParser(
        description="Extracts the spotify data seed (.zip) files and dumps them into another bucket \
            with all .json files converted to .jsonl"
    )
    parser.add_argument(
        "--data_type",
        type=str,
        choices=["account_data", "streaming_history"],
        required=True,
        help="the data type to extract. either the account data or the extended streaming history",
    )
    parser.add_argument(
        "--landing_bucket_name", type=str, required=True, help="name of the bucket to store the extracted data seeds"
    )
    parser.add_argument(
        "--landing_prefix",
        nargs="?",
        type=str,
        default="",
        help="prefix path location where the extract will be stored. \
            if undeclared it will use the same prefix path as the source",
    )

    args = parser.parse_args()

    extract_spotify_seed(args.landing_bucket_name, args.landing_prefix, args.data_type)


if __name__ == "__main__":
    main()
