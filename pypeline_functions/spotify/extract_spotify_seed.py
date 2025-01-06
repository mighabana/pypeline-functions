#!/usr/bin/env python

import argparse

from pypeline_functions.utils.storage import GoogleCloudStorage, LocalStorage


def extract_spotify_seed_gcs(landing_bucket_name: str, landing_prefix: str, data_type: str) -> None:
    """Run the extraction pipeline for the Spotify data seed."""
    gcs = GoogleCloudStorage()

    prefix_filter = f"spotify/{data_type}"

    if landing_prefix == "":
        blob_paths = gcs.extract_zip_files("data-seeds", prefix_filter, landing_bucket_name)
    else:
        blob_paths = gcs.extract_zip_files("data-seeds", prefix_filter, landing_bucket_name, landing_prefix)

    print(blob_paths)


def extract_spotify_seed_local(file_path: str, destination_path: str) -> None:
    """Run the extraction pipeline for the Spotify data seed to local storage."""
    ls = LocalStorage()

    blob_paths = ls.extract_zip_files(file_path, destination_path)

    print(blob_paths)


def main() -> None:  # noqa: D103
    parser = argparse.ArgumentParser(description="Extracts the spotify seed (.zip) files.")

    subparsers = parser.add_subparsers(dest="storage_type", required=True)

    # Subcommand for GCS extraction
    gcs_parser = subparsers.add_parser("gcs", help="Extract to Google Cloud Storage")
    gcs_parser.add_argument(
        "--landing_bucket_name", type=str, required=True, help="name of the bucket to store the extracted data seeds"
    )
    gcs_parser.add_argument(
        "--landing_prefix",
        nargs="?",
        type=str,
        default="",
        help="prefix path location where the extract will be stored. \
            if undeclared it will use the same prefix path as the source",
    )

    # Subcommand for Local extraction
    local_parser = subparsers.add_parser("local", help="Extract to local storage")
    local_parser.add_argument("--file_path", type=str, required=True, help="Path to the the .zip file to extract")
    local_parser.add_argument(
        "--destination_path",
        type=str,
        required=True,
        help="Directory path where the extracted files will be stored locally",
    )

    args = parser.parse_args()

    if args.storage_type == "gcs":
        extract_spotify_seed_gcs(args.landing_bucket_name, args.landing_prefix)
    elif args.storage_type == "local":
        extract_spotify_seed_local(args.file_path, args.destination_path)


if __name__ == "__main__":
    main()
