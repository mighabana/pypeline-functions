#!/usr/bin/env python

import argparse

from dlt import pipeline as dlt_pipeline
from pypeline_functions.spotify.sources import spotify_seed_gcs, spotify_seed_local


def spotify_seed_to_warehouse(seed_source: str, seed_path: str, dataset_name: str, destination: str) -> None:
    """Run the Spotify data seed to BigQuery pipeline."""
    pipeline = dlt_pipeline(
        pipeline_name="spotify_seed", dataset_name=dataset_name, destination=destination, dev_mode=True
    )

    if seed_source == "local":
        data = spotify_seed_local(seed_path)
    elif seed_source == "gcs":
        data = spotify_seed_gcs(seed_path)

    info = pipeline.run(data)
    print(info)


def main() -> None:  # noqa: D103
    parser = argparse.ArgumentParser(description="Transfers data from the Spotify data seed file to the Data Warehouse")
    parser.add_argument(
        "--seed_source", type=str, required=True, choices=["local", "gcs"], help="where the data seed file is located"
    )
    parser.add_argument("--seed_path", type=str, required=True, help="file path where the data seed is stored")
    parser.add_argument(
        "--dataset_name", type=str, required=True, help="name of the dataset where the data will be loaded"
    )
    parser.add_argument(
        "--destination", type=str, required=True, choices=["postgres", "bigquery"], help="where the data will be saved"
    )

    args = parser.parse_args()
    spotify_seed_to_warehouse(args.seed_source, args.bucket_name, args.dataset_name, args.destination)


if __name__ == "__main__":
    main()
