#!/usr/bin/env python

import argparse

from dlt import pipeline as dlt_pipeline

from pypeline_functions.spotify.sources import spotify_seed_gcs


def spotify_seed_to_bigquery(bucket_name: str, dataset_name: str) -> None:
    """Run the Spotify data seed to BigQuery pipeline."""
    pipeline = dlt_pipeline(
        pipeline_name="spotify_seed", dataset_name=dataset_name, destination="bigquery", dev_mode=True
    )

    data = spotify_seed_gcs(bucket_name)

    info = pipeline.run(data)
    print(info)


def main() -> None:  # noqa: D103
    parser = argparse.ArgumentParser(
        description="Transfers data from the Spotify data seed file on GCS to BigQuery using dlt"
    )
    parser.add_argument(
        "--bucket_name", type=str, required=True, help="name of the bucket where the .json files are stored"
    )
    parser.add_argument(
        "--dataset_name", type=str, required=True, help="name of the dataset where the data will be loaded"
    )

    args = parser.parse_args()

    spotify_seed_to_bigquery(args.bucket_name, args.dataset_name)


if __name__ == "__main__":
    main()
