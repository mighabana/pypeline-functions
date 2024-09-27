#!/usr/bin/env python

import dlt
from sources.spotify_seed import spotify_seed


def pipeline_run(
    bucket_name:str,
    dataset_name:str
) -> None:
    """Run the Spotify data seed to bigquery pipeline."""
    pipeline = dlt.pipeline(
        pipeline_name="spotify_seed",
        dataset_name=dataset_name,
        destination="bigquery"
    )

    data = spotify_seed(bucket_name)

    info = pipeline.run(data)
    print(info)

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Transfers data from the Spotify data seed file on GCS to BigQuery using dlt")
    parser.add_argument(
        "--bucket_name", type=str, required=True,
        help="name of the bucket where the .json files are stored"
    )
    parser.add_argument(
        "--dataset_name", type=str, required=True,
        help="name of the dataset where the data will be loaded"
    )

    args = parser.parse_args()

    pipeline_run(
        args.bucket_name,
        args.dataset_name
    )
