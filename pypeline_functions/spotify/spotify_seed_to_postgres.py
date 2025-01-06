#!/usr/bin/env python

import argparse

from dlt import pipeline as dlt_pipeline

from pypeline_functions.spotify.sources import spotify_seed_local


def spotify_seed_to_postgres(seed_path: str, dataset_name: str) -> None:
    """Run the Spotify data seed to Postgres pipeline."""
    pipeline = dlt_pipeline(pipeline_name="spotify_seed", dataset_name=dataset_name, destination="postgres")

    data = spotify_seed_local(seed_path)

    info = pipeline.run(data)
    print(info)


def main() -> None:  # noqa: D103
    parser = argparse.ArgumentParser(
        description="Transfers data from the Spotify data seed file on GCS to BigQuery using dlt"
    )
    parser.add_argument("--seed_path", type=str, required=True, help="file path where the data seed is stored")
    parser.add_argument(
        "--dataset_name", type=str, required=True, help="name of the dataset where the data will be loaded"
    )

    args = parser.parse_args()

    spotify_seed_to_postgres(args.seed_path, args.dataset_name)


if __name__ == "__main__":
    main()
