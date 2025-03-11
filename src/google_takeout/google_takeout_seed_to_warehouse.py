#!/usr/bin/env python
import argparse

from dlt import pipeline as dlt_pipeline
from pypeline_functions.google_takeout.sources import google_takeout_seed_gcs, google_takeout_seed_local


def google_takeout_seed_to_warehouse(seed_source: str, seed_path: str, dataset_name: str, destination: str) -> None:
    """Run the Google Takeout data seed to Postgres pipeline."""
    pipeline = dlt_pipeline(
        pipeline_name="google_takeout_seed_postgres",
        dataset_name=dataset_name,
        destination=destination,
    )

    if seed_source == "local":
        data = google_takeout_seed_local(seed_path)
    elif seed_source == "gcs":
        data = google_takeout_seed_gcs(seed_path)

    info = pipeline.run(data)
    print(info)


def main() -> None:  # noqa: D103
    parser = argparse.ArgumentParser(
        description="Transfers data from the Google Takeout data seed file to the Data Warehouse"
    )
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
    google_takeout_seed_to_warehouse(args.seed_source, args.seed_path, args.dataset_name, args.destination)


if __name__ == "__main__":
    main()
