#!/usr/bin/env python
import argparse

from dlt import pipeline as dlt_pipeline
from dlt.destinations import postgres

from pypeline_functions.google_takeout.sources import google_takeout_seed_local


def google_takeout_seed_to_postgres(seed_path: str, dataset_name: str) -> None:
    """Run the Google Takeout data seed to Postgres pipeline."""
    pipeline = dlt_pipeline(
        pipeline_name="google_takeout_seed_postgres",
        dataset_name=dataset_name,
        destination="postgres",
    )

    data = google_takeout_seed_local(seed_path)

    info = pipeline.run(data)
    print(info)


def main() -> None:  # noqa: D103
    parser = argparse.ArgumentParser(
        description="Transfers data from the Google Takeout data seed file on GCS to BigQuery using dlt"
    )
    parser.add_argument("--seed_path", type=str, required=True, help="file path where the data seed is stored")
    parser.add_argument(
        "--dataset_name", type=str, required=True, help="name of the dataset where the data will be loaded"
    )

    args = parser.parse_args()

    google_takeout_seed_to_postgres(args.seed_path, args.dataset_name)


if __name__ == "__main__":
    main()
