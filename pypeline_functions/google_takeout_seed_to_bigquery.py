#!/usr/bin/env python

import dlt

from .sources.google_takeout_seed import google_takeout_seed


def google_takeout_seed_to_bigquery(bucket_name: str, dataset_name: str) -> None:
    """Run the Google Takeout data seed to BigQuery pipeline."""
    pipeline = dlt.pipeline(
        pipeline_name="google_takeout_seed", dataset_name=dataset_name, destination="bigquery", dev_mode=True
    )

    data = google_takeout_seed(bucket_name)

    info = pipeline.run(data)
    print(info)


def main() -> None:  # noqa: D103
    import argparse

    parser = argparse.ArgumentParser(
        description="Transfers data from the Google Takeout data seed file on GCS to BigQuery using dlt"
    )
    parser.add_argument(
        "--bucket_name", type=str, required=True, help="name of the bucket where the .json files are stored"
    )
    parser.add_argument(
        "--dataset_name", type=str, required=True, help="name of the dataset where the data will be loaded"
    )

    args = parser.parse_args()

    google_takeout_seed_to_bigquery(args.bucket_name, args.dataset_name)


if __name__ == "__main__":
    main()
