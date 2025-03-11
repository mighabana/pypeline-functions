#!/usr/bin/env python

import argparse

from dlt import pipeline as dlt_pipeline
from pypeline_functions.rss.sources import rss_feed


def rss_feed_to_postgres(feed_url: str, dataset_name: str, destination: str) -> None:
    """Run the RSS feed to Postgres pipeline."""
    pipeline = dlt_pipeline(
        pipeline_name="rss_feed_postgres",
        dataset_name=dataset_name,
        destination=destination,
    )

    data = rss_feed(feed_url)

    info = pipeline.run(data)
    print(info)


def main() -> None:  # noqa: D103
    parser = argparse.ArgumentParser(description="Transfers data from an RSS feed to PostgreSQL using dlt")
    parser.add_argument("--feed_url", type=str, required=True, help="URL of the RSS feed")
    parser.add_argument(
        "--dataset_name", type=str, required=True, help="name of the dataset where the data will be loaded"
    )
    parser.add_argument(
        "--destination", type=str, required=True, choices=["postgres", "bigquery"], help="where the data will be saved"
    )

    args = parser.parse_args()

    rss_feed_to_postgres(args.feed_url, args.dataset_name)


if __name__ == "__main__":
    main()
