#!/usr/bin/env python
import argparse

from dlt import pipeline as dlt_pipeline
from dlt.sources.credentials import ConnectionStringCredentials
from dlt.sources.sql_database import sql_database


def ctu_relational_to_warehouse(destination: str) -> None:
    """Run the CTU Relational database to Postgres pipeline."""
    databases = ["financial", "imdb_full", "NBA"]

    for db in databases:
        pipeline = dlt_pipeline(
            pipeline_name=f"ctu_relational_{db}_postgres",
            dataset_name=db,
            destination=destination,
        )

        credentials = ConnectionStringCredentials(
            f"mariadb+mariadbconnector://guest:ctu-relational@relational.fel.cvut.cz:3306/{db}"
        )
        source = sql_database(credentials)

        info = pipeline.run(source)
        print(info)


def main() -> None:  # noqa: D103
    parser = argparse.ArgumentParser(
        description="Transfers data from the CTU relational database to the Data Warehouse"
    )
    parser.add_argument(
        "--destination", type=str, required=True, choices=["postgres", "bigquery"], help="where the data will be saved"
    )

    args = parser.parse_args()
    ctu_relational_to_warehouse(args.destination)


if __name__ == "__main__":
    main()
