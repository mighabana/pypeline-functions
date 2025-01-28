#!/usr/bin/env python
import argparse

from dlt import pipeline as dlt_pipeline
from dlt.sources.credentials import ConnectionStringCredentials
from dlt.sources.sql_database import sql_database


def ctu_relational_to_postgres() -> None:
    """Run the CTU Relational database to Postgres pipeline."""
    databases = ["financial", "imdb_full", "NBA"]

    for db in databases:
        pipeline = dlt_pipeline(
            pipeline_name=f"ctu_relational_{db}_postgres",
            dataset_name=db,
            destination="postgres",
        )

        credentials = ConnectionStringCredentials(
            f"mariadb+mariadbconnector://guest:ctu-relational@relational.fel.cvut.cz:3306/{db}"
        )
        source = sql_database(credentials)

        info = pipeline.run(source)
        print(info)


def main() -> None:  # noqa: D103
    ctu_relational_to_postgres()


if __name__ == "__main__":
    main()
