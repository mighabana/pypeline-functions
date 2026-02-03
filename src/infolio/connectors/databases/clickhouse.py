import os

import clickhouse_connect
import polars as pl

# TODO: implement logging

class Clickhouse:
    """
    ClickHouse utility class.

    Parameters
    ----------
    host : str | None
        ClickHouse host URL. If None, will use the `CONNECTOR__CLICKHOUSE__HOST` environment variable.
    port : str | None
        ClickHouse HTTP port. If None, will use the `CONNECTOR__CLICKHOUSE__HTTP_PORT` environment variable.
    username : str | None
        ClickHouse username. If None, will use the `CONNECTOR__CLICKHOUSE__USERNAME` environment variable
    password : str | None
        ClickHouse password. If None, will use the `CONNECTOR__CLICKHOUSE__PASSWORD` environment variable
    """

    def __init__(self, host:str|None=None, port:int|None=None, username:str|None=None, password:str|None=None) -> None:

        client_kwargs = {
            "host": host or os.getenv("CONNECTOR__CLICKHOUSE__HOST"),
            "port": port or os.getenv("CONNECTOR__CLICKHOUSE__HTTP_PORT"),
            "username": username or os.getenv("CONNECTOR__CLICKHOUSE__USERNAME"),
            "password": password or os.getenv("CONNECTOR__CLICKHOUSE__PASSWORD"),
        }


        self.client = clickhouse_connect.get_client(**client_kwargs)

    def query(self, query:str) -> pl.DataFrame:
        """
        Execute the given query and return the response as a Polars DataFrame.

        Parameters
        ----------
        query : str
            The query to be executed.

        Returns
        -------
        query_output : pl.DataFrame
            The query output as a Polars DataFrame.
        """
        return pl.from_arrow(self.client.query_arrow(query))

