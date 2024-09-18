"""Utility function for connecting to an MSSQL database as a source destination.

This module provides:
- SourceMSSQL: class containing helper functions for connecting to and fetching data from an MSSQL database.
"""

import datetime
import decimal
import logging
import traceback

import pyodbc
from sshtunnel import SSHTunnelForwarder


class SourceMSSQL:
    """A data source helper class for connecting to and fetching data from an MSSQL database.

    Attributes
    ----------
    name : str
        The name of the source for logging purposes.
    host : str
        The host address of the MSSQL database.
    db_name : str
        The database name of the MSSQL database to be accessed.
    username : str
        The username of the account to authenticate and access the specified databaase.
    password : str
        The password of the account to authenticate and access the specified database.
    port : int
        The port from which the database can be accessed.
    driver : str
        The name of the driver to be used by pyodbc to access the database.
    tds_version : str
        The TDS protocol version to be used by pyodbc to access the database.
    ssh_host : str, optional
        The host address of the SSH tunnel, is "127.0.0.1" by default.
    ssh_username : str, optional
        The username to authenticate into the SSH tunnel, is "root" by default.
    ssh_pkey : str, optional
        The path of the ssh private key to authenticate into the SSH tunnel, is "~/.ssh/id_ed25519" by default.
    ssh_pkey_password : str, optional
        The password of the SSH private key being used to access the SSH tunnel, is "" by default.

    Methods
    -------
    tunnel() -> bool
        Create an SSH Tunnel Connection to access the MSSQL database.
    connect(tunnel : bool, optional) -> bool
        Connect to the MSSQL database with/without an SSH tunnel.
    disconnect() -> bool
        Disconnect from the MSSQL database.
    query(query:str) -> list
        Execute the provided query on the connected database.
    fetch(fields:list, table:str, batch_size:int)
        Fetch the specified fields from the given table in batches.
    convert_data_types(incoming_type : any) -> str:
        Convert the incoming data to its proper pythonic datatype.
    """

    def __init__(
        self,
        name: str,
        host: str,
        db_name: str,
        username: str,
        password: str,
        port: int,
        driver: str,
        tds_version: str,
        ssh_host: str = "127.0.0.1",
        ssh_username: str = "root",
        ssh_pkey: str = "~/.ssh/id_ed25519",
        ssh_pkey_password: str = "",
    ) -> None:
        self.name = f"Source: (MSSQL) {name}"
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.db_name = db_name
        self.username = username
        # NOTE: check password security.
        # see if encryption is necessary to protect the password in transit.
        self.password = password
        self.driver = driver
        self.tds_version = tds_version
        self.ssh_host = ssh_host
        self.ssh_username = ssh_username
        self.ssh_pkey = ssh_pkey
        self.ssh_pkey_password = ssh_pkey_password

        self.connection = None
        self.cursor = None
        self.previous_query = None

    def tunnel(self) -> bool:
        """Create an SSH Tunnel Connection to access the MSSQL database."""
        try:
            ssh_tunnel = SSHTunnelForwarder(
                self.ssh_host,
                ssh_username=self.ssh_username,
                ssh_pkey=self.ssh_pkey,
                ssh_private_key_password=self.ssh_pkey_password,
                remote_bind_address=(self.host, int(self.port)),
            )
        except:
            logging.error(f"Could not establish connection to tunnel for {self.name}")
            traceback.print_exc()
            raise

        return ssh_tunnel

    def connect(self, tunnel: bool = False) -> bool:
        """Connect to the MSSQL database with/without an SSH tunnel.

        Parameters
        ----------
        tunnel : bool, optional
            A boolean value to determine if an SSH tunnel will be used or not to connect to the database.
        """
        logging.info(f"Connecting to {self.name}...")

        ssh_tunnel = None

        if tunnel:
            ssh_tunnel = self.tunnel()
            ssh_tunnel.start()

        if self.connection is None:
            try:
                conn = pyodbc.connect(
                    server=self.host if ssh_tunnel is None else "127.0.0.1",
                    database=self.db_name,
                    port=self.port
                    if ssh_tunnel is None
                    else ssh_tunnel.local_bind_port,
                    user=self.username,
                    password=self.password,
                    driver=self.driver,
                    tds_version=self.tds_version,
                )
            except:
                logging.error(f"Connection could not be established for {self.name}")
                traceback.print_exc()
                raise

                # We can return false when we figure out a retry strategy
                # return False

            self.connection = conn
            logging.info(f"Connection established for {self.name}")
            return True

        logging.info(f"Connection is already established for {self.name}")
        return True

    def disconnect(self) -> bool:
        """Disconnect from the MSSQL database."""
        logging.info(f"Disconnecting from {self.name}...")

        if self.connection is None:
            logging.error(
                f"Connection could not be disconnected because connection does not exist for {self.name}"
            )
            return True
        else:
            logging.info(f"Connection disconnected for {self.name}")
            self.cursor.close()
            self.connection.close()
            self.connection = None
            return True

    def query(self, query:str) -> list:
        """Execute the provided query on the connected database.

        Parameters
        ----------
        query : str
            The query to be executed.

        Warning.. This method is unsafe please make sure the query you are executing is tested and does not cause any
        unwanted side effects before running.
        """
        if self.connection is None:
            logging.error(
                f"Could not query from {self.name} because connection does not exist."
            )
            return []
        else:
            cur = self.connection.cursor()
            # TODO: investigate how to make the query execution safer.
            cur.execute(query)
            datum = cur.fetchall()
            columns = [col[0] for col in cur.description]
            cur.close()

            output = []

            for data in datum:
                row = {columns[i]: data[i] for i in range(len(data))}
                output.append(row)

            return output

    def fetch(self, fields: list, table: str, batch_size: int = 10000) -> tuple[list, dict]:
        """Fetch the specified fields from the given table in batches.

        Parameters
        ----------
        fields : list
            The list of fields to be extracted from the specified table. To select all fields use ["*"].
        table : str
            The name of the table to be extracted.
        batch_size : int, optional
            The number of rows to be processed for each batch, is 10000 by default.
        """
        logging.info(f"Fetching information from {self.name}...")

        # NOTE: possible SQL injection attack vector
        query = f'SELECT {", ".join(fields)} FROM {table}'  # noqa: S608

        try:
            if self.previous_query != query:
                cur = self.connection.cursor()
                self.cursor = cur
                self.cursor.execute(query)
                self.previous_query = query

            data = self.cursor.fetchmany(batch_size)
            column_headers = [desc[0] for desc in self.cursor.description]
            schema = {
                col[0]: self.convert_data_types(col[1])
                for col in self.cursor.description
            }
            output = []

            for item in data:
                row = {column_headers[i]: item[i] for i in range(len(item))}
                output.append(row)

            return (output, schema)

        except:
            logging.error(f"Failed to fetch data from {self.name}")
            traceback.print_exc()
            raise

            # We can return an empty list if we want it to fail nicely
            # Although the failure won't be caught by airflow if we do
            # return []

    def convert_data_types(self, incoming_type : any) -> str:
        """Convert the incoming data to its proper pythonic data type.

        Parameters
        ----------
        incoming_type : any
            The data type from MSSQL of the incoming data.
        """
        if incoming_type is str:
            return "string"
        elif incoming_type is int:
            return "Int64"
        elif incoming_type is datetime.datetime:
            return "datetime64[ns]"
        elif incoming_type is decimal.Decimal:
            return "float64"
        elif incoming_type is bytearray:
            return "object"
        elif incoming_type is bool:
            return "bool"
        else:
            print(incoming_type)
            raise (ValueError)
