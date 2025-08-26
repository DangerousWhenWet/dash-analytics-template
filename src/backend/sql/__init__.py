#pylint: disable=line-too-long, trailing-whitespace, missing-docstring
from typing import Optional, Union, Dict

from .base import DUCKDB, POSTGRES
from .duck import DuckDBMonitorMiddleware
from .postgres import PostgresMonitorMiddleware


ConnectorType = Union[DuckDBMonitorMiddleware, PostgresMonitorMiddleware]


def get_connector(table_name:str) -> Optional[ConnectorType]:
    tables: Dict[str, ConnectorType] = {} | \
        {tbl:DuckDBMonitorMiddleware for tbl in DuckDBMonitorMiddleware.ask_available_tables()} | \
        {tbl:PostgresMonitorMiddleware for tbl in PostgresMonitorMiddleware.ask_available_tables()}

    return tables.get(table_name)
