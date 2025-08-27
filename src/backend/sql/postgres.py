#pylint: disable=missing-docstring, line-too-long, trailing-whitespace
import json
from typing import Optional, List, Dict, Tuple, Union, Any

import duckdb
import pandas as pd

from . import DuckDBMonitorMiddleware
from . import base
from .base import DUCKDB, POSTGRES, ignore_warnings
from backend.restricted import safe_exec


class PostgresMonitorMiddleware:
    @staticmethod
    def ask_available_tables(conn:Optional[duckdb.DuckDBPyConnection]=None, with_etc: bool=False) -> Union[List[str], List[Tuple[str, Dict[str, Any]]]]:
        supplied_conn = conn is not None
        try:
            conn = conn or duckdb.connect(DUCKDB.PATH, read_only=True)
            sql = f"SELECT table_name{', etc' if with_etc else ''} FROM administrative.table_catalog WHERE external_type ='postgres';"
            result_set = conn.execute(sql).fetchall()
            return [(row[0], json.loads(row[1])) for row in result_set] if with_etc else [row[0] for row in result_set]
        finally:
            if not supplied_conn:
                print("PostgresMonitorMiddleware.ask_available_tables closing its own connection")
                conn.close() #type:ignore
    
    @staticmethod
    def get_dataframe(table_name:str, conn:Optional[duckdb.DuckDBPyConnection]=None, skip_logging:bool = False) -> Optional[pd.DataFrame]:
        supplied_conn = conn is not None
        try:
            conn = conn or duckdb.connect(DUCKDB.PATH, read_only=False)
            sql = """
                --sql
                SELECT etc
                FROM administrative.table_catalog
                WHERE external_type = 'postgres' AND table_name = ?
                LIMIT 1;
            """
            if etc := conn.execute(sql, (table_name,)).fetchone():
                etc = json.loads(etc[0])
                POSTGRES.ETC_SCHEMA.validate(etc)
                base.update_connection_map(conn)
                print(f"PostgresMonitorMiddleware.get_dataframe({table_name=}, {conn=}) -> {etc=}... {base.map_tables_to_connections=}")
                if connection := base.map_tables_to_connections.get(table_name):
                    print(f"PostgresMonitorMiddleware.get_dataframe found connection for {table_name=}: {connection=}")
                    with ignore_warnings(), connection.connect() as pg_conn:
                        df = pd.read_sql(etc['sql'], pg_conn)
                        if post_process_code := etc.get('post_process', None):
                            df = safe_exec(post_process_code, df=df)['df']
                        if not skip_logging:
                            DuckDBMonitorMiddleware.log_table_usage([table_name], conn=conn)
                        return df
        finally:
            if not supplied_conn:
                print("PostgresMonitorMiddleware.get_dataframe closing its own connection")
                conn.close() #type:ignore
