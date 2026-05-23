#pylint: disable=missing-docstring, line-too-long, trailing-whitespace
import json
from typing import Optional, List, Dict, Tuple, Union, Any

import duckdb
import pandas as pd

from . import DuckDBMonitorMiddleware
from . import base
from .base import DUCKDB, POSTGRES, ignore_warnings, managed_duck_conn
from backend.restricted import safe_exec


class PostgresMonitorMiddleware:
    @staticmethod
    def ask_available_tables(conn:Optional[duckdb.DuckDBPyConnection]=None, with_etc: bool=False) -> Union[List[str], List[Tuple[str, Dict[str, Any]]]]:
        sql = f"SELECT table_name{', etc' if with_etc else ''} FROM administrative.table_catalog WHERE external_type = 'postgres';"
        with managed_duck_conn(conn, read_only=True) as conn:
            result_set = conn.execute(sql).fetchall()
        return [(row[0], json.loads(row[1])) for row in result_set] if with_etc else [row[0] for row in result_set]
    
    @staticmethod
    def get_dataframe(table_name:str, conn:Optional[duckdb.DuckDBPyConnection]=None, skip_logging:bool = False) -> Optional[pd.DataFrame]:
        sql = """
            --sql
            SELECT etc
            FROM administrative.table_catalog
            WHERE external_type = 'postgres' AND table_name = ?
            LIMIT 1;
        """
        with managed_duck_conn(conn, read_only=False) as conn:
            if etc := conn.execute(sql, (table_name,)).fetchone():
                etc = json.loads(etc[0])
                if POSTGRES.ETC_SCHEMA is None:
                    POSTGRES.reload_config()
                POSTGRES.ETC_SCHEMA.validate(etc)
                base.update_connection_map(conn)
                if connection := base.map_tables_to_connections.get(table_name):
                    with ignore_warnings(), connection.connect() as pg_conn:
                        df = pd.read_sql_query(etc['sql'], pg_conn)
                        if post_process_code := etc.get('post_process', None):
                            df = safe_exec(post_process_code, df=df)['df']
                        if not skip_logging:
                            DuckDBMonitorMiddleware.log_table_usage([table_name], conn=conn)
                        return df
