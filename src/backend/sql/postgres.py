#pylint: disable=missing-docstring, line-too-long, trailing-whitespace
import json
from typing import Optional, List

import duckdb
import pandas as pd
import psycopg2 as pg

from .base import DUCKDB, POSTGRES, get_external_connection_detail, ignore_warnings

class PostgresMonitorMiddleware:
    @staticmethod
    def ask_available_tables(conn:Optional[duckdb.DuckDBPyConnection]=None) -> List[str]:
        supplied_conn = conn is not None
        try:
            conn = conn or duckdb.connect(DUCKDB.PATH, read_only=True)
            sql = "SELECT table_name FROM administrative.table_catalog WHERE external_type ='postgres';"
            result_set = conn.execute(sql).fetchall()
            return [row[0] for row in result_set]
        finally:
            if not supplied_conn:
                conn.close() #type:ignore
    
    @staticmethod
    def get_dataframe(table_name:str, conn:Optional[duckdb.DuckDBPyConnection]=None) -> Optional[pd.DataFrame]:
        supplied_conn = conn is not None
        try:
            conn = conn or duckdb.connect(DUCKDB.PATH, read_only=True)
            sql = """
                --sql
                SELECT etc
                FROM administrative.table_catalog
                WHERE external_type = 'postgres' AND table_name = ?
                LIMIT 1;
            """
            if result := conn.execute(sql, (table_name,)).fetchone():
                etc = json.loads(result[0])
                POSTGRES.ETC_SCHEMA.validate(etc)
                if detail := get_external_connection_detail('postgres', etc):
                    print(detail)
                    with ignore_warnings(), detail.connect(etc['database']) as pg_conn:
                        df = pd.read_sql(etc['query'], pg_conn)
                        return df
        finally:
            if not supplied_conn:
                conn.close() #type:ignore