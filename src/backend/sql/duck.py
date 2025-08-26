#pylint: disable=missing-docstring,line-too-long,trailing-whitespace
from contextlib import contextmanager
import re
from typing import Optional, Any, List, Set
import warnings


import duckdb
import pandas as pd
import sqlparse

from .base import DUCKDB


@contextmanager
def ignore_warnings():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        yield


class DuckDBMonitorMiddleware:
    """
    It's a singleton that you can use to query DuckDB and it will track "hits" to tables for analytics purposes.
    i.e., so you know what datasets are "hot" and "not"
    You can also use it to log arbitrary hits to arbitrary tables (even if they don't exist -- which is relied
    upon by e.g. the PostgresMonitorMiddleware to log hits to different Postgres clusters as if they were names
    of DuckDB tables)
    """
    @staticmethod
    def ask_available_tables(conn:Optional[duckdb.DuckDBPyConnection]=None, schema_name:str='datasets') -> List[str]:
        sql = """
            SELECT table_name 
            FROM duckdb_tables() 
            WHERE
                schema_name = ? AND
                --exclude system tables
                table_name NOT IN (SELECT UNNEST(?)) AND
                table_name NOT LIKE ?;
        """
        params = [schema_name,DUCKDB.SYSTEM_TABLES, DUCKDB.MOTHERDUCK_TABLES_WILDCARD]
        
        if conn:
            result_set = conn.execute(sql, params).fetchall()
        else:
            with duckdb.connect(DUCKDB.PATH, read_only=True) as conn:
                result_set = conn.execute(sql, params).fetchall()
        return [row[0] for row in result_set]


    @staticmethod
    def _extract_all_tokens(parsed_query: sqlparse.sql.Statement) -> List[str]:
        """recursively extract all tokens from parsed SQL"""
        tokens: List[str] = []
        
        def recurse_tokens(token_list: List[Any]) -> None:
            for token in token_list:
                if hasattr(token, 'tokens'):
                    # It's a group, recurse into it
                    recurse_tokens(token.tokens)
                else:
                    # It's a leaf token
                    token_value: str = str(token).strip()
                    if token_value and not token_value.isspace():
                        tokens.append(token_value)
        
        recurse_tokens(parsed_query.tokens)
        return tokens


    @staticmethod
    def _find_referenced_tables(sql: str, tables: List[str]) -> List[str]:
        """find which if any of `tables` are referenced anywhere in the query"""
        referenced_tables: Set[str] = set()
        
        # first try sqlparse
        try:
            parsed: sqlparse.sql.Statement = sqlparse.parse(sql)[0]
            tokens: List[str] = DuckDBMonitorMiddleware._extract_all_tokens(parsed)
            for token in tokens:
                clean_token: str = token.strip().strip('"').strip("'").strip('`')
                if clean_token in tables:
                    referenced_tables.add(clean_token)
        except Exception as e: # pylint: disable=broad-except
            print(f"sqlparse failed, falling back to regex. {e.__class__.__name__}: {e}")
        
        # else fall back to regex
        if not referenced_tables:
            sql_upper: str = sql.upper()
            for table_name in tables:
                # Use word boundaries to avoid partial matches
                pattern: str = r'\b' + re.escape(table_name.upper()) + r'\b'
                if re.search(pattern, sql_upper):
                    referenced_tables.add(table_name)
        
        return list(referenced_tables)


    @staticmethod
    def log_query(sql: str, conn:duckdb.DuckDBPyConnection):
        tables = DuckDBMonitorMiddleware.ask_available_tables(conn)

        # find which tables are referenced in the query:
        #     occurence of any of `tables` in the query counts as a "hit" to the table, regardless of which clause it's in
        #     multiple occurences across the query still only count as one hit
        #     each one "hit" we will +1 to the `hits` column
        if referenced_tables := DuckDBMonitorMiddleware._find_referenced_tables(sql, tables):
            DuckDBMonitorMiddleware.log_table_usage(referenced_tables, conn=conn)

    @staticmethod
    def log_table_usage(tables: List[str], conn:Optional[duckdb.DuckDBPyConnection]=None):
        sql_upsert = """
            INSERT INTO administrative.table_catalog (table_name, hits, last_hit)
            SELECT table_name, 1, NOW()
            FROM (SELECT UNNEST(?) AS table_name)
            ON CONFLICT (table_name) DO UPDATE SET
                hits = hits + 1,
                last_hit = NOW();
        """
        params_upsert = [tables]
        
        try:
            if conn:
                conn.execute(sql_upsert, params_upsert)
            else:
                with duckdb.connect(DUCKDB.PATH) as conn:
                    conn.execute(sql_upsert, params_upsert)
        except duckdb.TransactionException:
            # NOTE: with high concurrency on same record this can happen. e.g. multiple celery workers hitting on the same datasource.
            #       we are dealing with the problem by applying an Ostrich Algorithm ;)
            pass


    @staticmethod
    def log_table_update(tables: List[str], conn: Optional[duckdb.DuckDBPyConnection] = None):
        sql_upsert = """
            INSERT INTO administrative.table_catalog (table_name, updates, updated)
            SELECT table_name, 1, NOW()
            FROM (SELECT UNNEST(?) AS table_name)
            ON CONFLICT (table_name) DO UPDATE SET
                updates = updates + 1,
                updated = NOW();
        """
        params_upsert = [tables]

        try:
            if conn:
                conn.execute(sql_upsert, params_upsert)
            else:
                with duckdb.connect(DUCKDB.PATH) as conn:
                    conn.execute(sql_upsert, params_upsert)
        except duckdb.TransactionException:
            # NOTE: with high concurrency on same record this can happen. e.g. multiple celery workers hitting on the same datasource.
            #       we are dealing with the problem by applying an Ostrich Algorithm ;)
            pass


    @staticmethod
    def get_dataframe(sql: str, *args, skip_logging:bool=False, **kwargs):
        with ignore_warnings(), duckdb.connect(DUCKDB.PATH, *args, **kwargs) as conn:
            if not skip_logging:
                DuckDBMonitorMiddleware.log_query(sql, conn)
            return pd.read_sql(sql, conn)
