#pylint: disable=missing-docstring, line-too-long, trailing-whitespace
import pathlib as pl

import appdirs
import duckdb

SITE_DATA_DIR = pl.Path(appdirs.site_data_dir("DemoApp", "JB"))


class DUCKDB: #pylint: disable=too-few-public-methods
    """
    It's a singleton container for DuckDB connection info, and has only 1 utility function to ensure DB exists.
    """
    PATH = SITE_DATA_DIR / "dash.duckdb"
    SYSTEM_TABLES = ['current_notebook_id', 'has_onboarded', 'notebooks', 'notebook_versions']
    MOTHERDUCK_TABLES_WILDCARD = 'mdClientCache_%'
    ADMIN_TABLES = [f"administrative.{t}" for t in ['usage_tracking', 'update_tracking', 'user_tables']]

    @staticmethod
    def init():
        DUCKDB.PATH.parent.mkdir(parents=True, exist_ok=True)
        with duckdb.connect(DUCKDB.PATH) as conn:
            # create administrative tables

            conn.execute("""
                CREATE SCHEMA IF NOT EXISTS administrative;
                CREATE TABLE IF NOT EXISTS administrative.usage_tracking (
                    table_name TEXT NOT NULL PRIMARY KEY,
                    hits INT NOT NULL DEFAULT 0,
                    last_hit TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                );
                CREATE TABLE IF NOT EXISTS administrative.update_tracking (
                    table_name TEXT NOT NULL PRIMARY KEY,
                    last_update TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                );
                CREATE TABLE IF NOT EXISTS administrative.user_tables (
                    table_name TEXT NOT NULL PRIMARY KEY,
                    owned_by TEXT NOT NULL,
                    created TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    last_update TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                );
            """)

DUCKDB.init()


class POSTGRES: #pylint: disable=too-few-public-methods
    """
    It's a singleton container for Postgres cluster connections info.
    """
    pass