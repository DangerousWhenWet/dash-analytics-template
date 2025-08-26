#pylint: disable=missing-docstring, line-too-long, trailing-whitespace
import json
import pathlib as pl
from typing import cast, Optional, Literal, Dict, Any

import appdirs
import duckdb
import jsonschema
import pandas as pd
import pydataset

from pages.utils.etc import load_config

SITE_DATA_DIR = pl.Path(appdirs.site_data_dir("DemoApp", "JB"))
THIS_DIR = pl.Path(__file__).parent
BASE_DIR = THIS_DIR.parent.parent
ASSETS_DIR = BASE_DIR / "assets"


config = load_config()
with open(THIS_DIR/'pg_etc.schema.json', 'r', encoding='utf8') as f:
    pg_etc_schema = jsonschema.Draft7Validator(json.load(f))


class DUCKDB: #pylint: disable=too-few-public-methods
    """
    It's a singleton container for DuckDB connection info, and has only 1 utility function to ensure DB exists.
    """
    PATH = SITE_DATA_DIR / "dash.duckdb"
    SYSTEM_TABLES = ['current_notebook_id', 'has_onboarded', 'notebooks', 'notebook_versions']
    MOTHERDUCK_TABLES_WILDCARD = 'mdClientCache_%'
    ADMIN_TABLES = [f"administrative.{t}" for t in ['table_catalog']]

    @staticmethod
    def init(wipe:bool=False):
        DUCKDB.PATH.parent.mkdir(parents=True, exist_ok=True)
        if wipe and DUCKDB.PATH.exists():
            DUCKDB.PATH.unlink()

        with duckdb.connect(DUCKDB.PATH) as conn:
            # create administrative schemata and tables
            conn.execute("""
                CREATE SCHEMA IF NOT EXISTS datasets;
                CREATE SCHEMA IF NOT EXISTS administrative;
                CREATE TABLE IF NOT EXISTS administrative.table_catalog (
                    table_name TEXT NOT NULL PRIMARY KEY,
                    table_description TEXT, --nullable
                    table_type TEXT NOT NULL DEFAULT 'system' CHECK (table_type IN ('system', 'user-temporary', 'user-persistent')),
                    -- ^^ DuckDB has `enum` type but it kind of sucks, "fake it" with a CHECK constraint
                    owned_by TEXT NOT NULL DEFAULT 'unknown',
                    created TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updates INT NOT NULL DEFAULT 0,
                    updated TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    hits INT NOT NULL DEFAULT 0,
                    last_hit TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    is_pseudo_table BOOLEAN NOT NULL DEFAULT FALSE,
                    etc JSON NOT NULL DEFAULT '{}'
                );
            """)

            # create sample data tables
            df_iris = pydataset.data('iris')
            df_economics = pydataset.data('economics')
            df_octoprint = pd.read_csv(
                ASSETS_DIR/'3d-printer-temps.csv',
                parse_dates=['last_changed'],
            ) # <-- i downloaded a sensor stream from my octoprint 3D printer server via home assistant...
              #     needed timestamped timeseries data...
            DUCKDB.ingest(
                cast(pd.DataFrame, df_iris),
                table_name='iris',
                table_description=(
                    "A 1936 dataset prepared by statistician Ronald Fisher, contains multivariate data describing three "
                    "species of Iris flowers (Iris setosa, Iris virginica, and Iris versicolor). Widely used as a "
                    "test dataset for machine learning algorithms."
                ),
                table_type="system",
                conn=conn
            )
            DUCKDB.ingest(
                cast(pd.DataFrame, df_economics),
                table_name='economics',
                table_description="This dataset was produced from US economic time series data available from the Federal Reserve Bank of St. Louis.",
                table_type="system",
                conn=conn
            )
            DUCKDB.ingest(
                df_octoprint,
                table_name='octoprint',
                table_description="A sensor stream from my personal Octoprint 3D printer server via HomeAssistant...",
                table_type="system",
                owned_by='jbechen',
                conn=conn
            )
            if wipe:
                DUCKDB.pseudo_ingest(
                    table_name='pg_foo',
                    table_description='A silly dummy DB I created to test the Postgres pseudo-ingestion path.',
                    table_type='system',
                    owned_by='jbechen',
                    external_type='postgres',
                    etc={
                        'cluster': 'nucleus.home.arpa',
                        'database': 'foo',
                        'sql': 'SELECT * FROM my_table;'
                    }
                )

            # WAL cleanup
            conn.sql("CHECKPOINT;")
    
    @staticmethod
    def ingest(
                df:pd.DataFrame, #pylint: disable=unused-argument
                table_name:str,
                table_description:Optional[str] = None,
                table_type:Optional[Literal['system', 'user-temporary', 'user-persistent']] = 'system',
                owned_by:Optional[str] = 'unknown',
                conn:Optional[duckdb.DuckDBPyConnection] = None
            ):
        supplied_conn = conn is not None
        try:
            conn = conn or duckdb.connect(DUCKDB.PATH)
            if table_already_exists := conn.execute("SELECT COUNT(*) FROM duckdb_tables() WHERE schema_name = 'datasets' AND table_name = ?;", [table_name]).fetchone():
                table_already_exists = bool(table_already_exists[0])
            conn.sql(f"CREATE OR REPLACE TABLE datasets.{table_name} AS SELECT * FROM df;")

            params = {k:v for k,v in zip(
                ['table_name', 'table_description', 'table_type', 'owned_by'],
                [table_name, table_description, table_type, owned_by]
            ) if v is not None}
            if table_already_exists:
                conn.sql(f"""
                    INSERT INTO administrative.table_catalog ({','.join(params.keys())}, updates, updated)
                    VALUES ({','.join(['?'] * len(params))}, 1, NOW())
                    ON CONFLICT (table_name) DO UPDATE SET
                        updates = updates + 1,
                        updated = NOW();
                """, params=list(params.values()))
            else:
                conn.sql(f"""
                    INSERT INTO administrative.table_catalog ({','.join(params.keys())})
                    VALUES ({','.join(['?'] * len(params))});
                """, params=list(params.values()))

        finally:
            if not supplied_conn:
                conn.close() #type: ignore
    
    @staticmethod
    def pseudo_ingest(
                table_name:str,
                table_description:Optional[str] = None,
                table_type:Optional[Literal['system', 'user-temporary', 'user-persistent']] = 'system',
                owned_by:Optional[str] = 'unknown',
                external_type:Literal['postgres'] = 'postgres',
                etc:Optional[Dict[str, Any]] = None,
                conn:Optional[duckdb.DuckDBPyConnection] = None
        ):
        """
        This is a "pseudo-ingest" function that doesn't actually ingest any data, but it does
        add the table to the catalog. Use it when you want to borrow the usage tracking / ownership
        mechanism but your data set is external to DuckDB.
        """
        supplied_conn = conn is not None
        etc = etc or {}
        try:
            match external_type:
                case 'postgres':
                    pg_etc_schema.validate(etc)
                    cluster_exists = etc['cluster'] in POSTGRES.CLUSTERS
                    db_exists = etc['database'] in POSTGRES.CLUSTERS.get(etc['cluster'], {}).get('databases', [])
                    if not (cluster_exists and db_exists):
                        raise ValueError(f"PostgreSQL cluster {etc['cluster']} or database {etc['database']} does not exist in the configuration.")
                case _:
                    raise ValueError(f"Unknown external_type: {external_type}")
            
            conn = conn or duckdb.connect(DUCKDB.PATH)
            params = {k:v for k,v in zip(
                ['table_name', 'table_description', 'table_type', 'owned_by', 'is_pseudo_table', 'etc'],
                [table_name, table_description, table_type, owned_by, True, etc]
            ) if v is not None}
            conn.sql(
                f"INSERT INTO administrative.table_catalog ({','.join(params.keys())}) VALUES ({','.join(['?'] * len(params))});",
                params=list(params.values())
            )
        finally:
            if not supplied_conn:
                conn.close() #type: ignore


class POSTGRES: #pylint: disable=too-few-public-methods
    """
    It's a singleton container for Postgres cluster connections info.
    """
    CLUSTERS: Dict[str, Dict[str, Any]] = config.get('PostgreSQL', {}).get('clusters', {})

    @staticmethod
    def ingest(
                table_name:str,
                table_description:Optional[str] = None,
                table_type:Optional[Literal['system', 'user-temporary', 'user-persistent']] = 'system',
                owned_by:Optional[str] = 'unknown',
                external_type:Literal['postgres'] = 'postgres',
                etc:Optional[Dict[str, Any]] = None,
                conn:Optional[duckdb.DuckDBPyConnection] = None
        ):
        """
        It's just a wrapper for DuckDB's `pseudo_ingest` function.
        """
        DUCKDB.pseudo_ingest(
            table_name=table_name,
            table_description=table_description,
            table_type=table_type,
            owned_by=owned_by,
            external_type=external_type,
            etc=etc,
            conn=conn
        )
