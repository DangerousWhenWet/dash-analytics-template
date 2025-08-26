#pylint: disable=missing-docstring, line-too-long, trailing-whitespace
from contextlib import contextmanager
import json
import pathlib as pl
from typing import cast, Optional, Literal, Dict, List, Any
import warnings

import appdirs
import duckdb
import hjson
import jsonschema
import pandas as pd
import psycopg2 as pg
import pydataset
from pydantic import BaseModel

from pages.utils.etc import load_config


SITE_DATA_DIR = pl.Path(appdirs.site_data_dir("DemoApp", "JB"))
THIS_DIR = pl.Path(__file__).parent
BASE_DIR = THIS_DIR.parent.parent
ASSETS_DIR = BASE_DIR / "assets"


config = load_config()


@contextmanager
def ignore_warnings():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        yield


class DUCKDB: #pylint: disable=too-few-public-methods
    """
    It's a singleton container for DuckDB connection info, and has utility functions for initialization, ingestion, and maintenance of data.
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
                    external_type TEXT CHECK ((external_type IN ('postgres')) OR (external_type IS NULL)),
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
                    POSTGRES.ETC_SCHEMA.validate(etc)
                    cluster_exists = etc['cluster'] in POSTGRES.CLUSTERS
                    db_exists = etc['database'] in POSTGRES.CLUSTERS.get(etc['cluster'], {}).get('databases', [])
                    if not (cluster_exists and db_exists):
                        raise ValueError(f"PostgreSQL cluster {etc['cluster']} or database {etc['database']} does not exist in the configuration.")
                case _:
                    raise ValueError(f"Unknown external_type: {external_type}")
            
            conn = conn or duckdb.connect(DUCKDB.PATH)
            if table_already_exists := conn.execute("SELECT COUNT(*) FROM duckdb_tables() WHERE schema_name = 'datasets' AND table_name = ?;", [table_name]).fetchone():
                table_already_exists = bool(table_already_exists[0])
            
            params = {k:v for k,v in zip(
                ['table_name', 'table_description', 'table_type', 'owned_by', 'is_pseudo_table', 'external_type', 'etc'],
                [table_name, table_description, table_type, owned_by, True, external_type, etc]
            ) if v is not None}

            if table_already_exists:
                # Build the set clause for all fields except table_name (the key)
                update_fields = [k for k in params.keys() if k != 'table_name']
                set_clause = ', '.join([f"{field} = ?" for field in update_fields])
                
                conn.sql(f"""
                    --sql
                    INSERT INTO administrative.table_catalog ({','.join(params.keys())}, updates, updated)
                    VALUES ({','.join(['?'] * len(params))}, 1, NOW())
                    ON CONFLICT (table_name) DO UPDATE SET
                        {set_clause},
                        updates = updates + 1,
                        updated = NOW();
                """, params=list(params.values()) + [params[field] for field in update_fields])
            else:
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
    with open(THIS_DIR/'pg.schema.json', 'r', encoding='utf8') as f:
        _schema = json.load(f)
        _resolver = jsonschema.RefResolver.from_schema(_schema)
    ETC_SCHEMA = jsonschema.Draft202012Validator({"$ref": "#/$defs/PseudoIngestRequest"}, resolver=_resolver)
    HJSON_SCHEMA = jsonschema.Draft202012Validator({"$ref": "#/$defs/UnattendedPseudoIngestRequest"}, resolver=_resolver)

    @staticmethod
    def init(conn:Optional[duckdb.DuckDBPyConnection] = None):
        for hjson_file in (THIS_DIR/'pg_datasets').glob('*.hjson'):
            POSTGRES.ingest_hjson(hjson_file, conn=conn)

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

    @staticmethod
    def ingest_hjson(hjson_file:pl.Path, conn:Optional[duckdb.DuckDBPyConnection] = None):
        """
        Ingests a HJSON file into the database.
        """
        with open(hjson_file, 'r', encoding='utf8') as f: #pylint: disable=redefined-outer-name
            definition = hjson.load(f)
            POSTGRES.HJSON_SCHEMA.validate(definition)
            POSTGRES.ingest(
                table_name=definition['table_name'],
                table_description=definition.get('table_description'),
                table_type=definition.get('table_type'),
                owned_by=definition.get('owned_by'),
                external_type='postgres',
                etc={k:definition[k] for k in ('cluster', 'database', 'sql')},
                conn=conn
            )


class PostgresConnectionDetail(BaseModel):
    host: str
    port: int
    username: str
    password: str
    databases: List[str]

    def connect(self, database: str) -> pg.extensions.connection:
        if database not in self.databases:
            raise ValueError(f"Database '{database}' is not in the list of available databases.")
        return pg.connect(
            host=self.host,
            port=self.port,
            user=self.username,
            password=self.password,
            database=database
        )

def get_external_connection_detail(external_type:Literal['postgres'], etc:Dict[str, Any], config:Optional[Dict[str, Any]]=None) -> Optional[PostgresConnectionDetail]:
    config = config or load_config()
    try:
        match external_type:
            case 'postgres':
                return PostgresConnectionDetail(**config.get('PostgreSQL', {}).get('clusters', {}).get(etc['cluster'], {}))
            case _:
                return None
    except Exception as e: #pylint:disable=broad-except
        print(f"Error occurred while getting external connection detail: {e.__class__.__name__}: {e}")
        return None


def init(wipe:bool=False):
    DUCKDB.init(wipe=wipe)
    with duckdb.connect(DUCKDB.PATH) as conn:
        POSTGRES.init(conn=conn)
        conn.sql('CHECKPOINT;') # for WAL
