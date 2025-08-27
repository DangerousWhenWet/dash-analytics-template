#pylint: disable=missing-docstring, line-too-long, trailing-whitespace, redefined-outer-name, unused-argument, unnecessary-ellipsis
from contextlib import contextmanager
import functools
import itertools
import json
import pathlib as pl
from typing import cast, Optional, Literal, Dict, List, Any, Protocol, TypeVar
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
                print("DUCKDB.ingest closing its own connection")
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
            #if table_already_exists := conn.execute("SELECT COUNT(*) FROM duckdb_tables() WHERE schema_name = 'datasets' AND table_name = ?;", [table_name]).fetchone():
            if table_already_exists := conn.execute("SELECT COUNT(*) FROM administrative.table_catalog WHERE table_name = ?;", [table_name]).fetchone():
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
                print("DUCKDB.pseudo_ingest closing its own connection")
                conn.close() #type: ignore


class POSTGRES: #pylint: disable=too-few-public-methods
    """
    It's a singleton container for Postgres cluster connections info.
    """
    # CLUSTERS: Dict[str, Dict[str, Any]] = config.get('PostgreSQL', {}).get('clusters', {})
    # with open(THIS_DIR/'pg.schema.json', 'r', encoding='utf8') as f:
    #     _schema = json.load(f)
    #     _resolver = jsonschema.RefResolver.from_schema(_schema)
    # MAP_DATABASES_TO_CLUSTERS = {
    #     db_name: cluster_name
    #     for cluster_name, cluster_info in CLUSTERS.items()
    #     for db_name in cluster_info.get("databases", [])
    # }
    # ETC_SCHEMA = jsonschema.Draft202012Validator({"$ref": "#/$defs/PseudoIngestRequest"}, resolver=_resolver)
    # HJSON_SCHEMA = jsonschema.Draft202012Validator({"$ref": "#/$defs/UnattendedPseudoIngestRequest"}, resolver=_resolver)
    CLUSTERS: Dict[str, Dict[str, Any]] = {}
    MAP_DATABASES_TO_CLUSTERS: Dict[str, str] = {}
    ETC_SCHEMA: jsonschema.Draft202012Validator = None #type: ignore
    HJSON_SCHEMA: jsonschema.Draft202012Validator = None #type: ignore

    @classmethod
    def reload_config(cls):
        cls.CLUSTERS: Dict[str, Dict[str, Any]] = config.get('PostgreSQL', {}).get('clusters', {})
        with open(THIS_DIR/'pg.schema.json', 'r', encoding='utf8') as f:
            _schema = json.load(f)
            _resolver = jsonschema.RefResolver.from_schema(_schema)
        cls.MAP_DATABASES_TO_CLUSTERS = {
            db_name: cluster_name
            for cluster_name, cluster_info in cls.CLUSTERS.items()
            for db_name in cluster_info.get("databases", [])
        }
        cls.ETC_SCHEMA = jsonschema.Draft202012Validator({"$ref": "#/$defs/PseudoIngestRequest"}, resolver=_resolver)
        cls.HJSON_SCHEMA = jsonschema.Draft202012Validator({"$ref": "#/$defs/UnattendedPseudoIngestRequest"}, resolver=_resolver)


    @staticmethod
    def init(conn:Optional[duckdb.DuckDBPyConnection] = None):
        POSTGRES.reload_config()
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
                etc={k:v for k,v in {k:definition.get(k) for k in ('cluster', 'cluster_friendly_name', 'database', 'sql', 'post_process')}.items() if v is not None},
                conn=conn
            )


ConnectionContext = TypeVar('ConnectionContext', covariant=True) #pylint: disable=typevar-name-incorrect-variance
class ConnectionDetail(Protocol[ConnectionContext]):
    def connect(self, *args, **kwargs) -> ConnectionContext:
        """Return a database connector context manager"""
        ...

    def get_dataframe(
        self,
        *args,
        table_name: Optional[str] = None,
        duck_conn: Optional[duckdb.DuckDBPyConnection] = None,
        skip_logging:bool=False, 
        **kwargs
    ) -> Optional[pd.DataFrame]:
        """Get a DataFrame from the specified table."""
        ...
    
    @property
    def friendly_name(self) -> str:
        """Returns human-friendly text name of this connection."""
        ...


class DuckDbConnectionDetail:
    def connect(self, *args, **kwargs) -> duckdb.DuckDBPyConnection:
        return duckdb.connect(DUCKDB.PATH, *args, **kwargs)

    def get_dataframe(self, *args, table_name:Optional[str]=None, sql:Optional[str]=None, skip_logging:bool=False, **kwargs) -> Optional[pd.DataFrame]:
        from .duck import DuckDBMonitorMiddleware  #pylint: disable=import-outside-toplevel
        if all([table_name is None, sql is None]):
            raise ValueError("Either table_name or sql must be provided.")
        sql = sql or f"SELECT * FROM datasets.{table_name};"
        return DuckDBMonitorMiddleware.get_dataframe(sql, skip_logging=skip_logging)

    @property
    def friendly_name(self) -> str:
        return "Flatfiles"


class PostgresConnectionDetail(BaseModel):
    cluster: str
    cluster_friendly_name: Optional[str] = None
    port: int
    username: str
    password: str
    database: str

    def connect(self, *args, **kwargs) -> pg.extensions.connection:
        return pg.connect(
            host=self.cluster,
            port=self.port,
            user=self.username,
            password=self.password,
            database=self.database
        )
    
    def get_dataframe(self, table_name:str, *args, duck_conn:Optional[duckdb.DuckDBPyConnection]=None, skip_logging:bool=False, **kwargs) -> Optional[pd.DataFrame]:
        """
        for convenience, it's just an alias/wrapper for the PostgresMonitorMiddleware singleton
        """
        print(f"PostgresConnectionDetail.get_dataframe({table_name=}, {duck_conn=}, {skip_logging=})")
        from .postgres import PostgresMonitorMiddleware #pylint: disable=import-outside-toplevel
        return PostgresMonitorMiddleware.get_dataframe(table_name, conn=duck_conn, skip_logging=skip_logging)
    
    @property
    def friendly_name(self) -> str:
        return f"PostgreSQL: {self.database}@{self.cluster_friendly_name or self.cluster}"


def get_connection_detail(connection_type:Literal['duck', 'postgres'], table_name:str, etc:Optional[Dict[str, Any]]=None, config:Optional[Dict[str, Any]]=None) -> Optional[ConnectionDetail[Any]]:
    config = config or load_config()
    etc = etc or {}
    try:
        match connection_type:
            case 'duck':
                return DuckDbConnectionDetail()
            case 'postgres':
                return PostgresConnectionDetail(**(
                    functools.reduce(
                        lambda l,r: l | r,
                        [
                            config['PostgreSQL']['clusters'].get(etc['cluster'], {}),  # base cluster properties from main app config
                            {k:etc[k] for k in ('cluster_friendly_name', 'database')}, # supporting properties from etc/individual datasource definition
                        ]
                    )
                    
                ))
            case _:
                return None
    except Exception as e: #pylint:disable=broad-except
        print(f"Error occurred while getting external connection detail: {e.__class__.__name__}: {e}")
        return None


#module global singleton map of key: table_name in the catalog, value: a connection detail object capable of providing connection and friendly name of the backend
map_tables_to_connections: Dict[str, ConnectionDetail[Any]] = {}
def update_connection_map(conn: Optional[duckdb.DuckDBPyConnection] = None):
    supplied_conn = conn is not None
    try:
        conn = conn or duckdb.connect(DUCKDB.PATH, read_only=True)
        from . import DuckDBMonitorMiddleware, PostgresMonitorMiddleware  #pylint: disable=import-outside-toplevel
        global map_tables_to_connections #pylint: disable=global-statement
        duck_details = {tbl: get_connection_detail('duck', tbl, cast(Dict[str, Any], {})) for tbl in DuckDBMonitorMiddleware.ask_available_tables(conn=conn,)}
        duck_details = {k:v for k,v in duck_details.items() if v is not None}
        pg_details = {tbl: get_connection_detail('postgres', tbl, cast(Dict[str, Any], etc)) for (tbl, etc) in PostgresMonitorMiddleware.ask_available_tables(conn=conn, with_etc=True)}
        pg_details = {k:v for k,v in pg_details.items() if v is not None}
        map_tables_to_connections = duck_details | pg_details
    finally:
        if not supplied_conn:
            print("update_connection_map closing its own connection")
            conn.close() #type: ignore


def get_selectable_tables():
    ungrouped_tables = sorted(
        [(detail.friendly_name, table_name) for table_name, detail in map_tables_to_connections.items()],
        key=lambda tup: tup[0]
    )
    selectable_tables = [
        {
            'group': cluster_key,
            'items': [{'value': tbl, 'label': tbl} for _, tbl in tables_in_cluster]
        }
        for cluster_key, tables_in_cluster in itertools.groupby(ungrouped_tables, key=lambda x: x[0])
    ]
    return selectable_tables

def init(wipe:bool=False):
    DUCKDB.init(wipe=wipe)
    with duckdb.connect(DUCKDB.PATH) as conn:
        POSTGRES.init(conn=conn)
        conn.sql('CHECKPOINT;') # for WAL
    update_connection_map()
