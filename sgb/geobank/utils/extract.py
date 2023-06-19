from os import path, makedirs

from airflow.providers.oracle.operators.oracle import OracleHook
from airflow.hooks.filesystem import FSHook
# from airflow.providers.postgres.operators.postgres import PostgresHook


def get_st_geometry_tables(oracle_conn_id, filesystem_id, **kwargs):
    """
    """
    start = kwargs.get('dag_run').logical_date
    #"airflow_lake_fspath"

    dest_path = path.join(
        FSHook(filesystem_id).basepath, 
        'geobank-ora2pgsql',
        start.strftime("%Y/%m/%d")
    )
    
    makedirs(dest_path, exist_ok=True)

    pkl = path.join(dest_path, "st_geometry_tables.pkl")

    try:
        (
            OracleHook(oracle_conn_id).get_pandas_df(
                "SELECT OWNER, TABLE_NAME, COLUMN_NAME, SRID, GEOMETRY_TYPE "
                "FROM SDE.ALL_ST_GEOMETRY_COLUMNS_V WHERE TABLE_NAME LIKE '%LAYER%' "
                "ORDER BY OWNER, TABLE_NAME"
            )
            .rename(
                columns=lambda col: col.lower()
            )
            .to_pickle(
                pkl
            )
        )
    
    except Exception as e:
        raise e
    
    return pkl
