from os import environ, path, makedirs

from airflow.providers.oracle.operators.oracle import OracleHook
# from airflow.providers.postgres.operators.postgres import PostgresHook


def get_st_geometry_tables(oracle_conn_id, **kwargs):
    """
    """
    start = kwargs.get('dag_run').logical_date

    dest_path = path.join(
        environ.get("AIRFLOW_HOME", "/tmp"), 
        start.strftime("%Y/%m/%d"), 
        "st_geometry_tables.pkl"
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
