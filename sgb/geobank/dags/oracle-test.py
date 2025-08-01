import geopandas as gpd

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from sgb.common.airflow.hooks.sde import SDEOracleHook


# DAG
dag_id = "test-oracle"

def extract_table(**kwargs):
    hook = SDEOracleHook(oracle_conn_id="geobank-producao")
    table = hook.get_reflected_table('litoestratigrafia', 'ue_layer_100000')

    results = hook.get_geopandas_df_by_chunks(
        str(table.select()), 
        geom_col=table.columns['shape'].name, 
        crs=table.columns['shape'].type.srid, 
        index_col=table.primary_key.columns[0].name,
        chunksize=5000
    )

    out_file = "/tmp/ue_layer_100000.parquet"
    
    for df in results:
        df.to_parquet(out_file, index=True)

    return out_file


with DAG(
    dag_id=dag_id,
    start_date=datetime(2025, 7, 31),
    schedule=None,
    catchup=False
) as dag:
    extract_table = PythonOperator(
        task_id=f"{dag_id}_extract_table",
        python_callable=extract_table
    ) # type: ignore

if __name__ == '__main__':
    dag.test()