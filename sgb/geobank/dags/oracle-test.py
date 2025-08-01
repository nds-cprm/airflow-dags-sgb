from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from sgb.common.airflow.hooks.sde import SDEOracleHook


# DAG
def dag_factory(schema, table):
    dag_id = f"{schema}_{table}_etl"

    def extract_table(_schema, _table, **kwargs):
        hook = SDEOracleHook(oracle_conn_id="geobank-producao")
        table = hook.get_reflected_table(_schema, _table)

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
    
    # Create the DAG
    with DAG(
        dag_id=dag_id,
        start_date=datetime(2025, 7, 31),
        schedule=None,
        catchup=False
    ) as dag:
        extract_table = PythonOperator(
            task_id=f"{dag_id}_extract", 
            python_callable=extract_table,
            op_args=(schema, table),
        ) # type: ignore
    
    return dag


selected_tables = {
    "litoestratigrafia": ['ue_layer_100000', 'ue_layer_250000', 'ue_layer_50000', 'ue_layer_1000000'],
    "aflora": ['af_layer'],
    "recmin": ['rm_layer'],
}

for schema, tables in selected_tables.items():
    for table in tables:
        dag_factory(schema, table)
