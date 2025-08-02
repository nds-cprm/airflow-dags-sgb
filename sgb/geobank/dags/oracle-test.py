import logging

logger = logging.getLogger(__name__)

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pandas import concat

from sgb.common.airflow.hooks.sde import SDEOracleHook


# DAG
def dag_factory(dag_id, schema, table):
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

        out_file = f"/tmp/{_schema}_{_table}.parquet"

        # concatena os DataFrames obtidos pelo iterador e salva em um arquivo Parquet
        out_df = concat([df.set_index(table.columns['objectid'].name) for df in results]).to_parquet(out_file, index=True)
        logger.info(out_df.info())  # Log the DataFrame info for debugging

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


# selected_tables = {
#     "litoestratigrafia": ['ue_layer_100000', 'ue_layer_250000', 'ue_layer_50000', 'ue_layer_1000000'],
#     "aflora": ['af_layer'],
#     "recmin": ['rm_layer'],
# }


# for schema, tables in selected_tables.items():
#     for table in tables:
#         dag_id = f"oracle_{schema}_{table}_etl"
#         globals().update({
#             dag_id: dag_factory(dag_id, schema, table)
        # })

dag = dag_factory("oracle-test", "litoestratigrafia", 'ue_layer_100000')