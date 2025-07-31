import geopandas as gpd

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from sgb.common.airflow.hooks.sde import SDEOracleHook


# DAG
dag_id = "test-oracle"

def sde_version(**kwargs):
    return SDEOracleHook(oracle_conn_id="geobank-producao").sde_version


def extract_table(**kwargs):
    engine = SDEOracleHook(oracle_conn_id="geobank-producao").get_sqlalchemy_engine()
    
    with engine.connect() as conn:
        sql = """SELECT objectid, id_unidade_estratigrafica, sigla, hierarquia, nome, ambiente_tectonico, sub_ambiente_tectonico, 
              sigla_pai, nome_pai, legenda, escala, mapa, litotipos, range, idade_min, idade_max, eon_min, eon_max, era_min, 
              era_max, sistema_min, sistema_max, epoca_min, epoca_max, SDE.ST_ASBINARY(shape) AS geometry, siglas_historicas 
              FROM litoestratigrafia.ue_layer_25000"""  

        geodata = (
            gpd.read_postgis(sql, conn, geom_col="geometry", crs=4326)
            .rename(columns=lambda col: col.lower())
            .rename(columns={"objectid": "fid"})
            .set_index("fid")
        )

    geodata.to_parquet("/tmp/ue_layer_25000.parquet", index=True)

    return geodata.info()


with DAG(
    dag_id=dag_id,
    start_date=datetime(2025, 7, 31),
    schedule=None,
    catchup=False
) as dag:
    sde_version = PythonOperator(
        task_id=f"{dag_id}_sde_version",
        python_callable=sde_version
    )

    extract_table = PythonOperator(
        task_id=f"{dag_id}_extract_table",
        python_callable=extract_table
    )

    sde_version >> extract_table

if __name__ == '__main__':
    dag.test()