import geopandas as gpd

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.oracle.hooks.oracle import OracleHook
from datetime import datetime
from sqlalchemy import text


# make oracledb work with sqlalchemy 1.4
# https://stackoverflow.com/questions/74093231/nosuchmoduleerror-cant-load-plugin-sqlalchemy-dialectsoracle-oracledb
import sys
import oracledb
oracledb.init_oracle_client() # força inicialização do sdk oracle (thin)
oracledb.version = "8.3.0"
sys.modules["cx_Oracle"] = oracledb

class CustomOracleHook(OracleHook):
    @property
    def sqlalchemy_url(self):
        conn = self.connection
        return f"oracle://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.extra_dejson['service_name']}"


# DAG
dag_id = "test-oracle"

def sde_version(**kwargs):
    engine = CustomOracleHook(oracle_conn_id="geobank-producao").get_sqlalchemy_engine()
  
    with engine.connect() as conn:
        _table = 'sde.sde_version'

        if conn.dialect.name == 'oracle':
            _table = 'sde.version'        

        major, minor, bugfix = conn.execute(
            text(f"SELECT major, minor, bugfix FROM {_table}")
        ).fetchone()

    return major, minor, bugfix


def extract_table(**kwargs):
    engine = CustomOracleHook(oracle_conn_id="geobank-producao").get_sqlalchemy_engine()
    
    with engine.connect() as conn:
        sql = """SELECT objectid, id_unidade_estratigrafica, sigla, hierarquia, nome, ambiente_tectonico, sub_ambiente_tectonico, 
              sigla_pai, nome_pai, legenda, escala, mapa, litotipos, range, idade_min, idade_max, eon_min, eon_max, era_min, 
              era_max, sistema_min, sistema_max, epoca_min, epoca_max, SDE.ST_ASBINARY(shape) AS geometry, siglas_historicas 
              FROM litoestratigrafia.ue_layer_25000"""  

        geodata = (
            gpd.read_postgis(
                sql, 
                conn,
                geom_col = "geometry",
                crs = 4326,  # EPSG:4326
            )
            .rename(columns=lambda col: col.lower())
            .rename(columns={"objectid": "fid"})
            .set_index("fid")
        )

    geodata.to_parquet("/tmp/ue_layer_25000.parquet", index=True)

    return geodata.info()


with DAG(
    dag_id=dag_id,
    start_date=datetime(2025, 7, 31),
    schedule="@once",
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