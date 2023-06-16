from airflow import DAG
from airflow.models import Variable

from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator

from airflow.providers.oracle.operators.oracle import OracleOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from pendulum import datetime

from sgb.geobank.utils import extract


default_args = {
    "email":["carlos.mota@sgb.gov.br"],
    "email_on_failure": False
}

dbms_src_id = Variable.get("sgb_geobank_db")
dbms_dst_id = Variable.get("sgb_opendata_db")


with DAG (
    'geobank', 
    default_args = {
        "email":["carlos.mota@sgb.gov.br"],
        "email_on_failure": False
    },
    start_date = datetime(2023, 6, 1),
    schedule_interval = None, 
    catchup = False
) as dag:
    
    read_oracle = OracleOperator(
        task_id=f"{dag.dag_id}_read_oracle",
        oracle_conn_id=dbms_src_id,
        sql="SELECT 1 FROM dual"
    )

    transform = PythonOperator(
        task_id=f"{dag.dag_id}_transform",
        python_callable=extract.get_st_geometry_tables,
        op_args=[dbms_src_id]
    )

    write_postgres = PostgresOperator(
        task_id=f"{dag.dag_id}_write_postgres",
        postgres_conn_id=dbms_dst_id,
        sql="SELECT 1"
    )

    read_oracle >> transform >> write_postgres
    