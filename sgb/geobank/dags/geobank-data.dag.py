from airflow import DAG
from airflow.models import Variable

from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator

from airflow.providers.oracle.operators.oracle import OracleOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from pendulum import datetime


default_args = {
    "email":["carlos.mota@sgb.gov.br"],
    "email_on_failure": False
}

dbms_src_id = Variable.get("sgb_geobank_prod")
dbms_dst_id = Variable.get("sgb_opendata_prod")


with DAG (
    'p3m_etl', 
    default_args = {
        "email":["carlos.mota@sgb.gov.br"],
        "email_on_failure": False
    },
    start_date = datetime(2023, 7, 1),
    schedule_interval = None, 
    catchup = False
) as dag:
    
    read_oracle = OracleOperator(
        task_id="geobank_read_oracle",
        oracle_conn_id=dbms_src_id,
        sql="SELECT 1 FROM dual"
    )

    write_postgres = PostgresOperator(
        task_id="geobank_write_postgres",
        postgres_conn_id=dbms_dst_id,
        sql="SELECT 1 FROM dual"
    )
    