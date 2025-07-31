from airflow import DAG
from airflow.models import Variable

from airflow.operators.python import PythonOperator
# from airflow.operators.python import BranchPythonOperator
# from airflow.operators.empty import EmptyOperator

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from pendulum import datetime

from sgb.geobank.utils import extract


default_args = {
    "email":["carlos.mota@sgb.gov.br"],
    "email_on_failure": False
}

dbms_src_id = Variable.get("sgb_geobank_db")
dbms_dst_id = Variable.get("sgb_opendata_db")
fs_id = Variable.get("airflow_lake_fspath")


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
    
    read_oracle = SQLExecuteQueryOperator(
        task_id=f"{dag.dag_id}_read_oracle",
        conn_id=dbms_src_id,
        sql="SELECT 1 FROM dual"
    )

    transform = PythonOperator(
        task_id=f"{dag.dag_id}_transform",
        python_callable=extract.get_st_geometry_tables,
        op_args=[dbms_src_id, fs_id]
    )

    write_postgres = SQLExecuteQueryOperator(
        task_id=f"{dag.dag_id}_write_postgres",
        conn_id=dbms_dst_id,
        sql="SELECT 1"
    )

    read_oracle >> transform >> write_postgres
    