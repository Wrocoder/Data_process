import logging
import os
import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.append(f"{os.environ['HOME']}/PycharmProjects/Data_process")
from jobs import univercity_api, univercity_stg
# get the airflow.task logger
task_logger = logging.getLogger("airflow.task")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 6, 27),
    'email': ['iam@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'home': f"{os.environ['HOME']}/PycharmProjects/Data_process/sample/"
}
for k, v in os.environ.items():
    task_logger.info(f'{k}={v}')

with DAG(dag_id="dag_etl_university",
         default_args=default_args,
         schedule_interval=None,
         tags=["my_dags"],
         ) as dag:

    t1 = PythonOperator(
        task_id="extract_data_via_api",
        python_callable=univercity_api.uni_main,
        dag=dag,
    )
    t2 = PythonOperator(
        task_id="csv_to_parquet_processing",
        python_callable=univercity_stg.uni_main_stg,
        dag=dag,
    )

    t1 >> t2
