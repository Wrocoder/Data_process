import logging
import os
import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator

sys.path.append(f"{os.environ['HOME']}/PycharmProjects/Data_process")
from jobs import univercity_api, univercity_stg, geo_api, geo_stg, geo_dm, university_dm, \
    university_rank_dm, university_rank_stg  # noqa: E402

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
        task_id="extract_data_via_api_university",
        python_callable=univercity_api.uni_main,
        dag=dag,
    )
    t2 = PythonOperator(
        task_id="extract_data_via_api_geo",
        python_callable=geo_api.geo_main,
        dag=dag,
    )
    t3 = PythonOperator(
        task_id="csv_to_parquet_processing_university",
        python_callable=univercity_stg.uni_main_stg,
        do_xcom_push=True,
        dag=dag,
    )
    t4 = PythonOperator(
        task_id="csv_to_parquet_processing_geo",
        python_callable=geo_stg.geo_main_stg,
        do_xcom_push=True,
        dag=dag,
    )
    t5 = PythonOperator(
        task_id="csv_to_parquet_processing_university_rank",
        python_callable=university_rank_stg.uni_rank_stg,
        do_xcom_push=True,
        dag=dag,
    )
    t6 = PythonOperator(
        task_id="csv_to_parquet_processing_geo_dm",
        python_callable=geo_dm.geo_main_dm,
        do_xcom_push=True,
        dag=dag,
    )
    t7 = PythonOperator(
        task_id="csv_to_parquet_processing_university_dm",
        python_callable=university_dm.university_main_dm,
        do_xcom_push=True,
        dag=dag,
    )
    t8 = PythonOperator(
        task_id="csv_to_parquet_processing_university_rank_dm",
        python_callable=university_rank_dm.univ_rank_main_dm,
        do_xcom_push=True,
        dag=dag,
    )

    email = EmailOperator(
        task_id='send_email',
        to='my_mail@gmail.com',
        subject='Airflow Alert',
        html_content=""" <h3>Email Test</h3> """,
        dag=dag
    )

    t1 >> t3 >> t7 >> email
    t2 >> t4 >> t6 >> email
    t5 >> t8 >> email
