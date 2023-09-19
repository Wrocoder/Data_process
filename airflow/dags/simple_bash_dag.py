import logging
import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

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

with DAG(dag_id="test_simple_bash_dag",
         default_args=default_args,
         schedule_interval=None,
         tags=["my_dags"],
         ) as dag:

    file = f"{default_args['home']}/my_bash_file.txt"
    file_new = f"{default_args['home']}/new_file.txt"

    info = "This is some information."
    date_info = f"Generated on: {default_args['start_date']}"
    output_content = f"{info}\n{date_info}"

    t1 = BashOperator(bash_command=f"touch {file}", task_id="create_file")

    t2 = BashOperator(bash_command=f'echo -e "{output_content}" > "{file}"', task_id="load_info")

    t3 = BashOperator(bash_command=f"mv {file} {file_new}", task_id="change_file_name")

    t1 >> t2 >> t3
