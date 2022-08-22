from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "airflow",
    "start_date": datetime(2022, 8, 1)
}
dag = DAG("variable1", default_args=default_args, schedule_interval=timedelta(1))
t1 = BashOperator(task_id="print_path", bash_command="echo ((var.value.source path}]", dag=dag)
