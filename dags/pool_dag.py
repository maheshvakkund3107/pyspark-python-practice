from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "airflow",
    "start_date": datetime(2022, 8, 23),
}

with DAG("pools", default_args=default_args, schedule_interval="@once") as dag:
    t1 = BashOperator(task_id="task_1", bash_command="sleep 5", pool="pool_1")
    t2 = BashOperator(task_id="task_2", bash_command="sleep 5", pool="pool_1")
    t3 = BashOperator(task_id="task_3", bash_command="sleep 5", pool="pool_2")
    t4 = BashOperator(task_id="task_4", bash_command="sleep 5", pool="pool_2", priority_weight=2)
