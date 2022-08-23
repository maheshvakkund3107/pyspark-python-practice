from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor

default_arguments = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2022, 7, 27),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

dag = DAG("file_sensor",
          default_args=default_arguments,
          schedule_interval=timedelta(1))

f1 = FileSensor(task_id="file_chk", filepath="/opt/airflow/webserver_config.py",
                fs_conn_id="file_conn", poke_interval=10, timeout=100,
                dag=dag)
t1 = BashOperator(task_id="print_date", bash_command="date", dag=dag)
f1 >> t1
