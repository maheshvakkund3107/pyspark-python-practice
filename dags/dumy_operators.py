from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

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

dag = DAG("dummy_dag",
          default_args=default_arguments,
          schedule_interval=timedelta(1))
t1 = BashOperator(task_id="print_date1", bash_command="date", dag=dag)
t2 = BashOperator(task_id="print_date2", bash_command="date", dag=dag)
t3 = BashOperator(task_id="print_date3", bash_command="date", dag=dag)
t4 = BashOperator(task_id="print_date4", bash_command="date", dag=dag)
t5 = BashOperator(task_id="print_date5", bash_command="date", dag=dag)

t6 = BashOperator(task_id="print_hello1", bash_command="echo 'Hello' ", dag=dag)
t7 = BashOperator(task_id="print_hello2", bash_command="echo 'Hello' ", dag=dag)
t8 = BashOperator(task_id="print_hello3", bash_command="echo 'Hello' ", dag=dag)
t9 = BashOperator(task_id="print_hello4", bash_command="echo 'Hello' ", dag=dag)
t10 = BashOperator(task_id="print_hello5", bash_command="echo 'Hello' ", dag=dag)

td = DummyOperator(task_id="dummy1", dag=dag)
[t1, t2, t3, t4, t5] >> td >> [t6, t7, t8, t9, t10]
