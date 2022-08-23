from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator

default_arguments = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2022, 8, 23),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}


def push_function(**kwargs):
    pushed_value = 6
    ti = kwargs['ti']
    ti.xcom_push(key="pushed_value", value=pushed_value)


def branch_function(**kwargs):
    ti = kwargs['ti']
    pulled_value = ti.xcom_pull(key="pushed_value", task_ids='push_task')
    if pulled_value % 2 == 0:
        return 'even_task'
    else:
        return 'odd_task'


dag = DAG("branching_dag",
          default_args=default_arguments,
          schedule_interval=timedelta(1))

push_task = PythonOperator(task_id="push_task",
                           python_callable=push_function,
                           provide_context=True,
                           dag=dag)

branch_task = BranchPythonOperator(task_id="branch_task",
                                   python_callable=branch_function,
                                   provide_context=True,
                                   dag=dag)

even_task = BashOperator(task_id="even_task",
                         bash_command="echo 'recieved even value'",
                         dag=dag)

odd_task = BashOperator(task_id="odd_task",
                        bash_command="echo 'recieved odd value'",
                        dag=dag)
push_task >> branch_task >> [even_task, odd_task]
