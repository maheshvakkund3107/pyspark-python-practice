from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

default_arguments = {
    "owner": "airflow",
    "start_date": datetime(2022, 8, 1)
}
dag = DAG(dag_id="simple_xcom",
          default_args=default_arguments,
          schedule_interval="@daily")


def push_function(**kwargs):
    message = 'This is a pushed message'
    ti = kwargs['ti']
    ti.xcom_push(key="mymessage", value=message)


def pull_function(**kwargs):
    ti = kwargs['ti']
    pulled_message = ti.xcom_pull(key="mymessage", task_ids="push_task")
    print("Pulled message is : '%s' " % pulled_message)


t1 = PythonOperator(task_id="push_task",
                    python_callable=push_function,
                    provide_context=True,
                    dag=dag)

t2 = PythonOperator(task_id="pull_task",
                    python_callable=pull_function,
                    provide_context=True,
                    dag=dag)
t1 >> t2
