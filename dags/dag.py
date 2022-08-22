from airflow import DAG
from datetime import datetime, timedelta

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from random import randint

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

dag = DAG("first_dag",
          default_args=default_arguments,
          schedule_interval=timedelta(1))
t1 = BashOperator(task_id="print_date", bash_command="date", dag=dag)
t2 = BashOperator(task_id="sleep", bash_command="date", retries=3, dag=dag)

template_command = """
{% for i in range(5) %}
echo "{{ds}}"
echo "{{macros.ds_add(ds,7)}}"
echo "{{params.myparam}}"
{% endfor %}
"""

t3 = BashOperator(task_id="template_code", bash_command=template_command, params={"myparam": "param"}, dag=dag)


def training_model():
    return randint(1, 10)


t4 = PythonOperator(task_id="training_model", python_callable=training_model, dag=dag)
t2 << t1
t3 << t1
t3 >> t4
