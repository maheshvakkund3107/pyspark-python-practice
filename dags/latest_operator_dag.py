from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.latest_only import LatestOnlyOperator

dag = DAG(dag_id="latest_only_dag", start_date=datetime(2022, 8, 23), catchup=True,
          schedule_interval=timedelta(hours=6))

t1 = LatestOnlyOperator(task_id="latest_op")
t2 = DummyOperator(task_id="task2", dag=dag)
t3 = DummyOperator(task_id="task3", dag=dag)
t4 = DummyOperator(task_id="task4", dag=dag)
t5 = DummyOperator(task_id="task5", dag=dag)

t1 >> [t2, t4, t5]
