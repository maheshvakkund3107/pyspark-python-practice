import datetime

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

with DAG(
        dag_id="postgres_operator_dag",
        start_date=datetime.datetime(2022, 2, 17),
        schedule_interval="@once",
        catchup=False,
) as dag:
    create_pet_table = PostgresOperator(
        task_id="create_pet_table",
        sql="""
        CREATE TABLE IF NOT EXISTS pet (
        pet_id SERIAL PRIMARY KEY,
        name VARCHAR NOT NULL,
        pet_type VARCHAR NOT NULL,
        birth_date DATE NOT NULL,
        OWNER VARCHAR NOT NULL) ;
        """
    )

    populate_pet_table = PostgresOperator(
        task_id="populate_pet_table",
        sql="""
        INSERT INTO pet(name,pet_type, birth_date, OWNER)
        VALUES('Max','Dog','2018-07-05','Jane');
        INSERT INTO pet(name,pet_type, birth_date, OWNER)
        VALUES('Sussie','Cat','2018-07-06','Phil');
        INSERT INTO pet(name,pet_type, birth_date, OWNER)
        VALUES('Lester','Hamster','2018-07-07','Lilly');
        INSERT INTO pet(name,pet_type, birth_date, OWNER)
        VALUES('Quincy','Parrot','2018-07-08','Anne');
        """)
    get_all_pets = PostgresOperator(task_id="get_all_pets", sql="SELECT * FROM pet;")

    create_pet_table >> populate_pet_table >> get_all_pets
