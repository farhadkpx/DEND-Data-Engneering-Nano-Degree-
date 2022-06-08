import datetime
import os
from airflow import DAG

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator

# creating DAG task for dropping and creating tables
dag = DAG('Table_Creation_dag',
          description='Drop and Create Tables in Redshift using Airflow',
          schedule_interval=None,
          start_date=datetime.datetime.now()
         )

Drop_Tables_Task = PostgresOperator(
    task_id = "Dropping_Table_Task",
    dag = dag,
    postgres_conn_id = "redshfit",
    sql = "Drop_Tables.sql"
)


Create_Tables_Task = PostgresOperator(
    task_id = "Create_Tables_Task",
    dag = dag,
    postgres_conn_id="redshfit",
    sql = "Create_Tables.sql"
)

Drop_Tables_Task >> Create_Tables_Task
#-----------------------------------------------------------------------------------
#with DAG('create_table_dag', 
#         start_date=datetime.datetime.now(),
#         default_args=default_args) as dag:
    
#    PostgresOperator(
#        task_id="create_table",
#        dag=dag,
#        postgres_conn_id="redshift",
#        sql='create_tables.sql'
#    )
