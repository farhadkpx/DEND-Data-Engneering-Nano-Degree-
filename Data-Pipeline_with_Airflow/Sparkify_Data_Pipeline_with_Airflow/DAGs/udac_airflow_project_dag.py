from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)

from airflow.operators.postgres_operator import PostgresOperator
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

# DAG arguments
default_args = {
    'owner': 'udacity',
    'start_date': datetime(2018, 1, 1),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

# DAG specification initialized
dag = DAG('udac_airflow_project_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          #end_date=datetime(2022, 2, 10), # causes infinite looping
          schedule_interval='0 * * * *'
          
        )

#================================================================================
# starting the DAG execution
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

#  Using task is against the project Rubric. So this task goes to a separate DAG
#  Calling the "create_tables.sql" file for table creation
#  create_tables_task = PostgresOperator(
#  task_id="create_tables",
#  dag=dag,
#  sql='create_tables.sql',
#  postgres_conn_id="redshift"
#  )

#------------------------------------------------------------
# staging events(log data file) DAG
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table="staging_events",
    
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    
    s3_bucket="udacity-dend",
    s3_key="log_data",
    
    file_format="json",
    json_path="s3://udacity-dend/log_json_path.json",
    provide_context=True
)

# staging songs(song data file) DAG
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table="staging_songs",
    
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    
    s3_bucket="udacity-dend",
    s3_key="song_data/A",
    
    file_format="json",
    json_path='auto',
    provide_context=True
)

#===============================================================
# songplays table DAG
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    
    table="songplays",
    sql_query=SqlQueries.songplay_table_insert
)

# user table DAG
load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,  
    redshift_conn_id="redshift",
    
    table="users",
    sql_query=SqlQueries.user_table_insert,
    truncate=True
)

# song table DAG
load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    
    table="songs",
    sql_query=SqlQueries.song_table_insert,
    truncate=True
)

# artist table DAG
load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    
    table="artists",
    sql_query=SqlQueries.artist_table_insert,
    truncate=True
)

# time table DAG
load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    
    table="time",
    sql_query=SqlQueries.time_table_insert,
    truncate=True
)

#--------------------------------------------------------------------
# DATAT Quality checking DAG
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tables=['songplays', 'users', 'songs', 'artists', 'time'],
)

# end operator
end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

#===================================================================
# DAG sequencing steps
# starting to staging songs and events DAG
start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

# staging tables to loading FACT-tables DAG
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

# Fact table to dimension table DAGS
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

# running data quality checks on all dimension tables
load_song_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

# end of DAG operation
run_quality_checks >> end_operator