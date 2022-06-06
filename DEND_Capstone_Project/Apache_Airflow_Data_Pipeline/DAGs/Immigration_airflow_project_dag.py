import datetime, timedelta
import os
from airflow import DAG

from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)

from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator

#from airflow.operators import (StageToRedshiftOperator,LoadDimensionOperator, LoadFactOperator, DataQualityOperator)
from helpers import SqlQueries


# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

------------ Creating DAG ------------------------
# are they necessary..?
default_args = {
                 'owner': 'FARHAD',
                 'start_date' : datetime(2022, 4, 4),
                 'depends_on_past: False,
                 'retries' : 5,
                 'retry_delay' : timedelta(minutes=5),
                 'catchup' : False,
                 'email_on_retry': False,
               }

# DAG specification initialized
dag = DAG('Immigration_Airflow_Project_Dag',
           default_args = default_args,
           description = 'Load and transform immigration & other tables in Redshift with Airflow',
           schedule_interval = '0 * * * *'
           start_date = datetime(2022, 5, 1, 0, 0, 0, 0)
          #end_date=datetime(2022, 5, 10), # causes infinite looping

#============================== DAG ============================================================
# starting the DAG execution
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

#------------------Clearing the staging tables----------------------------------------
# Creating and clearing staging table task
Clearing_Staging_Tables_Task = PostgresOperator(
    task_id="Clearing_Staging_Tables_Before_Insertion",
    dag = dag,
    postgres_conn_id = "redshfit",
    sql_query="Clearing_Staging_Tables.sql"
)

   
#---------------ALL....STAGING TABLES---------------------------
#------Staging Fact Immigration table from S3 bucket
Stage_Fact_Immigration_to_redshift = StageToRedshiftOperator(
    task_id="Staging_Immigration_Fact_Table",
    dag=dag,
    
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    
    destination_table="Staging_Fact_Immigration_Table",
    
    s3_bucket="Farhad_Capstone/Saved_Files",
    s3_key="Fact_Immigration_Table.parquet/"  
    provide_context=True
    # s3_path = 's3://claytv-dend-capstone/GlobalLandTemperaturesByCity.csv',
    # copy_options = "DELIMITER',' IGNOREHEADER 1 DATEFORMAT 'YYYY-MM-DD'"
)          

#--- Staging Individual Immigrants Records table from S3 bucket
Stage_Individual_Immigrants_Records_to_redshift = StageToRedshiftOperator(
    task_id="Staging_Individual_Immigrants_Records_Table",
    dag=dag,
    
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    
    destination_table="Staging_Individual_Immigrants_Records",    
    s3_bucket="Farhad_Capstone/Saved_Files",  # My bucket..
    s3_key="Individual_Immigrants_Records.parquet/" 
    provide_context=True
)   
          
#---Staging Dated Arrival and Departure Table from S3 bucket
Stage_Dated_Arrival_Departure_to_redshift = StageToRedshiftOperator(
    task_id="Staging_Individual_Immigrants_Detail_Date_Record",
    dag=dag,
    redshift_conn_id="redshift",
    
    aws_credentials_id="aws_credentials",
    destination_table="Staging_Dated_Arrival_Departure",
    
    s3_bucket="Farhad_Capstone/Saved_Files",
    s3_key="Dated_Arrival_Departure.parquet/"
    provide_context=True
)
          
#---Staging World City Temperature Table from S3 bucket          
Stage_World_Citys_Temp_to_redshift = StageToRedshiftOperator(
    task_id='Staging_World_City_Temperature_Table',
    dag=dag,
    destination_table = "Staging_World_City_Temperature" //?
    
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    
    s3_bucket="Farhad_Capstone/Saved_Files",
    s3_key="World_City_Temperature.parquet/",    
    provide_context=True
)         

#---Staging US City Temperature Table from S3 bucket
Stage_US_Citys_Temp_to_redshift = StageToRedshiftOperator(
    task_id='Staging_US_City_Temperature_table',
    dag=dag,
    destination_table = "Staging_US_City_Temperature" //?
    
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    
    s3_bucket="Farhad_Capstone/Saved_Files",
    s3_key="US_Cities_Tempr.parquet/",  ..??  FILE NAME
    provide_context=True
    
    # s3_path = 's3://claytv-dend-capstone/GlobalLandTemperaturesByCity.csv',
    # copy_options = "DELIMITER',' IGNOREHEADER 1 DATEFORMAT 'YYYY-MM-DD'"
) 
          
Stage_US_Citys_Demog_Race_table_to_redshift = StageToRedshiftOperator(
    task_id='Staging_City_Demog_Race_table',
    dag=dag,
    destination_table = "Staging_US_City_Demog_Race" //?
    
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    
    s3_bucket="Farhad_Capstone/Saved_Files",
    s3_key="US_City_Demog_N_Race.parquet/", ....?? FILE NAME   
    provide_context=True
)    

#------------------PORT LOCATIONS CODE---------------------
# reading Port locations table from my S3 bucket
Stage_Port_Locations_to_redshift = StageToRedshiftOperator(
    task_id='Staging_Port_Locations_Table',
    dag=dag,
    destination_table = "Staging_Port_Locations"
    
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    
    s3_bucket="Farhad_Capstone/Saved_Files",
    s3_key="Port_Locations.parquet/",   ..check file name? 
    provide_context=True
)    

# reading saved Country Codes file from my S3 bucket
Stage_Country_Codes_to_redshift = StageToRedshiftOperator(
    task_id='Staging_Country_Codes_table',
    dag=dag,
    destination_table = "Staging_Country_Codes" 
    
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    
    s3_bucket="Farhad_Capstone/Saved_Files",
    s3_key="Country_Codes.parquet/",   ...check file name?   
    provide_context=True
) 

Stage_State_Codes_to_redshift = StageToRedshiftOperator(
    task_id='Staging_State_Codes_Table',
    dag=dag,
    destination_table = "Staging_State_Codes" //?
    
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    
    s3_bucket="Farhad_Capstone/Saved_Files",
    s3_key="State_Codes.parquet/",    ...check file name?
    provide_context=True
) 
          
done_staging = DummyOperator(task_id= 'Done_staging', dag=dag)
          
#================================= LOADING TABLES FROM STAGING TO ====== lOAD TABLES =========
#--------------------- loading Immigration Fact Table ------------------
Load_Fact_Immigration_Table = LoadDimensionOperator(
    task_id="Loading_Immigration_Fact_Table",
    dag=dag,
    
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    
    destination_table="Fact_Immigration_Table",
    # selecting columns to be infused
    columns_sql = "(Immigration_Id, Immigrants_Age, Citizenship_Country, Residency_Country, Current_State,\
                Visa_Type, Immigration_Year, Immigration_Month, Port_Code, Arrival_Date, Departure_Date,\
                Match_Flag, Birth_Year, Gender, Airline_Code, Admission_Num, Flight_Num, Means_of_Travel, \
                Visa_Purpose)",
    
    sql_query = SqlQueries.Fact_Immigration_Table_insert,
    truncate=True
)

#---------------------loading Individual Immigrants_Records Dimension Table------------------        
Load_dim_Individual_Immigrants_Records_Table = LoadDimensionOperator(
    task_id="Loading_dim_Individual_Immigration_Records",
    dag=dag,
    
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    
    destination_table="dim_Individual_Immigrants_Records", 
    # selecting columns to be inserted to dimension table
    columns_sql = "(Entry_Num, Immigration_Id, Arrival_Date, Citizenship_Country, Immigrants_Age,\
                 Gender, Departure_Date, Visa_Type, Match_Flag)",    
    
    sql_query = SqlQueries.dim_Individual_Immigrants_Records_insert,
    truncate=True
)          
          
#---------------------loading Immigrants Dated Arrival and Departure Dimension Table-----------                  
Load_dim_Dated_Arrival_Departure_Table = LoadDimensionOperator(
    task_id="Loading_dim_Individual_Immigrants_Date_Records",
    dag=dag,
    
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    
    destination_table="dim_Dated_Arrival_Departure", 
    # selecting columns to be inserted to dimension table
    columns_sql = "(Entry_Date, Admission_Num, Citizenship_Country, Arrival_Year, Arrival_Month,\
                 Arrival_Day, Departure_Date, Depart_Year, Depart_Month, Depart_Day, Visa_Type,  Port_Code)",    
    
    sql_query = SqlQueries.dim_Dated_Arrival_Departure_insert,
    truncate=True
)                  
                                  
#---------------------loading World City Temperature Dimension Table-----------  
Load_World_Citys_Temp_Table = LoadDimensionOperator(
    task_id = "Loading_dim_World_City_Temperature_table",
    dag = dag,    
    redshift_conn_id = "redshift",
    
    destination_table = "World_City_Temperature",
    # selecting columns to be inserted to dimension table
    columns_sql = "(Date_Records, City, Country, Year, Month_Name, Month_Num, Day, Avg_Temp)",    
    sql_query = SqlQueries.dim_World_City_Temperature_insert,
    truncate = True
)    
            
#---------------------loading US City Temperature Dimension Table--------------
Load_dim_US_Citys_Temp_Table = LoadDimensionOperator(
    task_id = "Loading_dim_City_Temperature_Dimension_Table",
    dag = dag,    
    redshift_conn_id = "redshift",
    
    destination_table = "dim_US_City_Temperature",
    # selecting columns to be inserted to dimension table
    columns_sql = "(Date_Records, US_City, Country, Year, Month_Name, Month_Num, Day, Avg_Temp, US_Port, Port_States)",    
    sql_query = SqlQueries.dim_US_City_Temperature_insert,
    truncate = True
)    
  
#---------------------loading US City Demography and Race Dimension Table--------------
Load_dim_US_Citys_Demog_Race_Table = LoadDimensionOperator(
    task_id = "Loading_dim_Demography_Dimension_Table",
    dag = dag,   
    redshift_conn_id = "redshift",
    
    destination_table = "dim_US_City_Demog_Race",
    # selecting columns to be inserted to dimension table
    columns_sql = "(US_City, State, State_Code, Male_Population, Female_Population, Total_Population,\
                    Median_Age, Number_Of_Veterans, Foreign_Born, Average_Household_Size, \
                    American_Indian_and_Alaska_Native, Asian_Population, Black_or_African_American, \
                    Hispanic_or_Latino, White_Population, US_Port, Port_States)",
    
    sql_query = SqlQueries.dim_US_City_Demog_Race_insert,
    truncate = True
)    
 
#----------------- PORT + STATE + COUNTRY CODES -------------------------------------                 
#---------------------loading dim Port locations table Dimension Table--------------
Load_dim_Port_Locations_Table = LoadDimensionOperator(
    task_id = "Loading_Port_Locations_Dimension_Table",
    dag = dag,    
    redshift_conn_id = "redshift",
    
    destination_table = "dim_Port_Locations",    
    columns_sql = "(Port_Codes, Port_Citys, Port_States)",
    
    sql_query = SqlQueries.dim_Port_Locations_insert,
    truncate = True
)                     
          
          
#---------------loading dim Country Codes Dimension Table--------------          
Load_dim_Country_Codes_Table = LoadDimensionOperator(
    task_id = "Loading_Country_Codes_Dimension_Table",
    dag = dag,   
    redshift_conn_id = "redshift",
    
    destination_table = "dim_Country_Codes",
    columns_sql = "(Country_Codes, Country_Names)",
    
    sql_query = SqlQueries.dim_Country_Codes_insert,
    truncate = True
)    
          
         
#---------------loading dim State Codes Dimension Table--------------     
Load_dim_State_Codes_Table = LoadDimensionOperator(
    task_id = "Loading_State_Codes_Dimension_Table",
    dag = dag,    
    redshift_conn_id = "redshift",
    
    destination_table = "dim_State_Codes",
    columns_sql = "(State_Codes, State_Names)",
    
    sql_query = SqlQueries.dim_State_Codes_insert,
    truncate = True
)    

          
done_load_fact = DummyOperator(task_id='Done_load_fact', dag=dag)  
          
#============= Data quality checks =====================================
# DATA Quality checking DAG
Run_Data_Quality_Checks = DataQualityOperator(
    task_id='Running_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    
    #tables=["Fact_Immigration_Table", "dim_Individual_Immigrants_Records",\
    #         "dim_Dated_Arrival_Departure",    "dim_World_City_Temperature",\
    #        "dim_US_City_Temperature",       "dim_US_City_Demog_Race",\
    #        "dim_Port_locations", "dim_Country_Codes", "dim_State_Codes"]
    
    # There should not be any null value in primary key column of all these tables...?
    data_quality_checks = [
        {'check_sql': 'SELECT COUNT(*) FROM Fact_Immigration_Table\ 
                                             WHERE Immigration_Id is null','expected_result': 0},
        
        {'check_sql': 'SELECT COUNT(*) FROM dim_Individual_Immigrants_Records\
                                             WHERE Entry_Num is null', 'expected_result': 0},
        {'check_sql': 'SELECT COUNT(*) FROM dim_Dated_Arrival_Departure\
                                             WHERE Entry_Date is null', 'expected_result': 0},
        
        
        {'check_sql': 'SELECT COUNT(*) FROM dim_Port_Locations \
                                        WHERE Port_Codes is null', 'expected_result': 0},
        {'check_sql': 'SELECT COUNT(*) FROM dim_Country_Codes\
                                        WHERE Country_Codes is null', 'expected_result': 0},
        {'check_sql': 'SELECT COUNT(*) FROM dim_State_Codes \
                                        WHERE State_Codes is null', 'expected_result': 0},
                
        
        {'check_sql': 'SELECT COUNT(*) FROM dim_US_City_Temperature \
                                        WHERE US_Port is null', 'expected_result': 0},        
        {'check_sql': 'SELECT COUNT(*) FROM dim_US_City_Demog_Race \
                                        WHERE US_Port is null', 'expected_result': 0},
        
    ]
)

#..?          
#done_staging = DummyOperator(task_id= 'Done_staging', dag=dag)
#done_load_fact = DummyOperator(task_id='Done_load_fact', dag=dag)          

# ending task execution
end_operator = DummyOperator(task_id = 'Stop_execution', dag=dag)    
          
          
#-----------------------------------------------------------------------------------------
# DAG sequencing steps

# Starting DAG ...
# Staging DAG's to loading  fact table to loading dimension tables DAG
# Followed by a 'running data quality check' DAG to 'stopping execution' of all DAGs.
          
start_operator >> clear_staging_tables_task # (Clearing_Staging_Tables_Task?)

# staging tables to loading FACT and DIMENSION DAG          
clear_staging_tables_task >> Stage_Fact_Immigration_to_redshift
clear_staging_tables_task >> Stage_Individual_Immigrants_Records_to_redshift 
clear_staging_tables_task >> Stage_Dated_Arrival_Departure_to_redshift

clear_staging_tables_task >> Stage_World_Citys_Temp_to_redshift
clear_staging_tables_task >> Stage_US_Citys_Temp_to_redshift               
clear_staging_tables_task >> Stage_US_Citys_Demog_Race_table_to_redshift
          
clear_staging_tables_task >>  Stage_Port_Locations_to_redshift
clear_staging_tables_task >>  Stage_Country_Codes_to_redshift
clear_staging_tables_task >>  Stage_State_Codes_to_redshift


# loading data from Staging tables to Loading tables?
Stage_Fact_immigration_to_redshift  >>  Load_Fact_immigration_Table
Stage_Individual_Immigrants_to_redshift >> Load_dim_Individual_Immigrants_Records_Table
Stage_Dated_Arrival_Departure_to_redshift >>  Load_dim_Dated_Arrival_Departure_table 
          
Stage_World_Citys_Temp_to_redshift  >> Load_World_Citys_Temp_Table
Stage_US_Citys_Temp_to_redshift >> Load_dim_US_Citys_Temp_Table
Stage_US_City_Demog_Race_table_to_redshift >> Load_dim_US_Citys_Demog_Race_Table

# Loading port tables
Stage_Port_Locations_to_redshift >> Load_dim_Port_Locations_Table
Stage_Country_Codes_to_redshift  >> Load_dim_Country_Codes_Table
Stage_State_Codes_to_redshift    >> Load_dim_State_Codes_Table

# running data quality checks on all fact an dimension tables
Load_Fact_Immigration_Table  >>  Run_Data_Quality_Checks
Load_dim_Individual_Immigrants_Records_Table    >>  Run_Data_Quality_Checks
Load_dim_Dated_Arrival_Departure_Table  >>  Run_Data_Quality_Checks

Load_World_Citys_Temp_Table  >> Run_Data_Quality_Checks 
Load_dim_US_Citys_Temp_Table >> Run_Data_Quality_Checks
Load_dim_US_Citys_Demog_Race_Table  >> Run_Data_Quality_Checks
Load_dim_Port_Locations_Table      >> Run_Data_Quality_Checks
Load_dim_Country_Codes_Table       >> Run_Data_Quality_Checks  
Load_dim_State_Codes_Table         >> Run_Data_Quality_Checks
          
# end of DAG operation
Run_Data_Quality_Checks >> end_operator         
  
          
