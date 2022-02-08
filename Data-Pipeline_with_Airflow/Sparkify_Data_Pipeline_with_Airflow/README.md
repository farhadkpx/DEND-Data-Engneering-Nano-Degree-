<p align="center">
  <img src="https://github.com/farhadkpx/DEND-Data-Engneering-Nano-Degree-/blob/main/Data-Pipeline_with_Airflow/Sparkify_Data_Pipeline_with_Airflow/Airflow_logo_01.png"/>
</p>

## Sparkify:  Data Pipeline Project with Apache Airflow

**Udacity Nanodegree Course Project:**
> Here is my Airflow repository: [link](https://github.com/farhadkpx/DEND-Data-Engneering-Nano-Degree-/tree/main/Data-Pipeline_with_Airflow/Sparkify_Data_Pipeline_with_Airflow)

### `Project context:`

Sparkify, the music streaming company has decided that it will introduce more automation and monitoring to their data warehouse ETL pipelines. Their conclusion is the best tool to achieve this goal is to use Apache Airflow data pipeline. The pipeline should be dynamic and built from reusable operator tasks, easily monitored, and will allow easy backfills. 

They have also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The project source data resides in Amazon S3 bucket and needs to be processed in Sparkify's data warehouse, which is situated in Amazon Redshift. The s3 source datasets consist of CSV-logs that contains user activity in the application and JSON metadata about the songs the users listen to.

### `Project Description:`
We’ll build the Apache Airflow ETL data pipeline from  a data-source bucket hosted on Amazon S3. We’re given a DAG(.py) file and four  operators(.py) files and a helper(.py) files to help build the whole dynamic pipelines. These 5 files are partially coded and we're given the opportunity to customize them according to the project requirements.

`1.` udac_example_dag.py ( DAG.py file )

`2.` stage_redshift.py( StageToRedshiftOperator )

`3.` load_fact.py( LoadFactOperator )

`4.` load_dimension.py( LoadDimensionOperator )

`5.` data_quality.py ( DataQualityOperator )

`6.` sql_queries.py ( Helper SQL function )

These operator files are customizeable and we'll use them to build all the necessary DAG's for this project. Proper interconnection and sourcing is necessary to builds the Airflow DAG.

### `Data Source:`
Two main data source are located in on Amazon s3 bucket. 

+ JSON files containing log events data originated from the Sparkify app users: : **`Log data:` `s3://udacity-dend/log_data`**
+ JSON files containing meta information about song and artists data: **`Song data:` `s3://udacity-dend/song_data`**

**`Quick view:` How the DAG's should be sequenced for this project data pipeline**

![image](https://github.com/farhadkpx/DEND-Data-Engneering-Nano-Degree-/blob/main/Data-Pipeline_with_Airflow/Sparkify_Data_Pipeline_with_Airflow/Dag_Dependency_Steps.png)


## `Operators`
Each operator has to follow some strict guideline to function for this project. Operators will help to stage the data, transform the data, and run checks on data-quality and creates needed data tables. AWS and Redshift connectors help to built connection with AWS and Redshift platforms. Hooks are the programming plug-ins to use with DAG. All of the operators and task run SQL statements against the Redshift database.

### `Start and End Operators`
These two operators actually do not perform any real DAG-operational task. They create a clear visual distinction off the other DAGs used in the airflow UI.

### `Stage Operators`
The stage operator loads JSON formatted files from S3 to Amazon Redshift. The operator creates and runs a SQL COPY statement based on the parameters provided. The operator's parameters should specify where in S3 the file is loaded and what is the target table.

### `Fact and Dimension Load Operators`
The dimension and fact operators make use of the SQL helper class to run data transformations. Operators take as input the SQL statement from the helper class and target the database on which to run the query against. We’ll define a target table that will contains the results of the data transformation.

### `Data Quality Operator`
The data quality operator is used to run checks on the data itself. The operator's main functionality is to receive one or more SQL based test cases along with the expected results and execute the tests. For each the test, the test result and expected result are checked and if there is no match, the operator raises an exception and the task is retried until it fails eventually.

## `Airflow UI visualization DAGS:` 
We’ll create `“Stage events”` and `“Stage_song”`  DAGS with the help of  `“StageToRedshiftOperator”`. Also `“load_songplays_table”` DAG with the help of `“LoadFactOperator”` operator. 

We’ll develope 4 dimensional DAGS `'Load_user_dim_table'`, `'Load_song_dim_table'`, `'Load_artist_dim_table'`, `'Load_time_dim_table'` DAGs with the help of `“Load_DimensionOperator”`.

The last  DAG `“Run_data_quality_checks”` will be created with the help of `“DataQualityOperator”`.

We can say operators are the reusable, flexible and configurable module of creating DAGS. Final setting will be arranging the DAGS dependency in the order required by the project.
	


