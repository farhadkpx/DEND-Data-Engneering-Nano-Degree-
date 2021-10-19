
## `Project: Data Modeling with Cassandra`

`Contexture of the project:` A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analysis team is particularly interested in understanding what songs users are listening to in types of patterns. Currently, there is no easy way to query the data to generate the results, since the data reside in a directory of CSV files on user activity on the app. Company wants a data engineer to create an Apache Cassandra database on which they can run queries on song play data to answer the certain questions.

### `Project Overview`
In this project, we'll apply what you've learned on data modeling with Apache Cassandra and complete an ETL pipeline using Python programming lanugage. To complete the project, we will need to model your data by creating tables in Apache Cassandra to run queries. We are provided with part of the ETL pipeline that transfers data from a set of CSV files within a directory to create a streamlined CSV file to insert data into Apache Cassandra tables. A project template was given in  python notebook that takes care of all the imports and provides a structure for ETL pipeline we'd need to process this data.

**Datasets** : For this project, we're given a working dataset: `event_data`

### `Project Template`
To get started with the project, go to the workspace on the next page, where you'll find the project template (a Jupyter notebook file). You can work on your project and submit your work through this workspace.

The project template includes one Jupyter Notebook file, in which:

you will process the event_datafile_new.csv dataset to create a denormalized dataset
you will model the data tables keeping in mind the queries you need to run
you have been provided queries that you will need to model your data tables for
you will load the data into tables you create in Apache Cassandra and run your queries
Project Steps

## `Project Steps`
Below are steps you can follow to complete each component of this project.

Modeling your NoSQL database or Apache Cassandra database

+ Design tables to answer the queries outlined in the project template
+ Write Apache Cassandra CREATE KEYSPACE and SET KEYSPACE statements
+ Develop your CREATE statement for each of the tables to address each question
+ Load the data with INSERT statement for each of the tables
+ Include IF NOT EXISTS clauses in your CREATE statements to create tables only if the tables do not already exist. 

We recommend you also include DROP TABLE statement for each table, this way you can run drop and create tables whenever you want to reset your database and test your ETL pipeline
+ Test by running the proper select statements with the correct WHERE clause

## `Build ETL Pipeline`
+ Implement the logic in section Part I of the notebook template to iterate through each event file in event_data to process and create a new CSV file in Python
+ Make necessary edits to Part II of the notebook template to include Apache Cassandra CREATE and INSERT statements to load processed records into relevant tables in your data model
+ Test by running SELECT statements after running the queries on your database
