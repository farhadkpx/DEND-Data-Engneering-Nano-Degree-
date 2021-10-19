
## `Project: Data Modeling with Cassandra`

`Contexture of the project:` A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analysis team is particularly interested in understanding what songs users are listening to in types of patterns. Currently, there is no easy way to query the data to generate the results, since the data reside in a directory of CSV files on user activity on the app. Company wants a data engineer to create an Apache Cassandra database on which they can run queries on song play data to answer the certain questions.

### `Project Overview`
In this project, we'll apply what you've learned on data modeling with Apache Cassandra and complete an ETL pipeline using Python programming lanugage. To complete the project, we will need to model your data by creating tables in Apache Cassandra to run queries. We are provided with part of the ETL pipeline that transfers data from a set of CSV files within a directory to create a streamlined CSV file to insert data into Apache Cassandra tables. A project template(a Jupyter notebook file) was given to us that takes care of all the imports and provides a structure for ETL pipeline we'd need to process this data.

**Datasets** : For this project, we're given a working dataset: `event_data`
> Details of all the data  can be found with this link
### `Project summary directives:`
`1.` We'll process the event_datafile_new.csv dataset to create a denormalized dataset

`2.` We'll model the data tables keeping in mind the queries we need to run

`3.` A set of queries have been provided that we will need to model your data tables for

`4.` We'll load the data into newly created tables in Apache Cassandra to run assigned queries

## `Project Programming Steps:`
`1.` Write Apache Cassandra CREATE KEYSPACE and SET KEYSPACE statements.

`2.` Develop your CREATE statement for each of the tables to address each question.

`3.` Include IF NOT EXISTS clauses in your CREATE statements to create tables only if the tables do not already exist. 

`4.` Load the data from 'event_data.csv' file with INSERT statement for each of the tables.

`5.` Include DROP TABLE statement for each table, this way we can run drop and create tables whenever we want to reset our database and test your ETL pipeline.

`6.` Test by running the proper select statements with the correct WHERE clause

## `Summary: `
If table creation and data insertion works effectively then our query should produce answers as expected and directed.

