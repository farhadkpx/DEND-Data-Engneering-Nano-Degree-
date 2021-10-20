
## `Project: Data Modeling with Cassandra`

`Contexture of the project:` A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analysis team is particularly interested in understanding what songs users are listening to in what types of patterns. Currently, there is no easy way to query the data to generate the results, since the data reside in a directory of CSV files on user activity on the app. Company wants a data engineer to create an Apache Cassandra database on which they can run queries on user-song play data to answer the certain query-questions.

### `Project Overview:`
In this project, we'll apply what we've learned on data modeling with Apache Cassandra and complete an ETL pipeline using Python programming lanugage. To complete the project, we need to model our data by creating tables in Apache Cassandra to run queries. Udacity has provided us with part of the ETL pipeline that transfers data from a set of CSV files within a directory to create a streamlined CSV file to insert data into Apache Cassandra tables. A project template(a Jupyter notebook file) was given to us that takes care of all the imports and provides a structure for ETL pipeline we'd need to process this data. One this progression we need to remember carefully that with Apache Cassandra we model our database tables on the queries we want to run.

**Datasets** : For this project, we're given a working dataset: `event_data`
> All the needed query-table schema design and relevant files must be found on this [`github-page-link`](https://github.com/farhadkpx/DEND-Data-Engneering-Nano-Degree-/tree/main/Data-Modeling-with-Apache-Cassandra)

### `Project summary directives:`
`1.` We'll process the event_datafile_new.csv dataset to create a denormalized dataset.

`2.` We'll model the data tables keeping in mind what kind of queries we need to run.

`3.` A set of queries have been provided that we will need to model your data tables for.

`4.` At the end we'd close the notebook-session and Cassandra cluster connection.


## `Project Programming Steps:`
`1.` Write Apache Cassandra CREATE KEYSPACE and SET KEYSPACE statements.

`2.` Develop your CREATE statement for each of the tables to address each query.

`3.` Include IF NOT EXISTS clauses in your CREATE statements to make sure that tables do not already exist. 

`4.` On this processes we must design our `query-table schema` with appropriate `Primary key` choices(important).

`5.` Then load the data from `event_datafile_new.csv` file with INSERT statement for each of the newly created tables.

`6.` Test by running the proper query select statements with the correct WHERE clause as needed.

`7.` At the very end of the notebook we should include DROP TABLE statement for each table.

This way we can run drop and create tables whenever we want to reset our database and test your ETL pipeline.

## `Project Query Data-Model design and brief Output summary:`
**Note:** Here is a brief snap-shot of what one can expect from `Cassandra Query-Data-Model`, to see the detail python code please try this link: [`Project_1B_ Project_Template.ipynb`](https://github.com/farhadkpx/DEND-Data-Engneering-Nano-Degree-/blob/main/Data-Modeling-with-Apache-Cassandra/Project_1B_%20Project_Template.ipynb)

#### `Query 1:`  Give me the artist, song title and song's length in the music app history that was heard during  sessionId = 338, and itemInSession  = 4
![image](https://user-images.githubusercontent.com/16586123/138171266-00e3b7ec-7a73-419e-864e-c43795985daa.png)

![image](https://user-images.githubusercontent.com/16586123/138171634-7f15ad21-cdd8-43ed-bbf6-f50afeda21a9.png)

![image](https://user-images.githubusercontent.com/16586123/138171817-cc6dcdd5-9ef4-4a21-b304-a0e93db71f02.png)

![image](https://user-images.githubusercontent.com/16586123/138171897-49e460db-6d2b-4127-813d-c67713e15415.png)

![image](https://user-images.githubusercontent.com/16586123/138171981-579eb5be-20cd-4981-b798-324d4795d88b.png)
![image](https://user-images.githubusercontent.com/16586123/138172054-5840ca7a-eec0-4818-9960-1d376562ff9c.png)


## `Summary: `
If table creation and data insertion works effectively then our query should produce answers as expected and directed.

