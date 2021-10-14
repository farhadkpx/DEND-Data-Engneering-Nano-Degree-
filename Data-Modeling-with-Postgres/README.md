
# `Sparkify - User activity Data Modeling`
> ##  Songplaying analysis in Postgres Database Systems

 
This is the first project of `DEND program`, where we'll be using the song and log datasets to create a star schema optimized for queries on users song playing in Sparkify streaming App. I'll be using project instruction steps to write my README.md file. This includes 1 Fact table and 4 dimension tables. This project needs constant monitoring of "Restart kernel" to close the connection to the database after running templated python notebooks, which sometimes are very puzzling but a paramount necessity.

This project is about how a startup named **Sparkify** wants to analyze their users song playing data in their music streaming App. They want to analyze what song users are listening to within definitive timeframe. In pursuing so we'd create a `Postgres Database and an ETL pipeline` to optimize queries for our song playing analysis steps. On building the databse I'll be designing a star database schema with our fact and dimension tables both in Postgres wrapped in python programming language. 

* All the needed table schema design and relevant files must be found on this [`github link`](https://github.com/farhadkpx/DEND-Data-Engneering-Nano-Degree-/tree/main/Data-Modeling-with-Postgres)

## `Database schema design` 
Indepth column by column schema design is detailed in on `sql_queries.py` and in connected `create_tables.py` script files.
Here I just summarized the column labels as it is instructed with the project.

### `Fact Table` 
+ `1.` `songplays` - records in log data associated with song plays i.e. records with page NextSong
songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

### `Dimension Tables` 
+ `2.` `users:`  users in the app
               user_id, first_name, last_name, gender, level
+ `3.` `songs:`  songs in music database
                song_id, title, artist_id, year, duration
+ `4.` `artists:`  artists in music database
                 artist_id, name, location, latitude, longitude
+ `5.` `time:`  timestamps of records in songplays broken down into specific units
                start_time, hour, day, week, month, year, weekday

### `Project Template`
To effectively create the Postgres database within instructed coherence we're given 6 files and most of them are semi-template files. All we have to do 
fill in the needed codes in on these files and run them to verify that our codes were working as designed, insturcted and needed.

**These six files(Python Notebook, py-script files) are:**
+ `1.` `test.ipynb:` displays the first few rows of each table to let you check your database.
+ `2.` `create_tables.py:` drops and creates your tables. You run this file to reset your tables before each time you run your ETL scripts.
+ `3.` `etl.ipynb:` reads and processes a single file from song_data and log_data and loads the data into your tables. 
                    This notebook contains detailed instructions on the ETL process for each of the tables.
+ `4.` `etl.py:` reads and processes files from song_data and log_data and loads them into your tables. You can fill this out based on your work in the ETL notebook.
+ `5.` `sql_queries.py:` contains all your sql queries, and is imported into the last three files above.

 
## `Building Project Steps:`
Below are steps you can follow to complete the project:

**Creating/writing Tables**
- Write CREATE statements in sql_queries.py to create each table.
- Write DROP statements in sql_queries.py to drop each table if it exists.

**Running tables**
- Running create_tables.py to create your database and tables.
- Running test.ipynb to confirm the creation of your tables with the correct columns.
- Running etl.py to confirm overall file processing worked

### `Building ETL Pipeline Processes:`
We have to follow instructions in the `etl.ipynb` notebook to develop ETL processes for each table. At the end of each table section, or at the end of the notebook we will `run test.ipynb` file to confirm that records were `successfully inserted` into each table. Remember to rerun `create_tables.py` to reset your tables before each time you run this notebook. 

+ The `etl.py` script connects to the Sparkify database, extracts and processes the log_data and song_data, and loads data into the five tables.
+ files which will process the entire datasets. 

`Conclusion:` This Data Modeling with Postgres and Pipeline creation project requires complex interconnection between 6 template-files, which in successful completion will help us to build a Postgres database to analyze users music listening patterns and more. 
