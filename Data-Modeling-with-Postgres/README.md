
## `Building user activity database with songplaying`
## `Creating ETL pipeline in Postgres`
## `Doing Song Play Analysis`

This is the first project where we'll be using the song and log datasets to create a star schema optimized for queries on song play analysis. I'll be using project instruction steps to write my README.md file. This includes 1 Fact table and 4 dimension tables. Make sure to click "Restart kernel" to close the connection to the database after running this notebook.

### `Fact Table` ###
- `1. songplays` - records in log data associated with song plays i.e. records with page NextSong
songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

### `Dimension Tables` ###
- `2. users:`  users in the app
               user_id, first_name, last_name, gender, level
- `3. songs:`  songs in music database
                song_id, title, artist_id, year, duration
- `4. artists:`  artists in music database
                 artist_id, name, location, latitude, longitude
- `5. time:`  timestamps of records in songplays broken down into specific units
                start_time, hour, day, week, month, year, weekday

### `Project Template`
To effectively create the project in coherence we're given 6 files and most of them are in a broad and minimalistice way are template files. All we have to do 
fill in the needed code and run them to verify that our codes were working as designed and needed.

`These six files are:`
- `1. test.ipynb:` displays the first few rows of each table to let you check your database.
- `2. create_tables.py:` drops and creates your tables. You run this file to reset your tables before each time you run your ETL scripts.
- `3. etl.ipynb:` reads and processes a single file from song_data and log_data and loads the data into your tables. This notebook contains detailed instructions on the ETL process for each of the tables.
- `4. etl.py:` reads and processes files from song_data and log_data and loads them into your tables. You can fill this out based on your work in the ETL notebook.
- `5. sql_queries.py:` contains all your sql queries, and is imported into the last three files above.


### `Building Project Steps:`
Below are steps you can follow to complete the project:

`Creating/writing Tables`
- Write CREATE statements in sql_queries.py to create each table.
- Write DROP statements in sql_queries.py to drop each table if it exists.

`Running tables`
- Run create_tables.py to create your database and tables.
- Run test.ipynb to confirm the creation of your tables with the correct columns.

### `Build ETL Processes:`
We have to follow instructions in the `etl.ipynb notebook` to develop ETL processes for each table. At the end of each table section, or at the end of the notebook, `run test.ipynb` to confirm that records were `successfully inserted into each table. Remember to rerun create_tables.py to reset your tables before each time you run this notebook.
