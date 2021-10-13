
## `User activity database with songplaying and  ETL pipeline in Postgres`
## `Schema for Song Play Analysis`

This is the first project where we'll be using the song and log datasets to create a star schema optimized for queries on song play analysis. I'll be using project instruction steps to write my README.md file. This includes 1 Fact table and 4 dimension tables.

### `Fact Table` ###
- `1. songplays` - records in log data associated with song plays i.e. records with page NextSong
songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

### `Dimension Tables` ###
- `2. users: `  users in the app
               user_id, first_name, last_name, gender, level
- `3. songs: `  songs in music database
                song_id, title, artist_id, year, duration
- `4. artists: `  artists in music database
                 artist_id, name, location, latitude, longitude
- `5. time: `  timestamps of records in songplays broken down into specific units
                start_time, hour, day, week, month, year, weekday
