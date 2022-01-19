import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
# Write SQL DROP statements to drop tables in the beginning of create_tables.py if the tables already exist. 
# This way, we can run create_tables.py whenever you want to reset your database and test your ETL pipeline.
#===========================================================================================================
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES
#=================================================================================
# stagin_events is the log dataset
staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        event_log_id    BIGINT IDENTITY(0,1),
        artist          VARCHAR(500),
        auth            VARCHAR(100),
        firstName       VARCHAR(100),
        gender          CHAR(1),
        
        itemInSession  INTEGER,
        lastName       VARCHAR(100),
        length         FLOAT,
        level          VARCHAR(20),
        
        location       VARCHAR(300),
        method         VARCHAR(50),
        page           VARCHAR(50),
        registration   VARCHAR,
        
        sessionId      INTEGER SORTKEY DISTKEY,
        song           VARCHAR(300),
        status         INTEGER,
        
        ts             BIGINT,
        userAgent      VARCHAR(300),
        userId         INTEGER
    );
""")

# staging_songs are song dataset
staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        artist_id        VARCHAR(50) SORTKEY DISTKEY,
        artist_latitude  FLOAT,
        artist_longitude FLOAT,
        
        artist_location  TEXT,
        artist_name      VARCHAR(400),
        
        duration         FLOAT,
        num_songs        INTEGER,
        song_id          VARCHAR(50),
        title            VARCHAR(400),
        year             INTEGER
    );
""")

#====================================== STAR SCHEMA =======================
# songplay table ( FACT TABLE )
# songplay schema design
songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id INT  IDENTITY(0,1) SORTKEY,
        start_time       TIMESTAMP NOT NULL,
        user_id          INTEGER NOT NULL DISTKEY,
        
        level            VARCHAR(20),
        song_id          VARCHAR(40) NOT NULL,
        artist_id        VARCHAR(50) NOT NULL,
        
        session_id        INTEGER NOT NULL,
        location          TEXT,
        user_agent        TEXT
    );
""")

#------------[ DIMENSION TABLES ]----------------------
# users table schema
user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id       INTEGER      SORTKEY,
        first_name    VARCHAR(70)  NOT NULL,
        last_name     VARCHAR(70)  NOT NULL,
        gender        CHAR(1),
        level         VARCHAR(20)  NOT NULL
    )diststyle all;
""")

# song table creation ( songs in music database )
# song table schema
song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id     VARCHAR(50)   SORTKEY,
        title       VARCHAR(400)  NOT NULL,
        artist_id   VARCHAR(50)   NOT NULL,
        year        INTEGER       NOT NULL,
        duration    FLOAT         NOT NULL
    )diststyle all;
""")

# artists table creation (artists in music database)
# artists table schema
artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id    VARCHAR(50) SORTKEY,
        name         VARCHAR(400) NOT NULL,
        location     TEXT,
        latitude     FLOAT,
        longitude    FLOAT
    ) diststyle all;
""")

# time table (timestamps of records in songplays broken down into specific units)
# time table schema
time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time TIMESTAMP  NOT NULL SORTKEY,
        hour        SMALLINT  NOT NULL,
        day         SMALLINT  NOT NULL,
        week        SMALLINT  NOT NULL,
        month       SMALLINT  NOT NULL,
        year        SMALLINT  NOT NULL,
        weekday     VARCHAR(30) NOT NULL
    ) diststyle all;
""")

#===============================================================================
# STAGING TABLES SOURCING
# staging_events_copy from Amazon S3
staging_events_copy = ("""
COPY staging_events FROM {}
    CREDENTIALS 'aws_iam_role={}'
    COMPUPDATE OFF region 'us-west-2'
    TIMEFORMAT as 'epochmillisecs'
    FORMAT AS JSON {};
""").format(config.get('S3', 'LOG_DATA'), 
            config.get('IAM_ROLE', 'ARN'), 
             config.get('S3', 'LOG_JSONPATH')
)
# staging_songs_copy from Amazon S3
staging_songs_copy = ("""
    COPY staging_songs FROM {}
    credentials 'aws_iam_role={}'
    format as json 'auto'
    ACCEPTINVCHARS AS '^'
    STATUPDATE ON
    region 'us-west-2';
""").format(config.get('S3', 'SONG_DATA'),
            config.get('IAM_ROLE', 'ARN'))

# ==============================================================================
# FINAL TABLES INSERTION-RESOURCING
# FACT TABLE >> SONGPLAY
songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    (SELECT DISTINCT TIMESTAMP with time zone 'epoch'+ se.ts/1000 *\
        interval '1 second' AS start_time, 
        se.userId           AS user_id, 
        se.level            AS level,
        ss.song_id          AS song_id, 
        ss.artist_id        AS artist_id,
        se.sessionId        AS session_id,
        se.location         AS location,
        se.userAgent        AS user_agent
    FROM staging_events AS se INNER JOIN staging_songs AS ss
    ON se.song = ss.title AND se.artist = ss.artist_name AND se.length = ss.duration
    WHERE se.page = 'NextSong');   
""")

# Inserting data into users table
# USERS TABLE
user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    (SELECT DISTINCT(userId)  AS user_id, 
    firstName                 AS first_name, 
    lastName                  AS last_name, 
    gender                    AS gender, 
    level                     AS level
    FROM staging_events
    WHERE page = 'NextSong' AND userId IS NOT NULL);
""")

# song_table_insert
# SONGS TABLE
song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    (SELECT DISTINCT(song_id) AS song_id, 
    title                     AS title, 
    artist_id                 AS artist_id,
    year                      AS year, 
    duration                  AS duration
    FROM staging_songs
    WHERE song_id IS NOT NULL);
""")

# artist_table_insert
# ARTIST TABLE
artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    (SELECT DISTINCT(artist_id) AS artist_id,
    artist_name                 AS name, 
    artist_location             AS location,
    artist_latitude             AS latitude,
    artist_longitude            AS longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL);
""")

# time_table_insert
# TIME TABLE
time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    (SELECT ts                              AS start_time, 
           extract(hour from start_time)    AS hour, 
           extract(day from start_time)     AS day, 
           extract(week from start_time)    AS week, 
           extract(month from start_time)   AS month, 
           extract(year from start_time)    AS year, 
           extract(weekday from start_time) AS weekday
    FROM ( SELECT DISTINCT  TIMESTAMP 'epoch' + se.ts/1000 *INTERVAL '1 second' as ts 
        FROM staging_events se     
    ));
""")

#==========Adding analytical queries========================================================================================
# ANALYTICAL QUERIES
analytical_queries = [
    # How many songs each user listened with the App
    'SELECT DISTINCT songplays.user_id, users.first_name, users.last_name, COUNT(songplays.artist_id) AS num_songs_listened\
     FROM songplays\
     JOIN users ON (songplays.user_id = users.user_id)\
     JOIN songs ON (songplays.artist_id = songs.artist_id)\
     GROUP BY songplays.user_id, users.first_name, users.last_name\
     ORDER BY num_songs_listened DESC\
     LIMIT 10;',
     # Total hour spent listening song by each user with the App
    'SELECT DISTINCT songplays.user_id, users.first_name, users.last_name, SUM(time.hour) AS total_hour\
    FROM songplays\
    JOIN users ON (songplays.user_id = users.user_id)\
    JOIN time ON (songplays.start_time = time.start_time)\
    GROUP BY songplays.user_id, users.first_name, users.last_name\
    ORDER BY total_hour DESC\
    LIMIT 10;'
    # Most widely listened singer by users
    'SELECT artists.name AS artist_name, COUNT(songplays.artist_id) AS artist_count\
     FROM songplays\
     JOIN artists ON(songplays.artist_id = artists.artist_id)\
     GROUP BY artists.name\
     ORDER BY artist_count DESC\
     LIMIT 10;'
     # In average how many hours a user spends listening songs
    'SELECT DISTINCT songplays.user_id, users.first_name, users.last_name, AVG(time.hour) AS avg_hour_spent\
    FROM songplays\
    JOIN users ON (songplays.user_id = users.user_id)\
    JOIN time ON (songplays.start_time = time.start_time)\
    GROUP BY songplays.user_id, users.first_name, users.last_name\
    ORDER BY avg_hour_spent DESC\
    LIMIT 10;'
    # In average how many hours a user's listens songs with the Appp
    'SELECT COUNT(users.user_id) AS total_user_count, AVG(time.hour) AS avg_hour_spent\
    FROM songplays\
    JOIN users ON (songplays.user_id = users.user_id)\
    JOIN time ON (songplays.start_time = time.start_time)'
    # title and artist name of the song most times listened to by users
    'SELECT songs.title AS song_title, artists.name, COUNT(songplays.song_id) AS num_listened\
     FROM songplays\
     LEFT JOIN songs ON (songplays.song_id = songs.song_id)\
     LEFT JOIN artists ON (songplays.artist_id = artists.artist_id)\
     GROUP BY songplays.song_id, songs.title, artists.name\
     ORDER BY num_listened DESC\
     LIMIT 10;'
]
# QUERY TITLES
analytical_query_titles = [
    'Number of songs each user listens to', 
    'Total hour spent by each user'
    'Most played artist by users choice', 
    'Each user in average spent how many hours listening music'
    'Average time user spent on listening music on Sparkify App'
    'Most number of times a song listened with artist name and title'
]

# QUERY LISTS
#==================================================================================================
# DROP TABLE QUEIRES LIST
drop_table_queries = [
                      staging_events_table_drop,
                      staging_songs_table_drop, 
                      songplay_table_drop, 
                       user_table_drop, 
                      song_table_drop, 
                      artist_table_drop, 
                      time_table_drop]

# CREATE TABLE QUERIES LIST
create_table_queries = [staging_events_table_create,
                        staging_songs_table_create,
                        songplay_table_create, 
                        user_table_create, 
                        song_table_create, 
                        artist_table_create, 
                        time_table_create]

# STAGING TABLES
copy_table_order = ['staging_events', 'staging_songs']

# STAGING COPIES
copy_table_queries = [staging_events_copy, staging_songs_copy]

# INSERT TABLES IN ORDER
insert_table_order = ['artists', 'songs', 'time', 'users', 'songplays']

# INSERTION IN ORDER
insert_table_queries = [artist_table_insert, 
                        song_table_insert, 
                        time_table_insert, 
                        user_table_insert, 
                        songplay_table_insert]
