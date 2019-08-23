# Libraries
import configparser    

# Read properties file
config = configparser.ConfigParser()
config.read('dwh.cfg')

# Drop tables 

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop ="DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS  time"

# Postgres Create Tables

##staging
staging_events_table_create= ("""CREATE TABLE staging_events(
                                event_id INT IDENTITY(0,1),
                                artist_name VARCHAR(255),
                                auth VARCHAR(50),
                                user_first_name VARCHAR(255),
                                user_gender  VARCHAR(1),
                                item_in_session	INTEGER,
                                user_last_name VARCHAR(255),
                                song_length	DOUBLE PRECISION, 
                                user_level VARCHAR(50),
                                location VARCHAR(255),	
                                method VARCHAR(100),
                                page VARCHAR(100),	
                                registration VARCHAR(100),	
                                session_id	BIGINT,
                                song_title VARCHAR(255),
                                status INTEGER,  
                                ts VARCHAR(100),
                                user_agent TEXT,	
                                user_id VARCHAR(100),
                                PRIMARY KEY (event_id))""")

staging_songs_table_create = ("""CREATE TABLE staging_songs(
                                song_id VARCHAR(100),
                                num_songs INTEGER,
                                artist_id VARCHAR(100),
                                artist_latitude DOUBLE PRECISION,
                                artist_longitude DOUBLE PRECISION,
                                artist_location VARCHAR(255),
                                artist_name VARCHAR(255),
                                title VARCHAR(255),
                                duration DOUBLE PRECISION,
                                year INTEGER,
                                PRIMARY KEY (song_id)) """)



## Dimension Tables
user_table_create = ("""CREATE TABLE IF NOT EXISTS users(
                                user_id VARCHAR(50) NOT NULL, 
                                first_name VARCHAR NOT NULL,
                                 last_name VARCHAR NOT NULL,
                                 gender VARCHAR NOT NULL, 
                                 level VARCHAR NOT NULL, 
                                 primary key (user_id))""")


song_table_create = ("""CREATE TABLE IF NOT EXISTS songs(
                        song_id varchar NOT NULL, 
                        title varchar,
                        artist_id varchar, 
                        year int, 
                        duration float, 
                        PRIMARY KEY (song_id))""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists(
                            artist_id varchar NOT NULL, 
                            name varchar, 
                            location varchar,
                            latitude numeric, 
                            longitude numeric,  
                            PRIMARY KEY (artist_id))""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time(
                        start_time timestamp NOT NULL, 
                        hour int, 
                        day int, 
                        week int,
                        month int, 
                        year int, 
                        weekday varchar, 
                        PRIMARY KEY (start_time))""")

## Fact Table
songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays(
                            songplay_id INT IDENTITY(0,1),
                            start_time timestamp, 
                            user_id VARCHAR(50), 
                            level text, 
                            song_id varchar, 
                            artist_id varchar, 
                            session_id int, 
                            location text, 
                            user_agent text)""")
                        
#Data Insert
    ## Staging Tables
staging_events_copy = ("""copy staging_events
                         from {}
                         iam_role {}
                         JSON {}
                         """).format(config.get('S3','LOG_DATA'),
                                     config.get('IAM_ROLE', 'ARN'),
                                     config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""copy staging_songs
                         from {}
                         iam_role {}
                         JSON 'auto'
                         """).format(config.get('S3','SONG_DATA'),
                                     config.get('IAM_ROLE', 'ARN'))


    ## FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
        SELECT DISTINCT 
            TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1second' as start_time,
            se.user_id, 
            se.user_level,
            ss.song_id, 
            ss.artist_id, 
            se.session_id, 
            se.location, 
            se.user_agent
        FROM staging_events se, staging_songs ss
        WHERE se.page = 'NextSong'
        AND se.song_title = ss.title
        AND se.artist_name = ss.artist_name
        AND se.song_length = ss.duration
        """)

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level)
        SELECT DISTINCT
            se.user_id, 
            se.user_first_name, 
            se.user_last_name, 
            se.user_gender, 
            se.user_level
        FROM staging_events se
        WHERE se.page = 'NextSong'
        """)

song_table_insert = ("""INSERT  INTO songs (song_id, title, artist_id, year, duration)
        SELECT DISTINCT
            ss.song_id, 
            ss.title,
            ss.artist_id,
            ss.year, 
            ss.duration
        FROM staging_songs ss
        WHERE song_id IS NOT NULL
        """)

artist_table_insert = ("""INSERT  INTO artists(artist_id, name, location, latitude, longitude)
        SELECT DISTINCT
            ss.artist_id, 
            ss.artist_name, 
            ss.artist_location, 
            ss.artist_latitude,
            ss.artist_longitude
        FROM staging_songs ss
        WHERE ss.artist_id IS NOT NULL
""")

time_table_insert = ("""INSERT  INTO time(start_time, hour, day, week, month, year, weekday)
        SELECT DISTINCT
            start_time, 
            EXTRACT(hr from start_time) AS hour,
            EXTRACT(d from start_time) AS day,
            EXTRACT(w from start_time) AS week,
            EXTRACT(mon from start_time) AS month,
            EXTRACT(yr from start_time) AS year, 
            EXTRACT(weekday from start_time) AS weekday
            FROM (
                SELECT DISTINCT  TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' as start_time 
                FROM staging_events s     
                )
                WHERE start_time NOT IN (SELECT DISTINCT start_time FROM time)
            """)

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]