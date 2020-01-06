from dotenv import load_dotenv, find_dotenv
import os
import pandas as pd

load_dotenv()

ARN = os.environ['ARN']
LOG_DATA = os.environ['LOG_DATA']
SONG_DATA = os.environ['SONG_DATA']
LOG_JSON_PATH = os.environ['LOG_JSON_PATH']

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create = ("""CREATE TABLE IF NOT EXISTS staging_events (
                                                            artist              VARCHAR,
                                                            auth                VARCHAR,
                                                            first_name          VARCHAR,
                                                            gender              VARCHAR,
                                                            item_in_session     VARCHAR,
                                                            last_name           VARCHAR,
                                                            length              VARCHAR,
                                                            level               VARCHAR,
                                                            location            VARCHAR,
                                                            method              VARCHAR,
                                                            page                VARCHAR,
                                                            registration        VARCHAR,
                                                            session_id          INTEGER,
                                                            song                VARCHAR,
                                                            status              VARCHAR,
                                                            ts                  BIGINT,
                                                            user_agent          VARCHAR,
                                                            user_id             INTEGER
                                                            )
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs (
                                                            song_id             VARCHAR,
                                                            num_songs           VARCHAR,
                                                            title               VARCHAR,
                                                            artist_name         VARCHAR,
                                                            artist_latitude     FLOAT,
                                                            year                INTEGER,
                                                            duration            FLOAT,
                                                            artist_id           VARCHAR,
                                                            artist_longitude    FLOAT,
                                                            artist_location     VARCHAR
                                                            )
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (
                                                            songplay_id         INTEGER     IDENTITY(1, 1)  PRIMARY KEY,
                                                            start_time          TIMESTAMP   NOT NULL    SORTKEY,
                                                            user_id             INTEGER,
                                                            level               VARCHAR     NOT NULL,
                                                            song_id             VARCHAR,
                                                            artist_id           VARCHAR,
                                                            session_id          INTEGER     NOT NULL,
                                                            location            VARCHAR,
                                                            user_agent          VARCHAR
                                                            )
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (
                                                            user_id             INTEGER     PRIMARY KEY,
                                                            first_name          VARCHAR,
                                                            last_name           VARCHAR,
                                                            gender              VARCHAR,
                                                            level               VARCHAR
                                                            ) diststyle all;
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (
                                                            song_id             VARCHAR     PRIMARY KEY,
                                                            title               VARCHAR,
                                                            artist_id           VARCHAR     NOT NULL,
                                                            year                INTEGER,
                                                            duration            FLOAT
                                                            ) diststyle all;
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (
                                                            artist_id           VARCHAR     PRIMARY KEY,
                                                            name                VARCHAR,
                                                            location            VARCHAR,
                                                            latitude            FLOAT,
                                                            longitude           FLOAT
                                                            ) diststyle all;
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (
                                                            start_time          TIMESTAMP   PRIMARY KEY,
                                                            hour                SMALLINT,
                                                            day                 SMALLINT,
                                                            week                SMALLINT,
                                                            month               SMALLINT,
                                                            year                SMALLINT,
                                                            weekday             SMALLINT
                                                            ) diststyle all;
""")

# STAGING TABLES

staging_events_copy = ("""COPY staging_events FROM '{}'
                          CREDENTIALS 'aws_iam_role={}'
                          REGION 'us-west-2'
                          FORMAT AS json '{}'
""").format(LOG_DATA, ARN, LOG_JSON_PATH)

staging_songs_copy = ("""COPY staging_songs FROM '{}'
                         CREDENTIALS 'aws_iam_role={}'
                         COMPUPDATE OFF REGION 'us-west-2'
                         JSON 'auto' TRUNCATECOLUMNS
""").format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT
    TIMESTAMP 'epoch' + e.ts/1000 * INTERVAL '1 second'
    , e.user_id
    , e.level
    , s.song_id
    , s.artist_id
    , e.session_id
    , e.location
    , e.user_agent
FROM staging_events AS e
JOIN staging_songs AS s
    ON e.song = s.title
    AND e.artist = s.artist_name
WHERE e.page = 'Next Song'
""")

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT
    user_id
    , first_name
    , last_name
    , gender
    , level
FROM staging_events
WHERE page = 'Next Song'
""")

song_table_insert = ("""INSERT INTO songs (artist_id, song_id, title, duration, year)
SELECT DISTINCT
    artist_id
    , song_id
    , title
    , duration
    , year
FROM staging_songs
""")

artist_table_insert = ("""INSERT INTO artists (artist_id, latitude, longitude, location, name)
SELECT DISTINCT
    artist_id
    , artist_latitude
    , artist_longitude
    , artist_location
    , artist_name
FROM staging_songs
""")

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT
    TIMESTAMP 'epoch' + CAST(ts AS INTEGER)/1000 * INTERVAL '1 second' AS start_time
    , EXTRACT(hour FROM start_time)
    , EXTRACT(day FROM start_time)
    , EXTRACT(week FROM start_time)
    , EXTRACT(month FROM start_time)
    , EXTRACT(year FROM start_time)
    , EXTRACT(week FROM start_time)
FROM staging_events
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
