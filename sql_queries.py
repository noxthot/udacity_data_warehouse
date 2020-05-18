import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')


# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"


# CREATE TABLES
staging_events_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_events (
    artist        VARCHAR, 
    auth          VARCHAR, 
    firstName     VARCHAR, 
    gender        VARCHAR, 
    itemInSession INTEGER, 
    lastName      VARCHAR, 
    length        NUMERIC, 
    level         VARCHAR,
    location      VARCHAR,
    method        VARCHAR,
    page          VARCHAR,
    registration  NUMERIC,
    sessionId     INTEGER,
    song          VARCHAR,
    status        INTEGER,
    ts            NUMERIC,
    userAgent     VARCHAR,
    userId        INTEGER
);
""")
 
staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs        INTEGER, 
    artist_id        VARCHAR, 
    artist_latitude  NUMERIC, 
    artist_longitude NUMERIC, 
    artist_location  VARCHAR, 
    artist_name      VARCHAR, 
    song_id          VARCHAR, 
    title            VARCHAR,
    duration         NUMERIC,
    year             INTEGER
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id INTEGER IDENTITY(0, 1) PRIMARY KEY, 
    start_time  TIMESTAMP, 
    user_id     INTEGER NOT NULL, 
    level       VARCHAR, 
    song_id     VARCHAR NOT NULL, 
    artist_id   VARCHAR NOT NULL, 
    session_id  INTEGER NOT NULL, 
    location    VARCHAR, 
    user_agent  VARCHAR
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id    INTEGER PRIMARY KEY, 
    first_name VARCHAR, 
    last_name  VARCHAR, 
    gender     VARCHAR, 
    level      VARCHAR
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id    VARCHAR PRIMARY KEY,
    title      VARCHAR, 
    artist_id  VARCHAR NOT NULL,
    year       INTEGER, 
    duration   NUMERIC
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR PRIMARY KEY, 
    name      VARCHAR, 
    location  VARCHAR, 
    latitude  NUMERIC, 
    longitude NUMERIC
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP, 
    hour       INTEGER, 
    day        INTEGER, 
    week       INTEGER, 
    month      INTEGER, 
    year       INTEGER, 
    weekday    INTEGER
);
""")


# STAGING TABLES
staging_events_copy = ("""
COPY staging_events FROM {}
CREDENTIALS 'aws_iam_role={}'
FORMAT AS json {};
""").format(config.get("S3", "LOG_DATA"), config.get("IAM_ROLE", "ARN"), config.get("S3", "LOG_JSONPATH"))

staging_songs_copy = ("""
COPY staging_songs FROM {}
CREDENTIALS 'aws_iam_role={}'
JSON 'auto';
""").format(config.get("S3", "SONG_DATA"), config.get("IAM_ROLE", "ARN"))


# FINAL TABLES
songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT timestamp 'epoch' + e.ts * interval '1 second' AS start_time,
       e.userId     AS user_id,
       e.level      AS level,
       s.song_id    AS song_id,
       s.artist_id  AS artist_id,
       e.sessionId AS session_id,
       e.location   AS location,
       e.userAgent  AS user_agent
FROM staging_events AS e
JOIN staging_songs AS s ON (e.artist = s.artist_name AND e.song = s.title)
WHERE e.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO users (user_id , first_name, last_name, gender, level)
SELECT e.userId    AS user_id,
       e.firstName AS first_name,
       e.lastName  AS last_name,
       e.gender    AS gender,
       e.level     AS level
FROM staging_events AS e
JOIN (SELECT MAX(ts) as maxts, userId FROM staging_events GROUP BY userId) AS ee ON (ee.userId = e.userId AND ee.maxts = e.ts);
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT s.song_id   AS song_id,
       s.title     AS title,
       s.artist_id AS artist_id,
       s.year      AS year,
       s.duration  AS duration
FROM staging_songs AS s;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT s.artist_id        AS artist_id,
       s.artist_name      AS name,
       s.artist_location  AS location,
       s.artist_latitude  AS latitude,
       s.artist_longitude AS longitude
FROM staging_songs AS s;
""")

# since we fill songplay table before time table and we are only interested in the timestamps from the songplay table, we
# can use that table/column to fill the time table
time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT s.start_time,
       EXTRACT(hour FROM s.start_time) AS hour,
       EXTRACT(day FROM s.start_time) AS day,
       EXTRACT(week FROM s.start_time) AS week,
       EXTRACT(month FROM s.start_time) AS month,
       EXTRACT(year FROM s.start_time) AS year,
       EXTRACT(weekday FROM s.start_time) AS weekday
FROM (SELECT DISTINCT start_time FROM songplays) AS s;
""")


# SELECT queries for having a short glimpse at the tables
songplay_select = ("""
SELECT * FROM songplays LIMIT 20;
""")

user_select = ("""
SELECT * FROM users LIMIT 20;
""")

song_select = ("""
SELECT * FROM songs LIMIT 20;
""")

artist_select = ("""
SELECT * FROM artists LIMIT 20;
""")

time_select = ("""
SELECT * FROM time LIMIT 20;
""")


# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
select_queries =  [songplay_select, user_select, song_select, artist_select, time_select]