import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events";
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs";
songplay_table_drop = "DROP TABLE IF EXISTS songplays";
user_table_drop = "DROP TABLE IF EXISTS users";
song_table_drop = "DROP TABLE IF EXISTS songs";
artist_table_drop = "DROP TABLE IF EXISTS artists";
time_table_drop = "DROP TABLE IF EXISTS time";

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE staging_events(
        artist VARCHAR,
        auth VARCHAR,
        firstName VARCHAR,
        gender CHAR,
        iteminSession INT,
        lastName VARCHAR,
        length NUMERIC,
        level VARCHAR,
        location VARCHAR,
        method VARCHAR,
        page VARCHAR,
        registration NUMERIC,
        sessionid INT,
        song VARCHAR,
        status INT,
        ts BIGINT,
        userAgent VARCHAR,
        userid INT
        );
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs(
        num_songs INT,
        artist_id VARCHAR,
        artist_latitude NUMERIC,
        artist_longitude NUMERIC,
        artist_location VARCHAR,
        artist_name VARCHAR,
        song_id VARCHAR,
        title VARCHAR,
        duration NUMERIC,
        year INT
        );
""")

songplay_table_create = ("""
    CREATE TABLE songplays(
        songplay_id INT IDENTITY(0,1) PRIMARY KEY,
        start_time TIMESTAMP,
        user_id INT,
        level VARCHAR,
        song_id VARCHAR SORTKEY,
        artist_id VARCHAR,
        session_id INT,
        location VARCHAR,
        user_agent VARCHAR
        )diststyle auto;
""")


user_table_create = ("""
    CREATE TABLE users(
        user_id INT PRIMARY KEY SORTKEY,
        first_name VARCHAR,
        last_name VARCHAR,
        gender CHAR,
        level VARCHAR
        )diststyle auto;

""")

song_table_create = ("""
    CREATE TABLE songs(
        song_id VARCHAR PRIMARY KEY,
        title VARCHAR SORTKEY,
        artist_id VARCHAR,
        year INT,
        duration NUMERIC
        )diststyle auto;
""")

artist_table_create = ("""
    CREATE TABLE artists(
        artist_id VARCHAR PRIMARY KEY SORTKEY,
        name VARCHAR,
        location VARCHAR,
        latitude NUMERIC,
        longitude NUMERIC
        )diststyle auto;
    
""")

time_table_create = ("""
    CREATE TABLE time(
        start_time TIMESTAMP PRIMARY KEY SORTKEY,
        hour INT,
        day INT,
        week INT,
        month INT,
        year INT,
        weekday INT
        )diststyle auto;

""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from {}
    IAM_ROLE  {}
    FORMAT AS JSON {}
    region 'us-west-2';

""").format(config.get('S3','LOG_DATA'),config.get('IAM_ROLE','ARN'),config.get('S3','LOG_JSONPATH'))



staging_songs_copy = ("""
    copy staging_songs from {}
    IAM_ROLE  {}
    FORMAT AS JSON 'auto'
    region 'us-west-2';

""").format(config.get('S3','SONG_DATA'),config.get('IAM_ROLE','ARN'))

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays(start_time, user_id, level , song_id ,
        artist_id , session_id ,location , user_agent)
    SELECT 
        timestamp 'epoch' + se.ts/1000::float * interval '1 second' as start_time,
        se.userid, se.level, ss.song_id, ss.artist_id, se.sessionid, se.location,
        se.userAgent
    FROM staging_events se, staging_songs ss
    WHERE se.page='NextSong'
    AND se.artist = ss.artist_name
    AND se.song = ss.title
""")

user_table_insert = ("""
    INSERT INTO users(user_id, first_name, last_name, gender, level)
    SELECT 
        DISTINCT se.userid, se.firstName, se.lastName, se.gender, se.level
    FROM staging_events se
    WHERE page = 'NextSong'

""")

song_table_insert = ("""
    INSERT INTO songs(song_id, title, artist_id, year, duration)
    SELECT 
        DISTINCT ss.song_id, ss.title, ss.artist_id, ss.year, ss.duration
    FROM staging_songs ss
    WHERE ss.song_id IS NOT NULL
    
""")

artist_table_insert = ("""
    INSERT INTO artists(artist_id, name, location, latitude, longitude)
    SELECT 
        DISTINCT ss.artist_id, ss.artist_name, ss.artist_location, ss.artist_latitude, ss.artist_longitude
    FROM staging_songs ss
    WHERE ss.artist_id IS NOT NULL
    

""")

time_table_insert = ("""
    INSERT INTO time(start_time, hour, day, week, month, year, weekday)
    SELECT 
        DISTINCT start_time, 
        EXTRACT(HOUR FROM start_time) as hour,
        EXTRACT(DAY FROM start_time) as day,
        EXTRACT(WEEK FROM start_time) as week,
        EXTRACT(MONTH FROM start_time) as month,
        EXTRACT(YEAR FROM start_time) as year,
        EXTRACT(DAYOFWEEK FROM start_time) as weekday
    FROM songplays

""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
