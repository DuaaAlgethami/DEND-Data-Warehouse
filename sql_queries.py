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

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events(
        artist varchar,
        auth varchar,
        firstName varchar,
        gender varchar,
        itemInSession integer ,
        lastName varchar,
        length float ,
        level varchar,
        location varchar,
        method varchar,
        page varchar,
        registration float,
        sessionId integer,
        song varchar,
        status integer,
        ts bigint,
        userAgent varchar,
        userId varchar
    );
    """)

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs(
        num_songs integer,
        artist_id  varchar, 
        artist_latitude float, 
        artist_longitude float,
        artist_location varchar,
        artist_name varchar, 
        song_id varchar,
        title varchar, 
        duration float, 
        year integer
    );
    """)

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays(
        songplay_id integer IDENTITY(0,1),
        start_time timestamp NOT NULL,
        user_id varchar NOT NULL,
        level varchar,
        song_id varchar,
        artist_id varchar,
        session_id integer,
        location varchar,
        user_agent varchar,
        PRIMARY KEY (songplay_id)
    );
    """)

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users(
        user_id varchar,
        first_name varchar,
        last_name varchar, 
        gender varchar,
        level varchar,
        PRIMARY KEY (user_id)
    );
    """)

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs(
        song_id varchar,
        title varchar, 
        artist_id varchar, 
        year integer , 
        duration float,
        PRIMARY KEY (song_id)
    );
    """)

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists(
        artist_id varchar, 
        name varchar,
        location varchar, 
        latitude float, 
        longitude float,
        PRIMARY KEY (artist_id)
    );
    """)

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time(
        start_time timestamp,
        hour integer,
        day integer,
        week integer,
        month integer ,
        year integer ,
        weekday varchar,
        PRIMARY KEY (start_time)
    );
    """)

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from {data_bucket}
    credentials 'aws_iam_role={role_arn}'
    region 'us-west-2' format as JSON {log_json_path}
    timeformat as 'epochmillisecs';
""").format(data_bucket=config['S3']['LOG_DATA'], role_arn=config['IAM_ROLE']['ARN'], 
            log_json_path=config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
    copy staging_songs from {data_bucket}
    credentials 'aws_iam_role={role_arn}'
    region 'us-west-2' format as JSON 'auto';
""").format(data_bucket=config['S3']['SONG_DATA'], role_arn=config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
        SELECT DISTINCT timestamp 'epoch' + e.ts/1000 * interval '1 second' as start_time,
                                 e.userId,
                                 e.level,
                                 s.song_id,
                                 s.artist_id,
                                 e.sessionId,
                                 e.location,
                                 e.userAgent
        FROM staging_events as e
        JOIN staging_songs as s
        ON e.song = s.title
        AND e.artist = s.artist_name
        AND e.length = s.duration
        WHERE e.page='NextSong';
""")

user_table_insert = ("""
    INSERT INTO users
        (user_id , first_name , last_name , gender , level )
    SELECT DISTINCT userId,
                    firstName,
                    lastName,
                    gender,
                    level
    FROM staging_events
    WHERE userId IS NOT NULL
    AND page LIKE 'NextSong';
""")

song_table_insert = ("""
    INSERT INTO songs
        (song_id , title , artist_id , year  , duration )
    SELECT DISTINCT song_id,
                    title,
                    artist_id,
                    year,
                    duration
    FROM staging_songs
    WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
    INSERT INTO artists
        (artist_id , name , location , latitude , longitude )
    SELECT DISTINCT artist_id,
                    artist_name,
                    artist_location,
                    artist_latitude,
                    artist_longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
    INSERT INTO time
        (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT TIMESTAMP 'epoch' + (ts/1000) * INTERVAL '1 second' as start_time,
           EXTRACT(hour FROM start_time) AS hour,
           EXTRACT(day FROM start_time) AS day,
           EXTRACT(weeks FROM start_time) AS week,
           EXTRACT(month FROM start_time) AS month,
           EXTRACT(year FROM start_time) AS year,
           to_char(start_time, 'Day') AS weekday
    FROM staging_events
    WHERE page LIKE 'NextSong';
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
