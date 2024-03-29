# DROP TABLES

songplay_table_drop = "drop table if exists songplays"
user_table_drop = "drop table if exists users"
song_table_drop = "drop table if exists songs"
artist_table_drop = "drop table if exists artists"
time_table_drop = "drop table if exists time"

# CREATE TABLES

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id serial PRIMARY KEY,
        start_time timestamp REFERENCES time(start_time),
        user_id int NOT NULL REFERENCES users(user_id), 
        level varchar NOT NULL,
        song_id varchar NOT NULL REFERENCES songs(song_id),
        artist_id varchar NOT NULL  REFERENCES artists(artist_id),
        session_id int NOT NULL,
        location varchar,
        user_agent varchar
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id int PRIMARY KEY,
        first_name varchar NOT NULL,
        last_name varchar NOT NULL,
        gender varchar NOT NULL,
        level varchar NOT NULL
    );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id varchar PRIMARY KEY,
        artist_id varchar  REFERENCES artists(artist_id),
        title varchar NOT NULL,
        year int NOT NULL,
        duration real NOT NULL
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id varchar PRIMARY KEY,
        name varchar NOT NULL,
        location varchar,
        latitude real,
        longitude real
    );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time timestamp PRIMARY KEY,
        hour smallint,
        day smallint,
        week smallint,
        month smallint,
        year varchar,
        weekday varchar
    );
""")

# INSERT RECORDS

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id,
    artist_id, session_id, location, user_agent)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    VALUES(%s, %s, %s, %s, %s)
    ON CONFLICT(user_id) DO UPDATE
    SET first_name = excluded.first_name,
        last_name = excluded.last_name,
        gender = excluded.gender,
        level = excluded.level;
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, artist_id, title, year, duration)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT(song_id) DO UPDATE
    SET artist_id = excluded.artist_id,
        title = excluded.title,
        year = excluded.year,
        duration = excluded.duration;
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT(artist_id) DO UPDATE
    SET name = excluded.name,
        location = excluded.location,
        latitude = excluded.latitude,
        longitude = excluded.longitude;
""")


time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
""")

# FIND SONGS

song_select = ("""
SELECT s.song_id, artists.artist_id FROM songs as S
JOIN artists ON S.artist_id = artists.artist_id 
WHERE S.title = %s AND artists.name= %s AND S.duration= %s
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create,
                        song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop,
                      song_table_drop, artist_table_drop, time_table_drop]
