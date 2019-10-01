import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Loads data into song and artist tables
    @parameter cur: cursor
    @parameter filepath: the target file path to load from
    """

    df = pd.read_json(filepath, lines=True)

    song_data = df[['song_id', 'title',
                    'artist_id', 'year', 'duration']].values[0]
    cur.execute(song_table_insert, song_data)

    artist_data = df[['artist_id', 'artist_name', 'artist_location',
                      'artist_latitude', 'artist_longitude']].values[0]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Loads data into time, user and songplays tables
    @parameter cur: cursor
    @parameter filepath: the target file path to load from
    """

    df = pd.read_json(filepath, lines=True)

    df = df[df["page"] == "NextSong"]

    t = df['ts'] = pd.to_datetime(df['ts'], unit='ms')

    accessor = t.dt
    time_data = (t, accessor.hour, accessor.day, accessor.week,
                 accessor.month, accessor.year, accessor.weekday)

    time_df = pd.DataFrame.from_dict({
        "timestamp": t,
        "hour":  accessor.hour,
        "day": accessor.day,
        "week": accessor.week,
        "month": accessor.month,
        "year": accessor.year,
        "weekday": accessor.weekday
    })

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    for index, row in df.iterrows():

        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        start_time = row["ts"]
        user_id = row["userId"]
        level = row["level"]
        song_id = songid
        artist_id = artistid
        session_id = row['sessionId']
        location = row['location']
        user_agent = row['userAgent']

        songplay_data = (start_time, user_id, level, song_id, artist_id, session_id,
                         location, user_agent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Maps the data to a function
    @parameter cur: cursor
    @parameter conn: database connection
    @parameter filepath: the target file path to load from
    @parameter filepath: func to map the data for
    """

    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))

    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=sparkifydb user=postgres password=postgres")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data',
                 func=process_song_file)
    process_data(cur, conn, filepath='data/log_data',
                 func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
