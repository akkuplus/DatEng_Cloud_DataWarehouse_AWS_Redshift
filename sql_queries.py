import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')  # See dwh_example.cfg for sections and elements

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create = """CREATE TABLE staging_events (artist varchar,
                                                                auth varchar,
                                                                first_name varchar,
                                                                gender char(1),
                                                                item_in_session int,
                                                                last_name varchar,
                                                                length decimal,
                                                                level varchar,
                                                                location varchar,
                                                                method varchar,
                                                                page varchar,
                                                                registration varchar,
                                                                session_id int,
                                                                song varchar,
                                                                status int,
                                                                ts timestamp,
                                                                user_agent varchar,
                                                                user_id int
  )"""

staging_songs_table_create = """CREATE TABLE staging_songs (artist_id varchar NOT NULL PRIMARY KEY,
                                                            artist_latitude decimal,
                                                            artist_location varchar,
                                                            artist_longitude decimal,
                                                            artist_name varchar,
                                                            duration decimal,
                                                            num_songs int,
                                                            song_id varchar,
                                                            title varchar,
                                                            year int
  )"""

songplay_table_create = """CREATE TABLE songplays(songplay_id serial, 
                                                    start_time timestamp NOT NULL, 
                                                    user_id int NOT NULL, 
                                                    level varchar, 
                                                    song_id varchar, 
                                                    artist_id varchar, 
                                                    session_id int NOT NULL, 
                                                    location varchar, 
                                                    user_agent varchar,

                                                    PRIMARY KEY(songplay_id)
                                                  )"""

user_table_create = """CREATE TABLE users(user_id int, 
                                            first_name varchar, 
                                            last_name varchar,
                                            gender varchar, 
                                            level varchar,

                                            PRIMARY KEY(user_id) )"""

song_table_create = """CREATE TABLE songs(song_id varchar, 
                                            title varchar NOT NULL, 
                                            artist_id varchar NOT NULL, 
                                            year int, 
                                            duration float NOT NULL,

                                            PRIMARY KEY(song_id)"""

artist_table_create = """CREATE TABLE artists(artist_id varchar, 
                                                name varchar NOT NULL, 
                                                location varchar, 
                                                latitude varchar, 
                                                longitude varchar,

                                                PRIMARY KEY (artist_id)"""

time_table_create = """CREATE TABLE times(start_time timestamp, 
                                            hour int NOT NULL,
                                            day int NOT NULL,
                                            week int NOT NULL,
                                            month int NOT NULL,
                                            year int NOT NULL,
                                            weekday int NOT NULL,                                           

                                            PRIMARY KEY(start_time)"""

# STAGING TABLES

staging_events_copy =f"""COPY staging_events FROM {config["LOG_DATA"]}
                            CREDENTIALS 'aws_iam_role={config["ARN"]}'
                            FORMAT AS JSON {config["LOG_JSON_PATH"]}
                            TIMEFORMAT as 'epochmillisecs'
                            TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
                            COMPUPDATE OFF
                            REGION 'us-west-2';"""

staging_songs_copy = f"""COPY staging_songs FROM {config["SONG_DATA"]}
                            CREDENTIALS 'aws_iam_role={config["ARN"]}'
                            JSON 'auto'
                            TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
                            COMPUPDATE OFF
                            REGION 'us-west-2';"""

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)\
                            SELECT DISTINCT ts,
                                e.user_id,
                                e.level,
                                s.song_id,
                                s.artist_id,
                                e.session_id,
                                e.location,
                                e.user_agent
                            FROM staging_events AS e
                            JOIN staging_songs AS s ON e.artist = s.artist_name
                            WHERE 1=1
                            AND     e.page = 'NextSong'
                            ON CONFLICT DO NOTHING;""")

user_table_insert = ("""INSERt INTO users(user_id, first_name, last_name, gender, level)
                        SELECT DISTINCT user_id,
                            first_name,
                            last_name,
                            gender,
                            level
                        FROM staging_events
                        WHERE 1=1
                        AND page = 'NextSong'
                        AND user_id IS NOT NULL
                        ON CONFLICT (user_id) DO UPDATE SET level = excluded.level;""")  # Upsert, on conflict, update field with new value

song_table_insert = ("""INSERT INTO songs(song_id, title, artist_id, year, duration) \
                        SELECT DISTINCT song_id,
                            title,
                            artist_id,
                            year,
                            duration
                        FROM staging_songs
                        WHERE 1=1
                        AND song_id IS NOT NULL
                        ON CONFLICT DO NOTHING;""")

artist_table_insert = ("""INSERT INTO artists(artist_id, name, location, latitude, longitude) \
                        SELECT DISTINCT artist_id,
                            artist_name,
                            artist_location,
                            artist_latitude,
                            artist_longitude
                        FROM staging_songs
                        WHERE 1=1
                        AND artist_id IS NOT NULL
                        ON CONFLICT DO NOTHING;""")

time_table_insert = ("""INSERT INTO times(start_time, hour, day, week, month, year, weekday) \
                        SELECT DISTINCT ts,
                            EXTRACT(hour FROM ts),
                            EXTRACT(day FROM ts),
                            EXTRACT(week FROM ts),
                            EXTRACT(month FROM ts),
                            EXTRACT(year FROM ts),
                            EXTRACT(weekday FROM ts)
                        FROM staging_events
                        WHERE 1=1
                        AND page = 'NextSong'
                        AND ts IS NOT NULL
                        ON CONFLICT DO NOTHING;""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
