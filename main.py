import psycopg2
import pandas as pd
import numpy as np
from config import config

def column_mapping():
    movies_mapping = {
        'movie_id': 'movie_id',
        'movies': 'movies',
        'year_of_release': 'year_of_release',
        'year_end_series': 'year_end_series'
    }

    movies_episode_mapping = {
        'movie_id': 'movie_id',
        'movies': 'movies',
        'rating': 'rating',
        'plot': 'plot',
        'votes': 'votes',
        'runtime': 'runtime',
        'gross': 'gross',
        'is_tv_movie': 'is_tv_movie',
        'is_video_game': 'is_video_game',
        'is_tv_special': 'is_tv_special',
        'is_tv_short': 'is_tv_short'
    }

    movies_genre_mapping = {
        'movie_id': 'movie_id',
        'movies': 'movies',
        'genre': 'genre'
    }

    movies_directors_mapping = {
        'movie_id': 'movie_id',
        'movies': 'movies',
        'directors': 'directors'
    }

    movies_stars_mapping = {
        'movie_id': 'movie_id',
        'movies': 'movies',
        'stars': 'stars'
    }
    
    return movies_mapping, movies_episode_mapping, movies_genre_mapping, movies_directors_mapping, movies_stars_mapping

def read_input():
    # Movies
    movies = pd.read_csv('processed data/movies.csv', na_values=['NaN', 'nan', 'N/A', 'na', ''])
    int_columns = movies.select_dtypes(include='float64').columns
    movies[int_columns] = movies[int_columns].astype('Int64')

    # Movies episode
    movies_episode = pd.read_csv('processed data/movies_episode.csv', na_values=['NaN', 'nan', 'N/A', 'na', ''])
    int_columns = ['votes']
    movies_episode[int_columns] = movies_episode[int_columns].astype('Int64')

    # Movies genre
    movies_genre = pd.read_csv('processed data/movies_genre.csv', na_values=['NaN', 'nan', 'N/A', 'na', ''])
    
    # Movies director
    movies_directors = pd.read_csv('processed data/movies_directors.csv', na_values=['NaN', 'nan', 'N/A', 'na', ''])

    # Movies stars
    movies_stars = pd.read_csv('processed data/movies_stars.csv', na_values=['NaN', 'nan', 'N/A', 'na', ''])

    return movies, movies_episode, movies_genre, movies_directors, movies_stars

def create_cursor():
    connection = None
    try:
        params = config()
        connection = psycopg2.connect(**params)
        crsr = connection.cursor()
        return crsr, connection
    except(Exception, psycopg2.DatabaseError) as error:
        print(error)

def connect():
    try:
        crsr, connection = create_cursor()
        check_db(crsr)
        crsr.close()
        return connection
    except(Exception, psycopg2.DatabaseError) as error:
        print(error)
        return None

def insert_df(connection, table_name, df, column_mapping):
    try:
        cursor = connection.cursor()

        columns = ','.join(column_mapping.values())
        values_placeholder = ','.join(['%s'] * len(column_mapping))
        
        insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({values_placeholder})"
        
        df_values = df.to_dict('records')
        data_to_insert = [
            tuple(int(row[column_name]) if isinstance(row[column_name], np.int64) else row[column_name] 
                  for column_name in column_mapping.keys())
            for row in df_values
        ]

        cursor.executemany(insert_query, data_to_insert)
        connection.commit()

        print(f"Data inserted into {table_name} table successfully.")
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        if connection is not None:
            connection.rollback()
    finally:
        if cursor is not None:
            cursor.close()

def check_db(crsr):
    # Database version
    print('PostgreSQL database version: ')
    crsr.execute('SELECT version()')
    db_version = crsr.fetchone()
    print(db_version)

    # Database name
    crsr.execute('SELECT current_database()')
    db_name = crsr.fetchone()[0]
    print('Connected to the database:', db_name)

    # Table list
    crsr.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE'")
    tables = crsr.fetchall()
    print('Tables in the database:')
    for table in tables:
        print(table[0])

def delete_all_data_from_table(connection, table_name):
    try:
        cursor = connection.cursor()

        # Construct the DELETE query
        delete_query = f"DELETE FROM {table_name}"

        # Execute the DELETE query
        cursor.execute(delete_query)

        # Commit the transaction
        connection.commit()

        print(f"All data deleted from {table_name} table successfully.")
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        if connection is not None:
            connection.rollback()
    finally:
        if cursor is not None:
            cursor.close()

if __name__ == "__main__":
    connection = connect()
    # delete_all_data_from_table(connection, 'movies')
    # delete_all_data_from_table(connection, 'movies_genre')
    # delete_all_data_from_table(connection, 'movies_episode')
    # delete_all_data_from_table(connection, 'movies_directors')
    # delete_all_data_from_table(connection, 'movies_stars')
    
    movies, movies_episode, movies_genre, movies_directors, movies_stars = read_input()
    movies_mapping, movies_episode_mapping, movies_genre_mapping, movies_directors_mapping, movies_stars_mapping = column_mapping()

    if connection is not None:
        try:
            insert_df(connection, 'movies', movies, movies_mapping)
            insert_df(connection, 'movies_episode', movies_episode, movies_episode_mapping)
            insert_df(connection, 'movies_genre', movies_genre, movies_genre_mapping)
            insert_df(connection, 'movies_directors', movies_directors, movies_directors_mapping)
            insert_df(connection, 'movies_stars', movies_stars, movies_stars_mapping)
        finally:
            connection.close()