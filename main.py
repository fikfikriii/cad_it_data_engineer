import psycopg2
import pandas as pd
from config import config

def read_csv_to_dataframe(file_path):
    try:
        df = pd.read_csv(file_path)
        return df
    except FileNotFoundError:
        print(f"File '{file_path}' not found.")
        return None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None
    
def connect():
    connection = None
    try:
        params = config()
        print('Connecting to the postgreSQL database ...')
        connection = psycopg2.connect(**params)

        # create a cursor
        crsr = connection.cursor()

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

        crsr.close()
    except(Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if connection is not None:
            connection.close()
            print('Database connection terminated.')

if __name__ == "__main__":
    # df = read_csv_to_dataframe('processed data/movies.csv')
    # print(df.head())
    connect()