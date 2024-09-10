import pandas as pd
import psycopg2
from psycopg2 import sql

def load_data_from_postgres(query):
    # PostgreSQL connection details
    connection_params = {
        'dbname': 'my_database',
        'user': 'postgres',
        'password': '123',
        'host': 'localhost',
        'port': '5432'
    }

    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(**connection_params)
        print("Database connection established")

        # Execute the SQL query and load the data into a pandas DataFrame
        df = pd.read_sql(query, conn)

        # Close the connection
        conn.close()
        print("Database connection closed")

        return df

    except Exception as e:
        print(f"Error: {e}")
        return None
