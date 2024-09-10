import psycopg2
import pandas as pd

class PostgresConnection:
    def __init__(self, dbname, user, password, host='localhost', port='5432'):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.conn = None
        self.cursor = None

    def connect(self):
        try:
            self.conn = psycopg2.connect(
                dbname=self.dbname,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port
            )
            self.cursor = self.conn.cursor()
            print("Connected to PostgreSQL database!")
        except Exception as e:
            print(f"Error: {e}")

    def execute_query(self, query):
        if self.cursor is None:
            print("Cursor is not initialized.")
            return None
        try:
            self.cursor.execute(query)
            rows = self.cursor.fetchall()
            return rows
        except Exception as e:
            print(f"Error executing query: {e}")
            return None

    def close_connection(self):
        if self.conn is not None:
            self.conn.close()
            print("Connection closed.")
        self.cursor = None

    def fetch_to_dataframe(self, query):
        if self.cursor is None:
            print("Cursor is not initialized.")
            return None
        try:
            rows = self.execute_query(query)
            if rows:
                df = pd.DataFrame(rows, columns=[desc[0] for desc in self.cursor.description])
                return df
            else:
                return None
        except Exception as e:
            print(f"Error fetching data: {e}")
            return None

# Instantiate and connect to the database
db = PostgresConnection(dbname='my_database', user='postgres', password='123', host='localhost', port='5432')
db.connect()

# Fetch data
query = "SELECT * FROM public.xdr_data"  # Update with the actual table name
df = db.fetch_to_dataframe(query)

# Check the result
if df is not None:
    print(df.head())

    # Data cleaning and processing
    df.dropna(inplace=True)
    df.fillna(0, inplace=True)
else:
    print("Failed to retrieve data.")

db.close_connection()
