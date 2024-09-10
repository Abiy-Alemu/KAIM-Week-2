import psycopg2
import pandas as pd

class Postgres_Connection:
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

    def fetch_to_dataframe(self, query):
        # Fetch data and return as a Pandas DataFrame
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


# Instantiate the database connection class
db = Postgres_Connection(dbname='my_database', user='postgres', password='123', host='localhost', port='5432')
db.connect()

# Query to fetch data from the table
query = "SELECT * FROM public.xdr_data"
df = db.fetch_to_dataframe(query)

# Check the result and display the first few rows
if df is not None:
    print(df.head())

    # Data cleaning and processing can start here
    df.dropna(inplace=True)  # Example of dropping missing values
    df.fillna(0, inplace=True)  # Imputation example
    # You can add more processing based on Task 1.1 and Task 1.2 needs

db.close_connection()
