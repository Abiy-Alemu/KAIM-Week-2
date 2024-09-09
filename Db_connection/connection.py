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


# Updated database credentials and table name
db = Postgres_Connection(dbname='my_database', user='postgres', password='123', host='localhost', port='5432')
db.connect()

# Query from your specific table
query = "SELECT * FROM public.xdr_data"
result = db.execute_query(query)

# Convert the result to a Pandas DataFrame if rows were fetched
if result:
    df = pd.DataFrame(result, columns=[desc[0] for desc in db.cursor.description])
    print(df.head())  # Display the first few rows of the DataFrame

# Close the connection when done
db.close_connection()
