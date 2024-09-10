import pandas as pd
from src.db_connection import Postgres_Connection
def load_data():
    # Instantiate the database connection
    db = Postgres_Connection(dbname='my_database', user='postgres', password='123', host='localhost', port='5432')
    db.connect()
    
    # Define the SQL query to fetch the data
    query = "SELECT * FROM public.xdr_data"
    
    # Fetch the data into a DataFrame
    df = db.fetch_to_dataframe(query)
    
    # Close the database connection
    db.close_connection()
    
    return df

def preprocess_data(df):
    # Example preprocessing steps
    # Drop rows with missing values
    df.dropna(inplace=True)
    
    # Fill any remaining missing values
    df.fillna(0, inplace=True)
    
    # Example of normalizing data (if needed)
    # df['normalized_column'] = (df['column'] - df['column'].mean()) / df['column'].std()
    
    # Additional preprocessing steps based on your dataset
    # For example: converting data types, encoding categorical variables, etc.
    
    return df

def save_data(df, file_path):
    df.to_csv(file_path, index=False)
    print(f"Data saved to {file_path}")

if __name__ == "__main__":
    # Load the data
    df = load_data()
    
    # Preprocess the data
    df = preprocess_data(df)
    
    # Save the processed data
    save_data(df, 'processed_data.csv')
