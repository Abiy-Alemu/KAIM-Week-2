from sqlalchemy import create_engine
import pandas as pd

def load_data_from_postgres():
    # Create SQLAlchemy engine
    engine = create_engine('postgresql://postgres:123@localhost:5432/my_database')
    
    # Query to fetch data
    query = "SELECT * FROM public.xdr_data;"
    
    # Load data into DataFrame
    df = pd.read_sql_query(query, engine)
    
    return df

# Example usage
if __name__ == "__main__":
    # Load the data into df
    df = load_data_from_postgres()
    
    # Print the first few rows
    print(df.head())
