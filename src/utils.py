import pandas as pd

def missing_values_table(df: pd.DataFrame):
    """
    Function to calculate and display missing values in a DataFrame.
    Args:
        df (pd.DataFrame): The DataFrame for which to calculate missing values.
    
    Returns:
        pd.DataFrame: DataFrame displaying columns with their missing value counts and percentages.
    """
    # Total missing values
    mis_val = df.isnull().sum()
    
    # Percentage of missing values
    mis_val_percent = 100 * mis_val / len(df)
    
    # Make a table with the results
    mis_val_table = pd.DataFrame({"Missing Values": mis_val, "Percentage": mis_val_percent})
    
    # Filter only columns with missing values
    mis_val_table = mis_val_table[mis_val_table['Missing Values'] > 0]
    
    # Sort the table by percentage of missing descending
    mis_val_table = mis_val_table.sort_values(by="Percentage", ascending=False)
    
    return mis_val_table

def convert_bytes_to_megabytes(bytes_size: int) -> float:
    """
    Converts a byte size into megabytes.
    
    Args:
        bytes_size (int): Size in bytes.
    
    Returns:
        float: Size in megabytes.
    """
    megabytes_size = bytes_size / (1024 ** 2)  # 1 MB = 1024^2 bytes
    return round(megabytes_size, 2)
def convert_bytes_to_megabytes(df, column_name):
    df[column_name] = df[column_name] / (1024 * 1024)  # Converting bytes to megabytes
    return df
