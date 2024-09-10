import mysql.connector

# Test connection
try:
    connection = mysql.connector.connect(
        user='root',
        password='password',
        host='localhost',
        database='my_database'
    )
    if connection.is_connected():
        print("Successfully connected to the database")
    else:
        print("Failed to connect to the database")
except mysql.connector.Error as err:
    print(f"Error: {err}")
finally:
    if 'connection' in locals() and connection.is_connected():
        connection.close()
