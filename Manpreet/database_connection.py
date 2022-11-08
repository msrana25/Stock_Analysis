import mysql.connector
from mysql.connector import Error


def server_connection(host_name, username, pwd):
    ser_connection = None
    try:
        ser_connection = mysql.connector.connect(host=host_name, user=username, passwd=pwd)
        print("MySQL Database connection successful")
    except Error as err:
        print(f"Error: '{err}'")

    return ser_connection


def get_db_password():
    return "#abcd"


# Creating database after establishing the connection
def create_database(ser_connection, query):
    cursor = ser_connection.cursor()
    try:
        cursor.execute(query)
        print("Database created successfully")
    except Error as err:
        print(f"Error: '{err}'")
