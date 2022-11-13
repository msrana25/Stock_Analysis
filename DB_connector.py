import mysql.connector
from mysql.connector import Error


class DBConnector(object):
    __connection = None

    @classmethod
    def get_connection_object(cls, host, user, pwd, db_name):
        if cls.__connection is None:
            try:
                cls.__connection = mysql.connector.connect(host=host, user=user, password=pwd)
                print("Connected to Server: Admin")
                cls.__connection = mysql.connector.connect(host=host, user=user, password=pwd, database=db_name)
                print("Connected to Database: ", db_name)

            except Error as e:
                print("Error: ", e)

        return cls.__connection

    def __init__(self):
        if DBConnector.__connection is not None:
            raise Exception("Only single instance is permitted!!!")
        else:
            DBConnector.__connection = self

