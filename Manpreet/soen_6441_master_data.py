from mysql.connector import Error


# Creating database after establishing the connection
def create_database(ser_connection, query):
    cursor = ser_connection.cursor()
    try:
        cursor.execute(query)
        print("Database created successfully")
    except Error as err:
        print(f"Error: '{err}'")


# create_database(connection, "CREATE DATABASE SOEN6441")


def execute_query(ser_connection, query):
    cursor = ser_connection.cursor()
    try:
        cursor.execute(query)
        ser_connection.commit()
        print("Query successful")
    except Error as err:
        print(f"Error: '{err}'")


# create_stock_tables(stocks)

create_table_stocks = """CREATE TABLE MASTER_STOCKS (  STOCKNAME VARCHAR(1000),
                                                SYMBOL VARCHAR(20) PRIMARY KEY,
                                                SECTOR VARCHAR(1000),
                                                INDUSTRY VARCHAR(2000),
                                                MARKETCAP BIGINT,
                                                ASSETTYPE VARCHAR(300),
                                                COMPANY_DESCRIPTION VARCHAR(10000)
                                                )"""


# execute_query(connection, create_table_stocks)

def add_master_stocks_data(stocks_master_data):
    for stock in stocks_master_data:
        url = 'https://www.alphavantage.co/query?function=OVERVIEW&symbol=%s&apikey=7NVAEAZYAXPYKMZB' % stock
        stock_data = requests.get(url).json()
        stock_query = """INSERT INTO MASTER_STOCKS VALUES ('%s', '%s', '%s', '%s', '%d', '%s', "%s")""" % (
            stock_data['Name'], stock_data['Symbol'], stock_data['Sector'], stock_data['Industry'],
            int(stock_data['MarketCapitalization']), stock_data['AssetType'], stock_data['Description'])

        execute_query(connection, stock_query)

# add_master_stocks_data(stocks)
