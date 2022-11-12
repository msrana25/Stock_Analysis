import mysql.connector
from mysql.connector import Error

class Database: 
    

    host, user, pwd, dbName = '', '', '', ''

    def __init__(self, host, user, password, dbName, stocks):
        self.host = host
        self.user = user
        self.pwd = password
        self.dbName = dbName
        self.connection = self.connect_to_database()
        self.execute_query(self.connection, self.create_table_stocks)
        #DB.add_stocks(DB.stocks)
        self.create_stock_tables(stocks)


#Execute SQL Query
    def execute_query(self, connection, mysql_query, verbose = False):
        cursor = connection.cursor()
        try:
            cursor.execute(mysql_query)
            connection.commit()
            if (verbose):
                print("Query successful")
        except Error as e:
            if (verbose):
                print("Error: ", e)
            
#Read Data from Database                      
    def read_query(self, connection, mysql_query, verbose = False):
        cursor = connection.cursor()
        result = None
        try:
            cursor.execute(mysql_query)
            result = cursor.fetchall()
            return result
        except Error as e:
            if (verbose):
                print("Error: ", e)

#Connect to the Database
    def connect_to_database(self):
            connection = None
            try:
                connection = mysql.connector.connect(host = self.host, user = self.user, passwd = self.pwd)
                print("Connected to Server: Admin")
                existing_db = self.read_query(connection, "SHOW DATABASES")
                if ('soen6441',) not in existing_db:
                    self.execute_query(connection, "CREATE DATABASE SOEN6441")
                    print("Created Database: ", self.dbName)

                connection = mysql.connector.connect(host = self.host, user = self.user, passwd = self.pwd, database = self.dbName)
                print("Connected to Database: ", self.dbName)
            except Error as e:
                print("Error: ", e)

            return connection

#Create Table Stocks
    create_table_stocks = """CREATE TABLE STOCKS (  STOCKNAME VARCHAR(1000),
                                                    SYMBOL VARCHAR(20) PRIMARY KEY,
                                                    SECTOR VARCHAR(1000),
                                                    INDUSTRY VARCHAR(2000),
                                                    MARKETCAP BIGINT,
                                                    ASSETTYPE VARCHAR(300),
                                                    COMPANY_DESCRIPTION VARCHAR(10000)
                                                    )"""


#Update Stock Information
    def add_stocks_to_db(self, connection, stock_data):   
        stock_query= """INSERT INTO STOCKS VALUES ('%s', '%s', '%s', '%s', '%d', '%s', "%s")""" % (stock_data['Name'], stock_data['Symbol'], stock_data['Sector'], stock_data['Industry'], int(stock_data['MarketCapitalization']), stock_data['AssetType'], stock_data['Description'])    
        self.execute_query(connection, stock_query)


#Create Tables for Individual Stocks
    def create_stock_tables(self, stocks):
        for stock in stocks:
            create_table = """CREATE TABLE %s ( ID INT PRIMARY KEY,
                                                STOCKSYMBOL VARCHAR(20),
                                                DATENTIME DATETIME NOT NULL,
                                                OPEN FLOAT(9, 4),
                                                HIGH FLOAT(9, 4),
                                                LOW FLOAT(9, 4),
                                                CLOSE FLOAT(9, 4)
                                                )""" % (stock)

            self.execute_query(self.connection, create_table)


#Store Intraday Data in the Database
    def update_intraday_data(self, connection, stock_symbol, stock_data):
        for num, item in zip(range(1, len(stock_data) + 1), stock_data):
                intraday_update_query= """INSERT INTO %s VALUES (%s, '%s', '%s', %f, %f, %f, %f)""" % (stock_symbol, num, stock_symbol, item[0], item[1], item[2], item[3], item[4])
                self.execute_query(connection, intraday_update_query)                                                                                         


    def retrieve_hourly_data(self, connection, stock):
        cursor = connection.cursor()
        hourly_stock_data = {}

        get_hour = """SELECT DISTINCT DATENTIME FROM {0} WHERE DATENTIME LIKE "%:00:00" """.format(stock)
        get_interval = """SELECT DISTINCT DATENTIME FROM {0} WHERE DATENTIME LIKE "%:00" """.format(stock)
       
        cursor.execute(get_hour)
        result = cursor.fetchall()
        cursor.execute(get_interval)
        res1 = cursor.fetchall()
        
        for i, j in zip(range(len(result)-1), range(4, len(res1)-1, 4)):
            high_value, low_value = [], []
            open_value = self.read_query(connection, """SELECT CLOSE FROM {0} WHERE DATENTIME LIKE "{1}" """.format(stock, result[i+1][0]))[0][0]
            close_value = self.read_query(connection, """SELECT CLOSE FROM {0} WHERE DATENTIME LIKE "{1}" """.format(stock, result[i][0]))[0][0]

            for hours in range(j-4, j):
                high_value.append(self.read_query(connection, """SELECT HIGH FROM {0} WHERE DATENTIME LIKE "{1}" """.format(stock, res1[hours][0]))[0][0])
                low_value.append(self.read_query(connection, """SELECT LOW FROM {0} WHERE DATENTIME LIKE "{1}" """.format(stock, res1[hours][0]))[0][0])
            
            hourly_stock_data[str(result[i][0])] = {"Open": open_value, "High": max(high_value), "Low": min(low_value), "Close": close_value}

        return hourly_stock_data

    
    def retrieve_daily_data(self, connection, stock, hourly_data):
        cursor = connection.cursor()
        daily_stock_data = {}
        
        get_dates = """SELECT DISTINCT DATE(DATENTIME) FROM {0} WHERE DATENTIME LIKE "%20:00:00" """.format(stock)
        cursor.execute(get_dates)
        result = cursor.fetchall()
        
        for i in range(len(result)-1):
            open_value = self.read_query(connection, """SELECT CLOSE FROM {0} WHERE DATENTIME LIKE "{1}_20:00:00" """.format(stock, result[i+1][0]))[0][0]
            close_value = self.read_query(connection, """SELECT CLOSE FROM {0} WHERE DATENTIME LIKE "{1}_20:00:00" """.format(stock, result[i][0]))[0][0]
            high = [hourly_data[stock][val]['High'] for val in hourly_data[stock] if(str(result[i][0]) == str(val).split()[0])]
            low = [hourly_data[stock][val]['Low'] for val in hourly_data[stock] if(str(result[i][0]) == str(val).split()[0])]

            daily_stock_data[str(result[i][0])] = {"Open": open_value, "High": max(high), "Low": min(low), "Close": close_value}

        return daily_stock_data
    
# stocks = ['GOOGL', 'META', 'AAPL', 'AMZN']

# DB = Database('localhost', 'root', "#Snroshan1998", 'soen6441', stocks)
# connection = DB.connect_to_database()
# print(DB.create_table_stocks)