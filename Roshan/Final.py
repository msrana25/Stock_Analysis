#!/usr/bin/env python
# coding: utf-8

# In[61]:


import json
import requests
import mysql.connector
from mysql.connector import Error
import pandas as pd
from datetime import datetime, date, timedelta


# In[2]:


stocks = ['GOOGL', 'META', 'AAPL', 'AMZN']
hourly_data = {}
daily_data = {}
weekly_data = {}


#                                         #CREATE CONNECTION TO MAIN SERVER
# def server_connection(host_name, username, pwd):
#     connection = None
#     try:
#         connection = mysql.connector.connect(host = host_name, user = username, passwd = pwd)
#         print("Server connection successful")
#     except Error as e:
#         print("Error: ", e)
# 
#     return connection
# 
# server = server_connection('localhost', 'root', '#Snroshan1998')

#                                               #CREATE A NEW DATABASE
# def create_database(connection, query):
#     cursor = connection.cursor()
#     try:
#         cursor.execute(query)
#         print("Database created successfully")
#     except Error as e:
#         print("Error: ", e)
#         
# create_database(server, "CREATE DATABASE SOEN6441")

# In[3]:


#CREATE A CONNECTION TO DATABASE
def create_db_connection(host_name, username, pwd, db_name):
    connection = None
    try:
        connection = mysql.connector.connect(host = host_name, user = username, passwd = pwd, database = db_name)
        print("MySQL Database connection successful")
    except Error as e:
        print("Error: ", e)

    return connection

connection = create_db_connection('localhost', 'root', '#Snroshan1998', "SOEN6441")


# In[4]:


#FUNCTION TO EXECUTE A QUERY
def execute_query(connection, mysql_query):
    cursor = connection.cursor()
    try:
        cursor.execute(mysql_query)
        connection.commit()
        print("Query successful")
    except Error as e:
        print("Error: ", e)


# In[5]:


#FUNCTION TO READ A QUERY
def read_query(connection, mysql_query):
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(mysql_query)
        result = cursor.fetchall()
        return result
    except Error as e:
        print("Error: ", e)


#                                                     #CREATE TABLE STOCKS
# create_table_stocks = """CREATE TABLE STOCKS (  STOCKNAME VARCHAR(1000),
#                                                 SYMBOL VARCHAR(20) PRIMARY KEY,
#                                                 SECTOR VARCHAR(1000),
#                                                 INDUSTRY VARCHAR(2000),
#                                                 MARKETCAP BIGINT,
#                                                 ASSETTYPE VARCHAR(300),
#                                                 COMPANY_DESCRIPTION VARCHAR(10000)
#                                                 )"""
# 
# execute_query(connection, create_table_stocks)

#                                                 #UPDATE STOCK INFORMATION
# def add_stocks(stocks):
#     for stock in stocks:
#         url = 'https://www.alphavantage.co/query?function=OVERVIEW&symbol=%s&apikey=7NVAEAZYAXPYKMZB' % (stock)
#         stock_data = requests.get(url).json()
#         stock_query= """INSERT INTO STOCKS VALUES ('%s', '%s', '%s', '%s', '%d', '%s', "%s")""" % (stock_data['Name'], stock_data['Symbol'], stock_data['Sector'], stock_data['Industry'], int(stock_data['MarketCapitalization']), stock_data['AssetType'], stock_data['Description'])
#         
#         execute_query(connection, stock_query)
# 
# add_stocks(stocks)
# 

#                                               #CREATE TABLES FOR INDIVIDUAL STOCKS
# def create_stock_tables(stocks):
#     for stock in stocks:
#         create_table = """CREATE TABLE %s ( ID INT PRIMARY KEY,
#                                             STOCKSYMBOL VARCHAR(20),
#                                             DATENTIME DATETIME NOT NULL,
#                                             OPEN FLOAT(9, 4),
#                                             HIGH FLOAT(9, 4),
#                                             LOW FLOAT(9, 4),
#                                             CLOSE FLOAT(9, 4)
#                                             )""" % (stock)
# 
#         execute_query(connection, create_table)
# 
#         
# create_stock_tables(stocks)

#                                                 #RETRIEVE INTRADAY VALUES FROM API
# def get_values(stock, interval, data):
#     intraday_values = []
#     for datetime in data:
#         intraday_values.append([datetime, float(data[datetime]['1. open']), float(data[datetime]['2. high']), float(data[datetime]['3. low']), float(data[datetime]['4. close'])])
#     return (intraday_values)

#                                                 #UPDATE INTRADAY VALUES FOR ALL STOCKS
# def update_intraday_values(stocks, interval = '15min'):
#     for stock in stocks:
#         execute_query(connection, """DELETE FROM %s""" %(stock))
#         url = 'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&interval=%s&symbol=%s&outputsize=full&apikey=7NVAEAZYAXPYKMZB' %(interval, stock)
#         intraday_data = requests.get(url).json()
#         values = get_values(stock, interval, intraday_data['Time Series (%s)' %(interval)])
#         for n, i in zip(range(1, len(values) + 1), values):
#             intraday_update_query= """INSERT INTO %s VALUES (%s, '%s', '%s', %f, %f, %f, %f)""" % (stock, n, intraday_data['Meta Data']['2. Symbol'], i[0], i[1], i[2], i[3], i[4])
#             execute_query(connection, intraday_update_query)        
#                                                                                            
# update_intraday_values(stocks)

# In[6]:


#OBTAIN HOURLY DATA
def update_hourly_data(connection, stocks):
    for stock in stocks:
        cursor = connection.cursor()
        hourly_stock_data = {}
        high_value, low_value = [], []
        get_hour = """SELECT DISTINCT DATENTIME FROM {0} WHERE DATENTIME LIKE "%:00:00" """.format(stock)
        get_interval = """SELECT DISTINCT DATENTIME FROM {0} WHERE DATENTIME LIKE "%:00" """.format(stock)
       
        cursor.execute(get_hour)
        result = cursor.fetchall()
        cursor.execute(get_interval)
        res1 = cursor.fetchall()
        
        for i, j in zip(range(len(result)-1), range(4, len(res1)-1, 4)):
            high_value, low_value = [], []
            open_value = read_query(connection, """SELECT CLOSE FROM {0} WHERE DATENTIME LIKE "{1}" """.format(stock, result[i+1][0]))[0][0]
            close_value = read_query(connection, """SELECT CLOSE FROM {0} WHERE DATENTIME LIKE "{1}" """.format(stock, result[i][0]))[0][0]

            for hours in range(j-4, j):
                high_value.append(read_query(connection, """SELECT HIGH FROM {0} WHERE DATENTIME LIKE "{1}" """.format(stock, res1[hours][0]))[0][0])
                low_value.append(read_query(connection, """SELECT LOW FROM {0} WHERE DATENTIME LIKE "{1}" """.format(stock, res1[hours][0]))[0][0])
            
            hourly_stock_data[result[i][0]] = {"Open": open_value, "High": max(high_value), "Low": min(low_value), "Close": close_value}

        hourly_data[stock] = hourly_stock_data
    return hourly_data

update_hourly_data(connection, stocks)


# In[7]:


#OBTAIN DAILY DATA
def update_daily_data(connection, stocks):
    for stock in stocks:
        cursor = connection.cursor()
        daily_stock_data = {}
        
        get_dates = """SELECT DISTINCT DATE(DATENTIME) FROM {0} WHERE DATENTIME LIKE "%20:00:00" """.format(stock)
        cursor.execute(get_dates)
        result = cursor.fetchall()
        
        for i in range(len(result)-1):
            open_value = read_query(connection, """SELECT CLOSE FROM {0} WHERE DATENTIME LIKE "{1}_20:00:00" """.format(stock, result[i+1][0]))[0][0]
            close_value = read_query(connection, """SELECT CLOSE FROM {0} WHERE DATENTIME LIKE "{1}_20:00:00" """.format(stock, result[i][0]))[0][0]
            high = [hourly_data[stock][val]['High'] for val in hourly_data[stock] if(str(result[i][0]) == str(val).split()[0])]
            low = [hourly_data[stock][val]['Low'] for val in hourly_data[stock] if(str(result[i][0]) == str(val).split()[0])]

            daily_stock_data[result[i][0]] = {"Open": open_value, "High": max(high), "Low": min(low), "Close": close_value}

        daily_data[stock] = daily_stock_data
    return daily_data
    
update_daily_data(connection, stocks)


# In[76]:


#OBTAIN WEEKLY DATA
def update_weekly_data(stocks):
    weekly_stock_data = {}
    temp_data = {}

    for stock in stocks:
        temp = list(daily_data[stock]) #Stores all dates
        weekly_stock_data[stock] = {}
        for i in range(0, len(temp)-5, 5):
            open_value = daily_data[stock][temp[i+5]]['Close']
            close_value = daily_data[stock][temp[i]]['Close']
            high_value = [daily_data[stock][temp[j]]['High'] for j in range(i, i+5)]
            low_value = [daily_data[stock][temp[j]]['Low'] for j in range(i, i+5)]
            
            weekly_stock_data[stock][temp[i]] = {'Open': open_value, "High": max(high_value), "Low": min(low_value), "Close": close_value}
    print(weekly_stock_data)
update_weekly_data(stocks)

