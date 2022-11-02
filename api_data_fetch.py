import mysql.connector
import requests

# READ Data on Daily Time Series for Google - Symbol: GOOGL
url_1 = 'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=GOOGL&interval=30min&outputsize=full' \
        '&apikey=7NVAEAZYAXPYKMZB '

# READ Data on Daily Time Series for Tesla - Symbol: TSLA
url_2 = 'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=TSLA&interval=30min&outputsize=full' \
        '&apikey=7NVAEAZYAXPYKMZB '

# READ Data on Daily Time Series for Amazon - Symbol: AMZN
url_3 = 'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=AMZN&interval=30min&outputsize=full' \
        '&apikey=7NVAEAZYAXPYKMZB '

# READ Data on Daily Time Series for Meta - Symbol: META
url_4 = 'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=META&interval=30min&outputsize=full' \
        '&apikey=7NVAEAZYAXPYKMZB '

# READ Data on Daily Time Series for Apple - Symbol: AAPL
url_5 = 'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=AAPL&interval=30min&outputsize=full' \
        '&apikey=7NVAEAZYAXPYKMZB '


# Storing api stock-API data url for a company and its name as key,value pairs
url_collection = {url_1: 'google', url_2: 'tesla', url_3: 'amazon', url_4: 'meta', url_5: 'apple'}

# Define database credentials and connect to mySQL database
mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="ManuRana007$",
    database="mydatabase"
)

my_cursor = mydb.cursor()

for url, value in url_collection.items():

    response = requests.get(url)
    # Fetching the JSON data from API
    original_data = response.json()
    rowcount = 0
    # Transforming the JSON data to extract the required info
    data = original_data['Time Series (30min)']
    keys = original_data['Time Series (30min)'].keys()

    for item in keys:
        key = item
        op = data[item]['1. open']
        high = data[item]['2. high']
        low = data[item]['3. low']
        close = data[item]['4. close']
        volume = data[item]['5. volume']
        rowcount += my_cursor.rowcount

        # Inserting JSON data from API into mySQL database
        sql = f"INSERT INTO {value} (timeseries, open, high, low, close, volume) VALUES (%s, %s, %s, %s, %s, %s)"
        val = (key, op, high, low, close, volume)
        my_cursor.execute(sql, val)

    mydb.commit()
    print(rowcount, f"records inserted in table {value}")

