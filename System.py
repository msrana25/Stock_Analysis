import requests
import Database as DB


class System:

#Create connection to database
    stocks = ['GOOGL', 'META', 'AAPL', 'AMZN']
    def __init__(self) -> None:
        self.database = DB.Database('localhost', 'root', "#Snroshan1998", 'soen6441', self.stocks)
        # self.connection = self.database.connect_to_database()
        self.intraday_data, self.hourly_data, self.daily_data, self.weekly_data = {}, {}, {}, {}
        self.get_intraday_data(self.stocks)
        self.get_hourly_data()
        self.get_daily_data()
        self.get_weekly_data()
        
# #Save data to respective .json files
#     def savedata(fname, data):
#         with open('json_data/%s.json'%(fname), 'w') as f:
#             f.write(json.dumps(data))

#Get data of stocks and add to database
    def add_stocks(self, stocks):
        for stock in stocks:
             url = 'https://www.alphavantage.co/query?function=OVERVIEW&symbol=%s&apikey=7NVAEAZYAXPYKMZB' % (stock)
             stock_data = requests.get(url).json()
             self.database.add_stocks_to_db(self.database.connection, stock_data)

#Get Intraday data and update the database
    def get_intraday_data(self, stocks, interval = '15min'):
        for stock in stocks:
            values = []
            self.intraday_data[stock] = {}
            url = 'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&interval=%s&symbol=%s&outputsize=full&apikey=7NVAEAZYAXPYKMZB' %(interval, stock)
            intraday = requests.get(url).json()
            data = intraday['Time Series (%s)' %(interval)]
            for datetime in data:
                values.append([datetime, float(data[datetime]['1. open']), float(data[datetime]['2. high']), float(data[datetime]['3. low']), float(data[datetime]['4. close'])])
                self.intraday_data[stock][datetime] = {'Open': float(data[datetime]['1. open']), 'High': float(data[datetime]['2. high']), 'Low': float(data[datetime]['3. low']), 'Close': float(data[datetime]['4. close'])}
            
            self.database.update_intraday_data(self.database.connection, stock, values)



#Compute and Get Hourly Data from the Database
    def get_hourly_data(self):
        for stock in self.stocks:
            self.hourly_data[stock] = self.database.retrieve_hourly_data(self.database.connection, stock)

#Compute and Get Daily Data from the Database and the hourly_data
    def get_daily_data(self):
        for stock in self.stocks:
            self.daily_data[stock] = self.database.retrieve_daily_data(self.database.connection, stock, self.hourly_data)

#Compute and Get Weekly Data from daily_data
    def get_weekly_data(self):
        for stock in self.stocks:
            temp = list(self.daily_data[stock]) #Stores all dates
            self.weekly_data[stock] = {}
            for i in range(0, len(temp)-5, 5):
                open_value = self.daily_data[stock][temp[i+5]]['Close']
                close_value = self.daily_data[stock][temp[i]]['Close']
                high_value = [self.daily_data[stock][temp[j]]['High'] for j in range(i, i+5)]
                low_value = [self.daily_data[stock][temp[j]]['Low'] for j in range(i, i+5)]
                
                self.weekly_data[stock][str(temp[i])] = {'Open': open_value, "High": max(high_value), "Low": min(low_value), "Close": close_value}