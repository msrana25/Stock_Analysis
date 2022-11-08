import requests
from mysql.connector import Error
import soen_6441_database_connection
from datetime import datetime, date, timedelta
import plotly.graph_objects as go

stocks = ['GOOGL', 'META', 'AAPL', 'AMZN']

# Creating connection with mysql
connection = soen_6441_database_connection.server_connection('localhost', 'root', soen_6441_database_connection.get_db_password())


def execute_query(ser_connection, query):
    cursor = ser_connection.cursor()
    try:
        cursor.execute(query)
        ser_connection.commit()
        print("Query successful")
    except Error as err:
        print(f"Error: '{err}'")


execute_query(connection, "use SOEN6441")


# ID INT AUTO_INCREMENT PRIMARY KEY,
def create_stock_tables(all_stocks):
    for stock in all_stocks:
        create_table = """CREATE TABLE %s ( ID INT PRIMARY KEY,
                                            STOCKSYMBOL VARCHAR(20),
                                            DATENTIME DATETIME NOT NULL,
                                            OPEN FLOAT(9, 4),
                                            HIGH FLOAT(9, 4),
                                            LOW FLOAT(9, 4),
                                            CLOSE FLOAT(9, 4)
                                            )""" % stock

        execute_query(connection, create_table)


def get_values(data):
    intraday_values = []
    for date_time in data:
        intraday_values.append([date_time, float(data[date_time]['1. open']), float(data[date_time]['2. high']),
                                float(data[date_time]['3. low']), float(data[date_time]['4. close'])])
    return intraday_values


# Insert intraday data for all stocks in respective stock tables
def update_intraday_values(individual_stocks, interval='15min'):
    for stock in individual_stocks:
        execute_query(connection, """DELETE FROM %s""" % stock)
        url = 'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&interval=%s&symbol=%s&outputsize=full' \
              '&apikey=7NVAEAZYAXPYKMZB' % (interval, stock)
        intraday_data = requests.get(url).json()
        values = get_values(intraday_data['Time Series (%s)' % interval])
        for n, i in zip(range(1, len(values) + 1), values):
            intraday_update_query = """INSERT INTO %s VALUES (%s, '%s', '%s', %f, %f, %f, %f)""" % (
                stock, n, intraday_data['Meta Data']['2. Symbol'], i[0], i[1], i[2], i[3], i[4])
            execute_query(connection, intraday_update_query)
            delete_friday_junk_values = """ delete from %s where date(datentime) = '2022-09-23' """ % stock
            execute_query(connection, delete_friday_junk_values)


def read_query(ser_connection, query):
    cursor = ser_connection.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except Error as err:
        print(f"Error: '{err}'")


# added below function for visualising intraday values
def visualise_intraday_values(ser_connection, all_stocks):
    for stock in all_stocks:
        cursor = ser_connection.cursor()
        intraday_data = {}

        get_intraday = """SELECT DATENTIME FROM {0} """.format(stock)
        cursor.execute(get_intraday)
        result = cursor.fetchall()

        for i in range(len(result) - 1):
            open_value = read_query(ser_connection,
                                    """SELECT OPEN FROM {0} where DATENTIME LIKE "{1}" """.format(stock, result[i][0]))[
                0][0]
            close_value = read_query(ser_connection,
                                     """SELECT CLOSE FROM {0} where DATENTIME LIKE "{1}" """.format(stock,
                                                                                                    result[i][0]))[0][0]
            high_value = read_query(ser_connection,
                                    """SELECT HIGH FROM {0} where DATENTIME LIKE "{1}" """.format(stock, result[i][0]))[
                0][0]
            low_value = read_query(ser_connection,
                                   """SELECT LOW FROM {0} where DATENTIME LIKE "{1}" """.format(stock, result[i][0]))[
                0][0]
            intraday_data[result[i][0]] = {"Open": open_value, "High": high_value, "Low": low_value,
                                           "Close": close_value}

        # print(stock, intraday_data)
        return intraday_data


visualise_intraday_values(connection, stocks)


# OBTAIN HOURLY DATA

def update_hourly_data(ser_connection, all_stocks, interval='15min'):
    for stock in all_stocks:
        cursor = ser_connection.cursor()
        hourly_data = {}

        get_hour = """SELECT DATENTIME FROM {0} WHERE DATENTIME LIKE "%:00:00" group by 1 """.format(stock)
        get_interval = """SELECT DATENTIME FROM {0} WHERE DATENTIME LIKE "%:00" group by 1 """.format(stock)

        cursor.execute(get_hour)
        result = cursor.fetchall()
        cursor.execute(get_interval)
        res1 = cursor.fetchall()

        # print(str(result[0][0]).split()[0])

        for i, j in zip(range(len(result) - 1), range(4, len(res1), 4)):
            high_value, low_value = [], []
            open_value = read_query(ser_connection,
                                    """SELECT CLOSE FROM {0} WHERE DATENTIME LIKE "{1}" """.format(stock,
                                                                                                   result[i + 1][0]))[
                0][0]
            close_value = read_query(ser_connection,
                                     """SELECT CLOSE FROM {0} WHERE DATENTIME LIKE "{1}" """.format(stock,
                                                                                                    result[i][0]))[0][0]
            for hours in range(j - 4, j):
                high_value.append(read_query(ser_connection,
                                             """SELECT HIGH FROM {0} WHERE DATENTIME LIKE "{1}" """.format(stock,
                                                                                                           res1[hours][
                                                                                                               0]))[0][
                                      0])
                low_value.append(read_query(ser_connection,
                                            """SELECT LOW FROM {0} WHERE DATENTIME LIKE "{1}" """.format(stock,
                                                                                                         res1[hours][
                                                                                                             0]))[0][0])
            hourly_data[result[i][0]] = {"Open": open_value, "High": max(high_value), "Low": min(low_value),
                                         "Close": close_value}

        return hourly_data
        # print(stock, hourly_data)


# update_hourly_data(connection, stocks)


# OBTAIN DAILY DATA
def update_daily_data(ser_connection, all_stocks):
    for stock in all_stocks:
        cursor = ser_connection.cursor()
        daily_data = {}

        get_dates = """SELECT DISTINCT DATE(DATENTIME) FROM {0} WHERE DATENTIME LIKE "%20:00:00" """.format(stock)
        cursor.execute(get_dates)
        result = cursor.fetchall()

        for i in range(len(result) - 1):
            open_value = read_query(ser_connection,
                                    """SELECT CLOSE FROM {0} WHERE DATENTIME LIKE "{1}_20:00:00" """.format(stock,
                                                                                                            result[
                                                                                                                i + 1][
                                                                                                                0]))[0][
                0]
            close_value = read_query(ser_connection,
                                     """SELECT CLOSE FROM {0} WHERE DATENTIME LIKE "{1}_20:00:00" """.format(stock,
                                                                                                             result[i][
                                                                                                                 0]))[
                0][0]
            high_value = read_query(ser_connection,
                                    """SELECT MAX(HIGH) FROM {0} WHERE DATENTIME LIKE "{1}_%" """.format(stock,
                                                                                                         result[i][0]))[
                0][0]
            low_value = read_query(ser_connection,
                                   """SELECT MIN(LOW) FROM {0} WHERE DATENTIME LIKE "{1}_%" """.format(stock,
                                                                                                       result[i][0]))[
                0][0]
            daily_data[result[i][0]] = {"Open": open_value, "High": high_value, "Low": low_value, "Close": close_value}

        # print(stock, daily_data)
        return daily_data


# update_daily_data(connection, stocks)


# OBTAIN Weekly DATA
def update_weekly_data(ser_connection, all_stocks):
    weekly_data = {}
    for stock in all_stocks:
        cursor = ser_connection.cursor()
        cursor1 = ser_connection.cursor()
        get_dates_open = """SELECT DATENTIME FROM {0} where DATENTIME like '%04:15:00' and dayname(date(
        datentime))='Monday' """.format(
            stock)
        cursor.execute(get_dates_open)
        result = cursor.fetchall()
        get_dates_close = """SELECT DATENTIME FROM {0} where DATENTIME like '%20:00:00' and dayname(date(
        datentime))='Friday' """.format(
            stock)
        cursor1.execute(get_dates_close)
        result1 = cursor1.fetchall()
        begin_dt = '2022-09-26'

        for i in range(0, len(result) - 1):
            open_value = read_query(ser_connection,
                                    """ SELECT OPEN FROM {0} WHERE DATENTIME LIKE "{1}" """.format(stock,
                                                                                                   result[i][0]))[0][0]
            close_value = read_query(ser_connection,
                                     """SELECT CLOSE FROM {0} WHERE DATENTIME LIKE "{1}" """.format(stock,
                                                                                                    result1[i][0]))[0][
                0]
            high_value = read_query(ser_connection,
                                    """SELECT MAX(HIGH) FROM {0} WHERE DATE(DATENTIME) between '{1}' and '{1}'+Interval '6' day """.format(
                                        stock, begin_dt))[0][0]
            low_value = read_query(ser_connection,
                                   """SELECT MIN(LOW) FROM {0} WHERE DATE(DATENTIME) between '{1}' and '{1}'+Interval '6' day """.format(
                                       stock, begin_dt))[0][0]
            intermediate = datetime.strptime(begin_dt, '%Y-%m-%d')
            intermediate = (intermediate + timedelta(days=7))
            begin_dt = str(intermediate.date())
            weekly_data[result[i][0].date()] = {"Open": open_value, "High": high_value, "Low": low_value,
                                                "Close": close_value}

    return weekly_data


# update_weekly_data(connection, ['GOOGL'])


# plotting the graph for intraday, hourly, daily and weekly

def graph_visualisation(x_axis, y_axis, custom, label):
    fig = go.Figure()

    fig.add_trace(
        go.Scatter(
            x=x_axis,
            y=y_axis,
            customdata=custom,
            hovertemplate='<br>'.join([
                'Open : $%{y:.2f}',
                'Date : %{x}',
                '%{customdata[0]}',
                '%{customdata[1]}',
                '%{customdata[2]}'

            ]),
        )
    )
    fig.update_layout(
        xaxis_title="Date", yaxis_title=f'{label}'
    )
    fig.show()


def plot(ser_connection, frequency, stock_name):
    if frequency == 'weekly':
        res = update_weekly_data(ser_connection, stock_name)

    if frequency == 'daily':
        res = update_daily_data(ser_connection, stock_name)

    if frequency == 'hourly':
        res = update_hourly_data(ser_connection, stock_name)

    if frequency == 'intraday':
        res = visualise_intraday_values(ser_connection, stock_name)

    output_list_key = []
    output_list_open = []
    output_list_remaining_final = []

    for key, value in res.items():
        output_list_remaining = []
        output_list_key.append(key)
        output_list_open.append(value['Open'])
        output_list_remaining.append("High : " + str(value['High']))
        output_list_remaining.append("Low : " + str(value['Low']))
        output_list_remaining.append("Close : " + str(value['Close']))
        output_list_remaining_final.append(output_list_remaining)

    output_list_remaining_final.reverse()
    graph_visualisation(output_list_key, output_list_open, output_list_remaining_final,
                        stock_name[0].upper())


plot(connection, 'daily', ['googl'])
