import Database as db
import System
import pytest
import requests


# The API allows 5 calls/min. The test cases annotated with @pytest.mark.skip should be run only one at a time. Once
# a test case marked with @pytest.mark.skip is commented and run, please wait for a minute to run the next test case
# to allow API call limit to be reset.

class Test:

    def test_working_api_connection(self):
        url = 'https://www.alphavantage.co/query?function=OVERVIEW&symbol=GOOGL&apikey=7NVAEAZYAXPYKMZB'
        stock_data = requests.get(url)
        assert stock_data.status_code == 200

    def test_database_connection(self):
        db_obj = db.Database('localhost', 'root', "#abcd", 'soen6441', ['GOOGL'])
        out = str(type(db_obj.ser_connection))
        assert out == '<class \'mysql.connector.connection_cext.CMySQLConnection\'>'

    @pytest.fixture()
    def database_object(self):
        db_obj = db.Database('localhost', 'root', "#abcd", 'soen6441', ['GOOGL'])
        return db_obj

    @pytest.fixture()
    def system_object(self):
        sys_obj = System.System()
        return sys_obj

    @pytest.fixture()
    def intraday_dict(self, system_object):
        intraday_dict = system_object.get_intraday_data(['GOOGL'])
        return intraday_dict

    @pytest.mark.skip
    def test_database_loading(self, database_object, system_object, intraday_dict):
        connection = database_object.ser_connection
        count = database_object.read_query(connection, "select count(1) from GOOGL")
        assert (count[0][0] >= 1000)

    @pytest.mark.skip
    def test_data_values_intraday(self, database_object, system_object):
        system_object.get_intraday_data(['GOOGL'])
        out_intraday_close = system_object.intraday_data['GOOGL']['2022-11-11 18:15:00']['Close']
        out_intraday_low = system_object.intraday_data['GOOGL']['2022-11-11 18:15:00']['Low']
        assert out_intraday_close == 96.24
        assert out_intraday_low == 96.2

    @pytest.mark.skip
    def test_data_values_hourly(self, database_object, system_object):
        connection = database_object.ser_connection
        system_object.get_hourly_data(database_object.ser_connection)
        out_hourly_open = system_object.hourly_data['GOOGL']['2022-11-11 18:00:00']['Open']
        out_hourly_low = system_object.hourly_data['GOOGL']['2022-11-11 18:00:00']['Low']
        assert out_hourly_open == 96.2
        assert out_hourly_low == 96.1

    @pytest.mark.skip
    def test_data_values_daily(self, database_object, system_object):
        connection = database_object.ser_connection
        system_object.get_daily_data(database_object.ser_connection)
        out_daily_high = system_object.daily_data['GOOGL']['2022-11-11']['High']
        out_daily_low = system_object.daily_data['GOOGL']['2022-11-11']['Low']
        assert out_daily_high == 96.93
        assert out_daily_low == 93.706

    @pytest.mark.skip
    def test_data_values_weekly(self, database_object, system_object):
        connection = database_object.ser_connection
        system_object.get_daily_data(connection)
        out_weekly_high = system_object.weekly_data['GOOGL']['2022-11-04']['High']
        out_weekly_open = system_object.weekly_data['GOOGL']['2022-11-04']['Open']
        assert out_weekly_high == 97.03
        assert out_weekly_open == 96.33
