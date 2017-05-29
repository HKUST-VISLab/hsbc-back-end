"""
Helpers that fetch and pre-process weather data
Weather data and forecast data do not follow the same station configuration
"""

import urllib
from urllib import request, error, parse
from urllib.error import HTTPError, URLError
import logging
from src import DB
from src.DB import document_db
from src.DB import mongodb
from src import utils
from src.utils import safe_open_url
from src.config import Config
import time
import os.path
from src.utils import time_convert
from datetime import datetime
# Configurations
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
STATION_CONFIG_DIR = os.path.join(CURRENT_DIR, '../config/station_config.json')
FULL_STATION_CONFIG_DIR = os.path.join(CURRENT_DIR, '../config/full_station_config.json')
FORECAST_STATION_CONFIG_DIR = os.path.join(CURRENT_DIR, '../config/forecast_station_config.json')
DB_CONFIG_DIR = os.path.join(CURRENT_DIR, '../config/db_config.json')
WEATHER_CODE_DIR = os.path.join(CURRENT_DIR, '../config/weather_code.json')

# Constants
ERROR_FETCH = 1
SUCCESS = 0

class WeatherFetcher:

    # logger config in this module
    _logger = utils.Logger(__name__)
    # _db_config = utils.parse_json_file(DB_CONFIG_DIR)
    # _station_config = utils.parse_json_file(STATION_CONFIG_DIR)
    # _full_station_config = utils.parse_json_file(FULL_STATION_CONFIG_DIR)
    # _forecast_config = utils.parse_json_file(FORECAST_STATION_CONFIG_DIR)
    _current_weather_handler = Config.get_collection_handler('current')
    _forecast_weather_handler = Config.get_collection_handler('forecast')
    _station_handler = Config.get_collection_handler('station')
    _forecast_stations = _station_handler.find({'has_forecast': True})
    _last_update_handler = Config.get_collection_handler('last_update')

    @classmethod
    def fetch_forecast_of_site(self, station_code):
        """
        Fetch forecast data of station with station_code
        :param station_code: code of station, should be of string type, specified in forecast_station_config
        :return: forecast data of the site
        """
        station_code = station_code.upper()

        path = 'http://maps.weather.gov.hk/ocf/dat/' + station_code + '.xml'

        response = utils.safe_open_url(path)
        if isinstance(response, str):
            return response
        data = utils.parse_json_file(response)
        return data

    @classmethod
    def fetch_forecast_data(self):
        """
        Fetch forecast weather data for all stations
        :return: a list of all fetched data, elements could be error code string or data dict
        """
        stations = self._forecast_stations
        data_list = []
        num = 0
        for station in stations:
            data_list.append(self.fetch_forecast_of_site(station['station_code']))
            if not isinstance(data_list[-1], str):
                num += 1
        if(num == 0):
            return '404'
        tm = str(data_list[0]['LastModified'])
        self._logger.info('Forecast data of ' + str(num) + '/16 stations at time ' + tm + ' are fetched.')
        
        return self.reformatting_raw_forecast_data(data_list)

    @classmethod
    def reformatting_raw_forecast_data(self, raw_data):
        """
        Deprecated. Reformatting the raw forecast data fetched by fetch_forecast_data()
        :return: a list of reformatted forecast data
        """
        if isinstance(raw_data, str):
            return raw_data

        for raw in raw_data:
            # key_list = ['Latitude', 'Longitude', 'DailyForecast']
            # for key in key_list:
            #     _s_key = utils.camel2snake(key)
            #     raw[_s_key] = raw.pop(key)
            # raw['model_time'] = time_convert(str(raw.pop('ModelTime')), '%Y%m%d%H')
            # raw['stn'] = raw.pop('StationCode')
            # raw['hourly_forecast'] = raw.pop('HourlyWeatherForecast')
            # raw['time'] = time_convert(str(raw.pop('LastModified')), '%Y%m%d%H%M%S')
            # raw_daily = raw['daily_forecast']
            # raw_hourly = raw['hourly_forecast']

            for key in raw:
                if key == 'ModelTime':
                    raw[key] = time_convert(raw[key], '%Y%m%d%H')
                elif key == 'LastModified':
                    raw[key] = time_convert(raw[key], '%Y%m%d%H%M%S')
                raw[utils.camel2snake(key)] = raw.pop(key)
            try:
                raw['loc'] = [raw.pop("latitude"), raw.pop("longitude")]
            except Exception:
                pass
            # turn keys in daily forecast into snake case

            # daily_key_list = [name for name in raw_daily[0]]
            # s_daily_key_list = [utils.camel2snake(name) for name in daily_key_list]
            # for onedaily in raw_daily:
            #     for i, key in enumerate(s_daily_key_list):
            #         onedaily[key] = onedaily.pop(daily_key_list[i])
            #     onedaily['forecaset_date']=time_convert(onedaily['forecast_date'], '%Y%m%d')
            for oneday in raw['daily_forecast']:
                for key in oneday:
                    oneday[utils.camel2snake(key)] = oneday.pop(key)
                oneday['forecast_date'] = time_convert(oneday['forecast_date'], '%Y%m%d')

            # # turn keys in hourly forecast into snake case

            # hourly_key_list = [name for name in raw_hourly[0]]
            # s_hourly_key_list = [utils.camel2snake(name) for name in hourly_key_list]
            # formatted_hourly = []
            # hourly_weather_forecast = []
            # for onehourly in raw_hourly:
            #     # deal with some incomplete data
            #     if len(onehourly) > 4:
            #         _dict = {}
            #         for i, key in enumerate(s_hourly_key_list):
            #             _dict[key] = onehourly[hourly_key_list[i]]
            #         formatted_hourly.append(_dict)
            #     if len(onehourly) != 5:
            #         key_list = ['ForecastWeather', 'ForecastHour']
            #         _temp = {utils.camel2snake(key): onehourly[key] for key in key_list}
            #         hourly_weather_forecast.append(_temp)
            # raw['hourly_forecast'] = formatted_hourly
            # raw['hourly_weather_forecast'] = hourly_weather_forecast
            for onehour in raw['hourly_weather_forecast']:
                for key in onehour:
                    onehour[utils.camel2snake(key)] = onehour.pop(key)
                onehour['forecast_hour'] = time_convert(onehour['forecast_hour'], '%Y%m%d%H')

        return raw_data

    @classmethod
    def fetch_and_store_weather_data(self, forecast=False):
        """
        Fetch and store weather data from the maps.weather.gov.hk
        :param forecast: indicating forecast or not
        :return: 1 if error in fetching data, 0 if function succeed
        """
        collection_str = 'current'
        collection_handler = None
        data_list = None
        if not forecast:
            collection_str = 'forecast'
            data_list = self.fetch_full_weather_data()
            collection_handler = self._current_weather_handler
        else:
            data_list = self.fetch_forecast_data()
            collection_handler = self._forecast_weather_handler

        if isinstance(data_list, str):
            self._logger.error("Unable to fetch " + collection_str + " weather data.")
            return ERROR_FETCH
        self._logger.info(collection_str + " weather data fetched.")
        
        try:
            modified_time = data_list[0]['last_modified']
        except:
            modified_time = data_list[0]['time']

        if self._update_time(modified_time, forecast):
            collection_handler.insert_many(data_list)
            self._logger.info("database updated successfully with " + collection_str + " weather data.")
        else:
            self._logger.info(collection_str + " datasets are the newest, no need to update.")

        return SUCCESS

    @classmethod
    def fetch_full_weather_data(self):
        """
        Fectch weather data through another source from maps.weather.gov.hk
        :return:  a list of all fetched data, elements could be error code string or data dict
        """
        # stations = self._full_station_config['Stations']
        path = "http://maps.weather.gov.hk/r4/input_files/latestReadings_AWS1"
        response = utils.safe_open_url(path)
        if isinstance(response, str):
            return response
        raw = utils.parse_csv_file(response)

        # pre-processing
        title = raw[0][0].split()
        timestr = title[4] + '-' + title[8] + '-' + title[9] + '-' + title[10]
        timestr = time_convert(timestr, '%H:%M-%d-%B-%Y')
        # timestr = time.strftime('%Y%m%d%H%M', tm)
        data = []
        for row in raw[2:]:
            _dict = {}
            for i, var in enumerate(raw[1]):
                if len(var) != 0: # and len(row[i])!=0:
                    var = var.lower()
                    _dict[var] = row[i]
            data.append(_dict)

        for row in data:
            # row['has_wind'] = False if row['winddirection'] == '' else True
            # row['has_temp'] = False if row['temp'] == '' else True
            # row['has_rh'] = False if row['rh'] == '' else True
            # row['has_grasstemp'] = False if row['grasstemp'] == '' else True
            # row['has_visibility'] = False if row['visibility'] == '' else True
            # row['has_pressure'] = False if row['pressure'] == '' else True
            row['time'] = timestr
        return data

    # @classmethod
    # def _create_db_handler(self):
    #     """
    #     Create a database handler
    #     :return: a handler for the database
    #     """
    #     weather_db = self._db_config['weather_db']
    #     return mongodb.MongoDB(weather_db['db_name'], weather_db['host'], weather_db['port'])

    @classmethod
    def _update_time(self, time_str, forecast = False):
        """
        use a formatted time string to update record time. return false if not need to update
        :param time_str: a string with format "YYYYmmddHHmm..."
        :param forecast: False if not forecast.
        :return: Boolean
        """
        # time_str = str(time_str)[:12] + '00'
        collection_handler = Config.get_collection_handler('last_update')
        collection_name = ''
        if forecast:
            collection_name = Config.get_collection_name('forecast')
        else:
            collection_name = Config.get_collection_name('current')
        last_record = collection_handler.find_one({'collection_name':collection_name})
        last_update = last_record['last_update_time']
        if time_str != last_update:
            collection_handler.update_one({'collection_name':collection_name}, {'$set': {'last_update_time': time_str}})
            return True
        return False

    # @classmethod
    # def _need_update(self, time_str, forecast = False):
    #     """
    #     use a formatted time string to update record time. return false if not need to update
    #     :param time_str: a string with format "YYYYmmddHHmm..."
    #     :param forecast: False if not forecast.
    #     :return: Boolean
    #     """
    #     time_str = str(time_str)
    #     if forecast:
    #         prefix = time_str[:10]
    #         if prefix != self.update_time_forecast:
    #             return True
    #     else:
    #         prefix = time_str[:12]
    #         if prefix != self.update_time:
    #             return True
    #     return False

    #     @classmethod
    # def fetch_weather_data_of_site(self, grid_id):
    #     """
    #     Deprecated. Fetch weather data of a site by the site's grid_id from hko
    #     :param grid_id: grid id of a station, you can find the grid id in the station_config.json file
    #     :return:
    #         a dict containing weather data, if return a string, it means an error occurs when requesting data
    #     """
    #     if not isinstance(grid_id, str):
    #         grid_id = str(grid_id)
    #         if len(grid_id) == 3:
    #             grid_id = '0'+grid_id
    #     try:
    #         assert grid_id.isdigit()
    #     except AssertionError:
    #         self._logger.error('grid_id should be digital from 0 to 9')

    #     path = 'http://www.hko.gov.hk/PDADATA/locspc/data/gridData/' + grid_id + '_en.xml'

    #     try:
    #         response = urllib.request.urlopen(path)
    #     except HTTPError as e:
    #         data = str(e.code)
    #         self._logger.error('HTTPError = ' + data + '. Check grid_id!')
    #     except URLError as e:
    #         data = str(e.reason)
    #         self._logger.error('URLError = ' + data)
    #     else:
    #         data = utils.parse_json_file(response)
    #     return data

    # @classmethod
    # def fetch_weather_data(self):
    #     """
    #     Deprecated. Fetch weather data for all stations
    #     :return: a list of all fetched data, elements could be error code string or data dict
    #     """
    #     stations = self._station_config['service_content']['station_details']
    #     data_list = []
    #     num = 0
    #     for station in stations:
    #         data = self.fetch_weather_data_of_site(station['gridID'])
    #         data_list.append(data)
    #         if not isinstance(data_list[-1], str):
    #             num += 1
    #     tm = str(data_list[0]['RegionalWeather']['ObsTime'])
    #     self._logger.info('Weather data of ' + str(num) + '/34 stations at time ' + tm + ' are fetched.')
    #     return data_list


if __name__ == '__main__':
    # log_to_file('./weather.log')
    # a = fetch_full_weather_data()
    # # fecth_weather_data_of_site(2207)
    # data_list = fetch_weather_data()
    # print(a)
    WeatherFetcher.fetch_and_store_weather_data(True)
    