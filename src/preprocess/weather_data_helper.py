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
import time

# Configurations
STATION_CONFIG_DIR = 'config/station_config.json'
FULL_STATION_CONFIG_DIR = 'config/full_station_config.json'
FORECAST_STATION_CONFIG_DIR = 'config/station_config.json'
DB_CONFIG_DIR = 'config/db_config.json'

# Constants
ERROR_FETCH = 1
SUCCESS = 0

# logger config in this module
_logger = utils.Logger(__name__)
_db_config = utils.parse_json_file(DB_CONFIG_DIR)
_station_config = utils.parse_json_file(STATION_CONFIG_DIR)
_full_station_config = utils.parse_json_file(FULL_STATION_CONFIG_DIR)
_forecast_config = utils.parse_json_file(FORECAST_STATION_CONFIG_DIR)


# def log_to_file(filename=None, level=logging.INFO):
#     """
#     Set log file to filename
#     :param filename: log file name
#     :param level: logging level
#     :return: None
#     """
#     _logger.setLevel(level)
#     if filename is None:
#         handler = logging.FileHandler('weather.log', 'w')
#         handler.setFormatter(_log_format)
#         _logger.addHandler(handler)
#     else:
#         handler = logging.FileHandler(filename, 'w')
#         handler.setFormatter(_log_format)
#         _logger.addHandler(handler)


def fetch_weather_data_of_site(grid_id):
    """
    Deprecated. Fetch weather data of a site by the site's grid_id from hko
    :param grid_id: grid id of a station, you can find the grid id in the station_config.json file
    :return:
        a dict containing weather data, if return a string, it means an error occurs when requesting data
    """
    if not isinstance(grid_id, str):
        grid_id = str(grid_id)
        if len(grid_id) == 3:
            grid_id = '0'+grid_id
    try:
        assert grid_id.isdigit()
    except AssertionError:
        _logger.error('grid_id should be digital from 0 to 9')

    path = 'http://www.hko.gov.hk/PDADATA/locspc/data/gridData/' + grid_id + '_en.xml'

    try:
        response = urllib.request.urlopen(path)
    except HTTPError as e:
        data = str(e.code)
        _logger.error('HTTPError = ' + data + '. Check grid_id!')
    except URLError as e:
        data = str(e.reason)
        _logger.error('URLError = ' + data)
    else:
        data = utils.parse_json_file(response)
    return data


def fetch_weather_data():
    """
    Deprecated. Fetch weather data for all stations
    :return: a list of all fetched data, elements could be error code string or data dict
    """
    stations = _station_config['service_content']['station_details']
    data_list = []
    num = 0
    for station in stations:
        data = fetch_weather_data_of_site(station['gridID'])
        data_list.append(data)
        if not isinstance(data_list[-1], str):
            num += 1
    tm = str(data_list[0]['RegionalWeather']['ObsTime'])
    _logger.info('Weather data of ' + str(num) + '/34 stations at time ' + tm + ' are fetched.')
    return data_list


def fetch_forecast_of_site(station_code):
    """
    Fetch forecast data of station with station_code
    :param station_code: code of station, should be of string type, specified in forecast_station_config
    :return: forecast data of the site
    """

    path = 'http://maps.weather.gov.hk/ocf/dat/' + station_code + '.xml'

    try:
        response = urllib.request.urlopen(path)
    except HTTPError as e:
        data = str(e.code)
        _logger.error('HTTPError = ' + data + '. Check station code!')
    except URLError as e:
        data = str(e.reason)
        _logger.error('URLError = ' + data)
    else:
        data = utils.parse_json_file(response)
    return data


def fetch_forecast_data():
    """
    Fetch forecast weather data for all stations
    :return: a list of all fetched data, elements could be error code string or data dict
    """
    stations = utils.parse_json_file(FORECAST_STATION_CONFIG_DIR)
    data_list = []
    num = 0
    for code in stations:
        data_list.append(fetch_forecast_of_site(code))
        if not isinstance(data_list[-1], str):
            num += 1
    tm = str(data_list[0]['LastModified'])
    _logger.info('Forecast data of ' + str(num) + '/16 stations at time ' + tm + ' are fetched.')
    return data_list


def reformatting_raw_weather_data(raw_data):
    """
    Deprecated. Reformatting the data fetched by fetch_weather_data()
    :return: a list of reformatted data
    """
    formatted_data = []
    for raw in raw_data:
        rgdata = raw['RegionalWeather']
        rainfall = raw['Rainfall']
        rh = rgdata['RH']
        temp = rgdata['Temp']
        wind = rgdata['Wind']

        grid_id = rgdata['Grid']
        obs_time = rgdata['ObsTime']

        # cleaning temp
        del temp['ModelMaxTemperature']
        del temp['ModelMinTemperature']
        del temp['Temperature_AroundtoOdd']

        # checking station matching problems

        formatted_data.append({
            'GridId': grid_id,
            'ObsTime': obs_time,
            'Rainfall': rainfall,
            'RelativeHumidity': rh,
            'Temp': temp,
            'Wind': wind
        })

    return formatted_data


def fetch_and_store_weather_data(forecast=False):
    """
    Fetch and store weather data from the maps.weather.gov.hk
    :param forecast: indicating forecast or not
    :return: 1 if error in fetching data, 0 if function succeed
    """
    db_handler = _create_db_handler()
    collections = _db_config['weather_db']['collections']
    if not forecast:
        current = db_handler.get_collection(collections['current'])
        data_list = fetch_full_weather_data()
        if isinstance(data_list, str):
            return ERROR_FETCH
        for data in data_list:
            current.replace_one({'stn': data['stn'], 'time': data['time']}, data, upsert=True)
    return SUCCESS


def fetch_full_weather_data():
    """
    Fectch weather data through another source from maps.weather.gov.hk
    :return:  a list of all fetched data, elements could be error code string or data dict
    """
    stations = _full_station_config['Stations']
    path = "http://maps.weather.gov.hk/r4/input_files/latestReadings_AWS1"
    response = utils.safe_open_url(path)
    if isinstance(response, str):
        return response
    raw = utils.parse_csv_file(response)
    title = raw[0][0].split()
    timestr = title[4] + '-' + title[8] + '-' + title[9] + '-' + title[10]
    tm = time.strptime(timestr, '%H:%M-%d-%B-%Y')
    timestr = time.strftime('%Y%m%d%H%M', tm)
    data = []
    for row in raw[2:]:
        _dict = {}
        for i, var in enumerate(raw[1]):
            if len(var) != 0: # and len(row[i])!=0:
                var = var.lower()
                _dict[var] = row[i]
        data.append(_dict)

    for row in data:
        row['has_wind'] = False if row['winddirection'] == '' else True
        row['has_temp'] = False if row['temp'] == '' else True
        row['has_rh'] = False if row['rh'] == '' else True
        row['has_grasstemp'] = False if row['grasstemp'] == '' else True
        row['has_visibility'] = False if row['visibility'] == '' else True
        row['has_pressure'] = False if row['pressure'] == '' else True
        row['time'] = timestr
    return data


def create_statation_collection():
    """
    Create a collection containing configuration information for stations
    :return:
    """
    db_handler = _create_db_handler()
    collection = db_handler.get_collection(_db_config['weather_db']['collections']['station'])
    for station in _full_station_config['Stations']:
        collection.replace_one(station, station, True)


def _create_db_handler():
    """
    Create a database handler
    :return: a handler for the database
    """
    weather_db = _db_config['weather_db']
    return mongodb.MongoDB(weather_db['db_name'], weather_db['host'], weather_db['port'])


if __name__ == '__main__':
    # log_to_file('./weather.log')
    # a = fetch_full_weather_data()
    # # fecth_weather_data_of_site(2207)
    # data_list = fetch_weather_data()
    # print(a)
    create_statation_collection()
