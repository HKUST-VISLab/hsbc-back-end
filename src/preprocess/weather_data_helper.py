"""
Helpers that fetch and pre-process weather data
Weather data and forecast data do not follow the same station configuration
"""

import urllib2
import time
import logging
import json

# logger in this module
__logger = logging.getLogger(__name__)
STATION_CONFIG_DIR = 'config/station_config.json'
FORECAST_STATION_CONFIG_DIR = 'config/station_config.json'


def log_to_file(filename=None, level=logging.INFO):
    """
    Set log file to filename
    :param filename: log file name
    :param level: logging level
    :return: None
    """
    if filename == None:
        logging.basicConfig(filename='weather.log', level=level)
    else:
        logging.basicConfig(filename=filename, level=level)


def fetch_weather_data_of_site(grid_id):
    """
    Fetch weather data of a site by the site's grid_id from hko
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
        __logger.error('grid_id should be digital from 0 to 9')

    path = 'http://www.hko.gov.hk/PDADATA/locspc/data/gridData/' + grid_id + '_en.xml'

    try:
        response = urllib2.urlopen(path)
    except urllib2.HTTPError, e:
        data = str(e.code)
        __logger.error('HTTPError = ' + data + '. Check grid_id!')
    except urllib2.URLError, e:
        data = str(e.reason)
        __logger.error('URLError = ' + data)
    else:
        data = json.loads(response.read())
    return data


def fetch_weather_data():
    """
    Fetch weather data for all stations
    :return: a list of all fetched data, elements could be error code string or data dict
    """
    config = open(STATION_CONFIG_DIR).read()
    station_config = json.loads(config)
    stations = station_config['service_content']['station_details']
    data_list = []
    num = 0
    for station in stations:
        data_list.append(fetch_weather_data_of_site(station['gridID']))
        if not isinstance(data_list[-1], str):
            num += 1
    tm = str(data_list[0]['RegionalWeather']['ObsTime'])
    __logger.error('Weather data of ' + str(num) + '/34 stations at time ' + tm + ' are fetched.')
    return data_list


def fetch_forecast_of_site(station_code):
    """
    Fetch forecast data of station with station_code
    :param station_code: code of station, should be of string type, specified in forecast_station_config
    :return: forecast data of the site
    """

    path = 'http://maps.weather.gov.hk/ocf/dat/' + station_code + '.xml'

    try:
        response = urllib2.urlopen(path)
    except urllib2.HTTPError, e:
        data = str(e.code)
        __logger.error('HTTPError = ' + data + '. Check station code!')
    except urllib2.URLError, e:
        data = str(e.reason)
        __logger.error('URLError = ' + data)
    else:
        data = json.loads(response.read())
    return data


def fetch_forecast_data():
    """
    Fetch forecast weather data for all stations
    :return: a list of all fetched data, elements could be error code string or data dict
    """
    config = open(FORECAST_STATION_CONFIG_DIR).read()
    stations = json.loads(config)
    data_list = []
    num = 0
    for code in stations:
        data_list.append(fetch_forecast_of_site(code))
        if not isinstance(data_list[-1], str):
            num += 1
    tm = str(data_list[0]['LastModified'])
    __logger.error('Forecast data of ' + str(num) + '/16 stations at time ' + tm + ' are fetched.')
    return data_list


if __name__ == '__main__':
    log_to_file('weather.log')
    # # fecth_weather_data_of_site(2207)
    # data_list = fetch_weather_data()
    a = fetch_forecast_of_site('SSH')
    print a