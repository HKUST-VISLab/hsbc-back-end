import os
from json import dump
from datetime import datetime
from src.utils import parse_json_file
from src.preprocess.air_quality_fetch_helper import fetch_air_quality
from src.preprocess.weather_data_helper import WeatherFetcher as WF
from src.config import Config

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_DIR = os.path.join(CURRENT_DIR, 'config')
AIR_STATION_DIR = os.path.join(CONFIG_DIR, 'air_station_config.json')
# FULL_STATION_CONFIG_DIR = os.path.join(CONFIG_DIR, 'full_station_config.json')
# FORECAST_STATION_CONFIG_DIR = os.path.join(CONFIG_DIR, 'forecast_station_config.json')
# _full_station_config = parse_json_file(FULL_STATION_CONFIG_DIR)
# _forecast_station_config = parse_json_file(FORECAST_STATION_CONFIG_DIR)

missing = 0

# weather_info = WF.fetch_full_weather_data()
# weather_stations =[]
# for weather_station in weather_info:
#     code = weather_station['stn']
#     # name = Config.get_station_from_code(code)
#     # if name.split(' ')[-1] == 'unfound':
#     #     missing +=1
#     weather_stations.append(code)

# forecast_info = WF.fetch_forecast_data()
# forecast_stations =[]
# for forecast_station in forecast_info:
#     code = forecast_station['station_code']
#     # name = Config.get_station_from_code(code)
#     forecast_stations.append(code)

air_quality = fetch_air_quality()
air_station_list={'update':str(datetime.today()),
                 'info':'air_quality_station',
                 'stations':[]}

for air_station in air_quality:
    name = air_station['station_name']
    # code = Config.get_code_from_station(name)
    # if name.split(' ')[-1] == 'unfound':
    #     missing += 1
    item = {'station_name':name}
    air_station_list['stations'].append(item)


with open(AIR_STATION_DIR, "w") as wf:
    dump(air_station_list, wf)
wf.close()

# # build a full list
# full_stations = []
# for station in weather_info:
#     code = station['stn']
#     item = {}
#     item['code'] = code
#     item['weather'] = True
#     item['forecast'] = False
#     item['air_quality'] = False
#     if code in forecast_stations:
#         item['forecast'] = True
#     if code in air_stations:
#         item['air_quality'] = True
#     full_stations.append(item)

# print(full_stations)
