"""
Config Class

create collections: db_seed(), db_seed_station(), db_seed_weather_code(), db_seed_last_update()
get db/collection handlers: 
extract info from collections: get_station_from_code(), get_weather_from_code()
"""

import os
from .DB import mongodb
from . import utils
import time
from datetime import datetime

class Config:

    CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
    CONFIG_DIR = os.path.join(CURRENT_DIR, 'config')
    FULL_STATION_CONFIG_DIR = os.path.join(CONFIG_DIR, 'full_station_config.json')
    FORECAST_STATION_CONFIG_DIR = os.path.join(CONFIG_DIR, 'forecast_station_config.json')
    DB_CONFIG_DIR = os.path.join(CONFIG_DIR, 'db_config.json')
    WEATHER_CODE_DIR = os.path.join(CONFIG_DIR, 'weather_code.json')
    STATION_CONFIG_DIR = os.path.join(CONFIG_DIR, 'station_config.json')
    AIR_STATION_CONFIG_DIR = os.path.join(CONFIG_DIR, 'air_station_config.json')

    _db_config = utils.parse_json_file(DB_CONFIG_DIR)
    _station_config = utils.parse_json_file(STATION_CONFIG_DIR)
    _air_station_config = utils.parse_json_file(AIR_STATION_CONFIG_DIR)
    _full_station_config = utils.parse_json_file(FULL_STATION_CONFIG_DIR)
    _forecast_config = utils.parse_json_file(FORECAST_STATION_CONFIG_DIR)
    _weather_code = (utils.parse_json_file(WEATHER_CODE_DIR))['weather_code']
    _db_handler = None


    @classmethod
    def get_collection_handler(cls, collection_name):
        db_handler = cls._create_db_handler()
        collection = cls._db_config['weather_db']['collections'][collection_name] #wired, change latter
        return db_handler.get_collection(collection)   

    @classmethod
    def get_collection_name(cls, collection_name):
        db_handler = cls._create_db_handler()
        return cls._db_config['weather_db']['collections'][collection_name]

    @classmethod
    def _create_db_handler(cls):
        """
        Create a database handler, only create once.
        :return: a handler for the database
        """
        if cls._db_handler == None:
            weather_db = cls._db_config['weather_db']
            cls._db_handler = mongodb.MongoDB(weather_db['db_name'], weather_db['host'], weather_db['port'])
        return cls._db_handler
    
    @classmethod
    def db_seed(cls):
        """
        Create a collection containing configuration information for stations
        :return:
        """
        cls.db_seed_station()
        cls.db_seed_air_station()
        cls.db_seed_weather_code()
        cls.db_seed_last_update()

        
    @classmethod
    def db_seed_station(cls):
        # db_handler = cls._create_db_handler()
        # collection_name = cls.get_collection_handler_name('station')
        # db_handler.drop_collection(collection_name)
        collection = cls.get_collection_handler('station')

        for station in cls._full_station_config['Stations']:
            station['has_forecast'] = False
            collection.replace_one({'station_code': station['station_code']},station,True)
        for station_code in cls._forecast_config:
            collection.update_many({'station_code': station_code.lower()}, {'$set': {'has_forecast': True}})

    @classmethod
    def db_seed_air_station(cls):
        # db_handler = cls._create_db_handler()
        # collection_name = cls.get_collection_handler_name('station')
        # db_handler.drop_collection(collection_name)
        collection = cls.get_collection_handler('air_station')

        for station in cls._air_station_config['stations']:
            collection.insert_one(station)

    @classmethod
    def db_seed_weather_code(cls):
        collection = cls.get_collection_handler('weather_code')
        for code, info in cls._weather_code.items():
            info["code"] = code
            collection.replace_one({'code': code}, info, True)

    
    @classmethod
    def db_seed_last_update(cls):
        last_collection = cls.get_collection_handler('last_update')
        collections = cls._db_config['weather_db']['collections']
        for collection_name in collections.values():
            if last_collection.find_one({'collection_name': collection_name}) is None:
                # timestr = time.strftime('%Y%m%d%H%M%S', time.gmtime())
                timestr = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
                last_collection.insert_one({
                    'collection_name': collection_name,
                    'last_update_time': timestr
                    })
    @classmethod
    def get_station_from_code(cls, code):
        code = code.upper()
        station_list = cls._station_config['service_content']['station_details']
        for station in station_list:
            if station['station_code'] == code:
                return station['station_name']['en']
        return code+' unfound'

# the name of weather stations are chinese or code
# the name of air stations are english or code
# so, uniform them with station code, upper case

    @classmethod
    def get_code_from_station(cls, stn):
        station_list = cls._station_config['service_content']['station_details']
        for station in station_list:
            if station['station_name']["en"] == stn:
                return station['station_code']
        return stn+' unfound'

    @classmethod
    def get_weather_from_code(cls, code):
        weather_list = cls._weather_code
        for key, value in weather_list:
            if key == code:
                return value
        return code+' unfound'
