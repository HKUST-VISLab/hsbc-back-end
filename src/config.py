"""
Config Class
"""

import os
from .DB import mongodb
from . import utils

class Config:

    CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
    CONFIG_DIR = os.path.join(CURRENT_DIR, 'config')
    FULL_STATION_CONFIG_DIR = os.path.join(CONFIG_DIR, 'full_station_config.json')
    FORECAST_STATION_CONFIG_DIR = os.path.join(CONFIG_DIR, 'forecast_station_config.json')
    DB_CONFIG_DIR = os.path.join(CONFIG_DIR, 'db_config.json')
    WEATHER_CODE_DIR = os.path.join(CONFIG_DIR, 'weather_code.json')

    _db_config = utils.parse_json_file(DB_CONFIG_DIR)
    # _station_config = utils.parse_json_file(STATION_CONFIG_DIR)
    _full_station_config = utils.parse_json_file(FULL_STATION_CONFIG_DIR)
    _forecast_config = utils.parse_json_file(FORECAST_STATION_CONFIG_DIR)
    _db_handler = None


    @classmethod
    def get_collection_handler(cls, collection_name):
        db_handler = cls._create_db_handler()
        collection = cls._db_config['weather_db']['collections'][collection_name]
        return db_handler.get_collection(collection)


    @classmethod
    def _create_db_handler(cls):
        """
        Create a database handler
        :return: a handler for the database
        """
        if cls._db_handler == None:
            weather_db = cls._db_config['weather_db']
            cls._db_handler = mongodb.MongoDB(weather_db['db_name'], weather_db['host'], weather_db['port'])
        return cls._db_handler