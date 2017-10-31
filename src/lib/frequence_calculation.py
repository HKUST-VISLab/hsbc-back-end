import time
from src.lib.library import *

def create_index():
    from pymongo import MongoClient
    import pymongo
    client = MongoClient("127.0.0.1", 27017)
    db = client['air_quality_model_hkust']
    aqi_collection = db['weather_station']
    aqi_collection.create_index([('loc', pymongo.GEO2D)])
    client.close()

def frequency(collection_name = 'weather_model_hkust'):
    from pymongo import MongoClient
    import pymongo
    client = MongoClient("127.0.0.1", 27017)
    db = client['air_quality_model_hkust']
    aqi_collection = db[collection_name]
    index = 0
    old_time = 0
    new_time = 0
    for record in aqi_collection.find().sort('time', pymongo.ASCENDING):
        index += 1
        if index % 1000 == 0:
            print(index)
        new_time = record['time']
        if new_time != old_time:
            if old_time != 0:

                if new_time - old_time != 3600:
                    print('error')
            old_time = new_time
    client.close()

def spatial_temporal_fusion(time_gap = 3600, spatial_radius = 5000):
    from pymongo import MongoClient

    client = MongoClient("127.0.0.1", 27017)
    db = client['air_quality_model_hkust']
    aqi_collection = db['air_quality_model_hkust']

    aqi_station = get_stations_conf()
    weather_station = get_stations_conf('weather_station')

    for record in aqi_collection.find():
        print(record['station_code'], record['time'])
        break

if __name__ == "__main__":
    spatial_temporal_fusion()