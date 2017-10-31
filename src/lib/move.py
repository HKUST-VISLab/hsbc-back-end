from src.lib.library import *

def copy_collection(source_station = 'weather_stations_hkust', target_station = 'weather_station'):
    from pymongo import MongoClient
    client = MongoClient('127.0.0.1', 27017)
    source_collecton = client['hk_weather_data'][source_station]

    destination_collecton = client['air_quality_model_hkust'][target_station]
    destination_collecton.remove({})
    for record in source_collecton.find():
        del record['_id']
        if target_station == 'aqi_station':
            loc = record['loc'][::-1]
            record['loc'] = loc
        destination_collecton.insert(record)
    client.close()

def check_duplicated_station_code_in_station():
    from pymongo import MongoClient
    client = MongoClient('127.0.0.1', 27017)
    stations = client['air_quality_model_hkust']['aqi_station']

    for station in stations.find():
        print(station['station_code'])



def check_duplicated_station_code_in_aqi():
    from pymongo import MongoClient
    client = MongoClient('127.0.0.1', 27017)
    stations = client['air_quality_model_hkust']['air_quality_model_hkust']
    station_conf = get_stations_conf()


    station_map = {}
    for station in stations.find():
        station_code = station['station_code']
        if station_code not in station_map:
            station_map[station_code] = 0

        station_map[station_code] += 1

    for s in station_map:
        if s not in station_conf:
            print('Staion ', s, 'is not in the stations config')
        print(s, station_map[s])

def extend_model_collection():
    from pymongo import MongoClient
    station_conf = get_stations_conf()
    client = MongoClient('127.0.0.1', 27017)
    source_collection = client['air_quality_model_hkust']['air_quality_model_hkust']
    destination_collection = client['air_quality_model_hkust']['air_quality_model_hkust_enrich']

    records = []
    index = 0
    destination_collection.remove({})
    for record in source_collection.find():
        if index % 1000 == 0:
            print(index)
        code = record['station_code']
        station = station_conf[code]
        record['station_name'] = station['station_name']
        record['latitude'] = station['loc'][0]
        record['longitude'] = station['loc'][1]
        destination_collection.insert(record)
        index += 1

def extend_model_collection_e():
    from pymongo import MongoClient
    station_conf = get_stations_conf()
    client = MongoClient('127.0.0.1', 27017)
    source_collection = client['air_quality_model_hkust']['air_quality_model_hkust']
    destination_collection = client['air_quality_model_hkust']['air_quality_model_hkust_enrich_2']

    records = []
    index = 0
    destination_collection.remove({})
    for record in source_collection.find():
        if index % 1000 == 0:
            destination_collection.insert_many(records)
            records = []
            print(index)
        code = record['station_code']
        station = station_conf[code]
        record['station_name'] = station['station_name']
        record['latitude'] = station['loc'][0]
        record['longitude'] = station['loc'][1]
        destination_collection.insert(record)
        records.insert(record)
        index += 1
#
def check_weather_stations():
    station_config = get_stations_conf("weather_station")
    # print(station_config)
    from pymongo import MongoClient
    client = MongoClient('127.0.0.1', 27017)
    weather_collection = client['air_quality_model_hkust']['weather_model_hkust']
    index = 0
    number = 0
    for record in weather_collection.find():
        index += 1
        if index % 1000 == 0:
            print(index)
        station_code = record['station_code']
        if station_code not in station_config:
            number += 1
            print('station_Code', station_code)


if __name__ == "__main__":
    # station_conf = get_stations_conf('weather_station')
    # for r in station_conf:
    #     print(r)
    # check_duplicated_station_code_in_aqi()
    # copy_collection()
    # x = '113.98_x'
    # print(modify_station_code("113.8500_22.2500"))
    copy_collection('air_stations_hkust', 'aqi_station')