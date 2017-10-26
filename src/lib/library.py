from pymongo import MongoClient
import pymongo
import time

weather_schemas = ['cloud_cover', 'dew_point', 'precipitation_size', 'precipitation_hour', 'station_pressure', 'relative_humidity', 'temperature', 'visibility_synop', 'wind_speed', 'wind_direction', 'irradiance']
aqi_schemas = ['AQHI', 'AQI', 'CO', 'NO2', 'NOX', 'O3', 'PM10', 'PM2_5', 'SO2']

def create_index_2d(station_code = 'aqi_station'):
    # from pymongo import MongoClient
    # import pymongo
    client = MongoClient('127.0.0.1', 27017)
    collection = client['air_quality_model_hkust'][station_code]

    collection.create_index([("loc", pymongo.GEOSPHERE)])

def create_index(station_code = 'aqi_station'):
    from pymongo import MongoClient
    import pymongo
    client = MongoClient('127.0.0.1', 27017)
    collection = client['air_quality_model_hkust']['air_quality_model_hkust_enrich']

    collection.create_index('time')


def get_weather_schemas():
    # from pymongo import MongoClient
    # import pymongo
    client = MongoClient('127.0.0.1', 27017)
    collection = client['air_quality_model_hkust']['weather_model_hkust']
    schemas = {}
    num = 0
    for record in collection.find():
        num += 1
        if num % 1000 == 0:
            print(num)
        del record['_id']
        for schema in record:
            if schema not in schemas:
                schemas[schema] = 0
            schemas[schema] += 1
    return schemas

def get_AQI_schemas():
    # from pymongo import MongoClient
    # import pymongo
    client = MongoClient('127.0.0.1', 27017)
    collection = client['air_quality_model_hkust']['air_quality_model_hkust']
    schemas = {}
    num = 0
    for record in collection.find():
        num += 1
        if num % 1000 == 0:
            print(num)
        del record['_id']
        for schema in record:
            if schema not in schemas:
                schemas[schema] = 0
            schemas[schema] += 1
    return schemas

def modify_station_code(origin_code):
    """hack"""
    code_array = origin_code.split('_')
    if len(code_array) != 2:
        return origin_code
    [c1, c2] = code_array

    if (not isfloat(c1)) or (not isfloat(c2)):
        return origin_code

    modified_code = '{}_{}'.format(float(c1), float(c2))
    return modified_code

def get_stations_conf(station_name = 'aqi_station'):
    # from pymongo import MongoClient
    client = MongoClient('127.0.0.1', 27017)
    stations = client['air_quality_model_hkust'][station_name]

    station_map = {}
    for station in stations.find():
        station_code = station['station_code']
        m_station_code = modify_station_code(station_code)

        if m_station_code not in station_map:
            station_map[m_station_code] = station
    return station_map


def find_nearby_stations(lat, lon, distance):
    # from pymongo import MongoClient
    client = MongoClient('127.0.0.1', 27017)
    db = client['air_quality_model_hkust']
    aqi_station_collection = db['aqi_station']
    aqi_stations = []
    for r in aqi_station_collection.find({
        'loc': {
            '$near': {
                '$geometry': {
                    'type': "Point",
                    'coordinates': [lon, lat]
                },
                '$maxDistance': distance
            }
        }

    }):
        aqi_stations.append(r['station_code'])

    weather_station_collection = db['weather_station']
    weather_stations = []
    for r in weather_station_collection.find({
        'loc': {
            '$near': {
                '$geometry': {
                    'type': "Point",
                    'coordinates': [lon, lat]
                },
                '$maxDistance': distance
            }
        }

    }):
        weather_stations.append(r['station_code'])

    return {'AQI': aqi_stations, 'weather': weather_stations}
    pass


client = MongoClient('127.0.0.1', 27017)
db = client['air_quality_model_hkust']
weather_collection = db['weather_model_hkust']
aqi_collection = db['air_quality_model_hkust']

def find_weather_records(station_code, start_time, end_time):

    result = weather_collection.find({
        'station_code': station_code,
        'time': {
            '$gte': start_time,
            '$lte': end_time
        }
    })
    return list(result)

def find_AQI_records(station_code, start_time, end_time):
    # from pymongo import MongoClient
    number = 0
    result = aqi_collection.find({
        'station_code': station_code,
        'time': {
            '$gte': start_time,
            '$lte': end_time
        }
    })
    return list(result)



def aggregate_records(records, data_type = 'weather'):
    """
    data_type: AQI or Weather
    aggregation: {feature:{"sum":xx, "number"}}

    """
    all_schemas = weather_schemas if data_type == 'weather' else aqi_schemas
    schema_map = {}
    for schema in all_schemas:
        schema_map[schema] = {'sum': 0, 'num': 0}
    for record in records:
        for schema in record:
            if schema not in schema_map:
                continue

            value = record[schema]['obs'] if (type(record[schema]) == dict) else record[schema]

            if isfloat(value):

                schema_map[schema]['sum'] += float(value)
                schema_map[schema]['num'] += 1
            elif value != None:
                print("Error value")

    output_schema = {}
    for schema in schema_map:
        schema_obj = schema_map[schema]
        output_schema[schema] = schema_obj['sum'] / schema_obj['num'] if schema_obj['num'] != 0 else None

    return output_schema

def query_spatial_temporal_record(lat, lon, distance, start_time, end_time):
    station_obj = find_nearby_stations(lat = lat, lon = lon, distance = distance)
    AQI_stations = station_obj['AQI']
    weather_stations = station_obj['weather']

    weather_records = []
    for station in weather_stations:
        weather_records += find_weather_records(station_code=station, start_time = start_time, end_time = end_time)
    weather_aggregation = aggregate_records(weather_records, data_type='weather')

    AQI_records = []
    for station in AQI_stations:
        AQI_records += find_AQI_records(station_code=station, start_time=start_time, end_time=end_time)
    AQI_aggregation = aggregate_records(AQI_records, data_type='aqi')
    return {"AQI": AQI_aggregation, "weather": weather_aggregation}

def query_spatial_temporal_record_by_station_code(station_code, distance, start_time, end_time):
    station_config = get_stations_conf('aqi_station')
    lat = None
    lon = None
    station_type = None
    if station_code in station_config:
        [lon, lat] = station_config[station_code]['loc']
        station_type = 'AQI'

    station_config = get_stations_conf('weather_station')
    if station_code in station_config:
        [lon, lat] = station_config[station_code]['loc']
        station_type = 'weather'

    result = query_spatial_temporal_record(lat=lat, lon=lon, distance=distance, start_time=start_time, end_time=end_time)

    return result

def generate_aggregation_collection(time_range = 3600, distance = 5000):
    # from pymongo import MongoClient

    client = MongoClient('127.0.0.1', 27017)
    db = client['air_quality_model_hkust']
    aqi_collection = db['air_quality_model_hkust_enrich']
    output_collection = db['aqi_aggregation_hkust_5000_3600']

    output_collection.remove({})

    insert_cache = []
    process_number = 0
    start_time = time.time()
    for record in aqi_collection.find().sort('time'):
        AQI_station_code = record['station_code']
        current_time = record['time']
        # Time range +,- 1 hour; distance: 5000
        aggregation_result = query_spatial_temporal_record_by_station_code(AQI_station_code, 5000, current_time - 3600, current_time + 3600)

        process_number += 1
        if process_number % 100 == 0:
            print(process_number)

        del record['_id']
        new_record = {}
        for schema in record:
            if schema not in aqi_schemas:
                new_record[schema] = record[schema]
        new_record['station_type'] = 'AQI'
        new_record['aggregation_AQI'] = aggregation_result['AQI']
        new_record['aggregation_weather'] = aggregation_result['weather']
        new_record['station_record'] = record
        insert_cache.append(new_record)
        if len(insert_cache) == 100:
            output_collection.insert_many(insert_cache)
            insert_cache = []
            end_time = time.time()
            print(process_number, end_time - start_time)
            start_time = end_time

    output_collection.insert_many(insert_cache)


def isfloat(x):
    try:
        a = float(x)
    except ValueError:
        return False
    except TypeError:
        return False
    else:
        return True

if __name__ == '__main__':
    # create_index('weather_station')
    # station_obj = find_nearby_stations(lat=22.2845, lon=114.2169, distance=5000)
    # print(station_obj)

    # result = find_weather_records("NP_AWS", 1464710400.0, 1464717600.0)
    # print(result)
    # print('\n')
    # aggregate_weather_records(result)

    # result = find_weather_records("NP_AWS", 1464710400.0, 1464717600.0)
    # print(result)
    # print('\n')
    # result = aggregate_records(result)
    # print(result)

    # result = find_AQI_records("EN_A", 1464717600.0, 1464728600.0)
    # print(result)
    # print('\n')
    # result = aggregate_records(result, data_type='aqi')
    # print(result)

    # query_spatial_temporal_record(lat=22.2845, lon=114.2169, distance=5000, start_time = 1464721200.0, end_time = 1464725200)
    # import time
    # start_time = time.time()
    # for i in range(0, 100000000):
    #     pass
    # end_time = time.time()
    # print(end_time - start_time)
    generate_aggregation_collection()
    # create_index()
    # index = 0
    # insert_arr = []
    # output = []
    # for i in range(0, 48):
    #     insert_arr.append(i)
    #     if len(insert_arr) == 10:
    #         output += insert_arr
    #         insert_arr = []
    #
    # output += insert_arr
    # print([i for i in range(0, 48)])
    # print(output)
