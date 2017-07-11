from pymongo import MongoClient
import json
sub_attrs = ['obs', 'CAMx', 'CMAQ', 'NAQPMS'];

attrs_aqi = [
    {'attr': 'station_code', 'sub_attr': []},
    {'attr': 'time', 'sub_attr': []},
    {'attr': 'AQHI', 'sub_attr': sub_attrs},
    {'attr': 'AQI', 'sub_attr': sub_attrs},
    {'attr': 'CO', 'sub_attr': sub_attrs},
    {'attr': 'NO2', 'sub_attr': sub_attrs},
    {'attr': 'NOX', 'sub_attr': sub_attrs},
    {'attr': 'O3', 'sub_attr': sub_attrs},
    {'attr': 'PM10', 'sub_attr': sub_attrs},
    {'attr': 'PM2_5', 'sub_attr': sub_attrs},
    {'attr': 'SO2', 'sub_attr': sub_attrs}
]

attrs_weather = [
    'station_code',
    'time',
    'temperature',
    'cloud_cover',
    'dew_point',
    'precipitation_size',
    'precipitation_hour',
    'relative_humidity',
    'visibility_synop',
    'wind_speed',
    'wind_direction',
    'station_pressure',
    'irradiance'
]


def get_attrs(attrs):
    arr = []
    for attrObj in attrs:
        if len(attrObj['sub_attr']) == 0:
            arr.append(attrObj['attr'])
        else:
            sub_attr = attrObj['sub_attr']
            for _attr in sub_attr:
                arr.append(attrObj['attr']+'.'+_attr)
    return arr

def parser_aqi_record(attrs, record):
    arr = []
    for attr_dict in attrs:
        _attr = attr_dict['attr']
        _sub_attrs = attr_dict['sub_attr']
        if len(_sub_attrs) == 0:
            v = record[_attr] if (_attr in record) else None
            arr.append(v)
        else:
            v = [record[_attr][_sub_attr] for _sub_attr in _sub_attrs] if (_attr in record) else [None, None , None]
            arr += v
    return ','.join([str(d) for d in arr])


def output_aqi(aqi_output_path):
    client = MongoClient('127.0.0.1', 27017)
    aqi_weather_db = client['air_quality_model_hkust']
    station_db = client['hk_weather_data']
    aqi_station_c = station_db['air_stations_hkust']
    weather_c = aqi_weather_db['weather_model_hkust']
    aqi_c = aqi_weather_db['air_quality_model_hkust']

    attrs_string = get_attrs(attrs_aqi)
    head = ','.join(attrs_string)
    with open(aqi_output_path, 'w') as output:
        output.write(head + '\n')
        for record in aqi_c.find().sort('time'):
            parse_results = parser_aqi_record(attrs_aqi, record)
            output.write(parse_results + '\n')

    print('done')
def parser_weather_record(attrs, record):
    arr = []
    for attr in attrs:
        v = record[attr] if attr in record else None
        arr.append(v)
    return ",".join([str(d) for d in arr])



def output_weather(weather_output_path):
    client = MongoClient('127.0.0.1', 27017)
    aqi_weather_db = client['air_quality_model_hkust']
    weather_c = aqi_weather_db['weather_model_hkust']

    attrs_string = attrs_weather
    head = ",".join(attrs_string)
    with open(weather_output_path, 'w') as output:
        output.write(head + '\n')
        for record in weather_c.find().sort('time'):
            parse_results = parser_weather_record(attrs_weather, record)
            output.write(parse_results + '\n')


def output_aqi_stations(aqi_station_path):
    client = MongoClient('127.0.0.1', 27017)
    station_db = client['hk_weather_data']
    aqi_station_c = station_db['weather_stations_hkust']
    arr = []
    for station in aqi_station_c.find():
        del station['_id']
        arr.append(station)
    with open(aqi_station_path, 'w') as output:
        json.dump(arr, output)


if __name__ == "__main__":
    output_aqi_stations('weather_stations.json')
