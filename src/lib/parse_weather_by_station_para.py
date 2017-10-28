# from library import *
from src.lib.library import *
import os
import re

client = MongoClient('127.0.0.1', 27017)

db = client['air_quality_model_hkust']
sub_hour_weather_collection_name = 'subhour_weather_hkust'
sub_hour_weather_collection = db[sub_hour_weather_collection_name]

def init_collection():
    indexs = []
    if sub_hour_weather_collection_name not in db.collection_names():
        sub_hour_weather_collection.create_index('time')
        sub_hour_weather_collection.create_index([("loc", pymongo.GEOSPHERE)])
    else:
        for index_agg in sub_hour_weather_collection.index_information():
            indexs.append(index_agg.split("_")[0])
        if 'time' not in indexs:
            sub_hour_weather_collection.create_index('time')
        if 'loc' not in indexs:
            sub_hour_weather_collection.create_index([("loc", pymongo.GEOSPHERE)])

def read_all_files():
    file_list = []
    current_dir = os.path.dirname(os.path.abspath(__file__))
    # print('os.path.abspath(__file__): ', os.path.abspath(__file__))
    # print('current_dir: ', current_dir)
    data_folder = os.path.join(current_dir, '../../data/weather_station_param')

    dirs = [f for f in os.listdir(data_folder) if os.path.isdir(os.path.join(data_folder, f))]
    files = []
    for dir in dirs:
        dir_path = os.path.join(data_folder, dir)
        filename_objs = [{'filename': f, 'sub_dir': dir} for f in os.listdir(dir_path) if
                         os.path.isfile(os.path.join(dir_path, f))]
        files += filename_objs

    for file_obj in files:
        filename = file_obj['filename']
        sub_dir = file_obj['sub_dir']
        file_list.append(data_folder + '/' + sub_dir + '/' + filename)

    return file_list

def extract_para_and_station(file_path):
    """
    First three lines describe the fuke, fourth line describe schema, others for data
    :param file_path:
    :return:
    """

    with open(file_path) as input:
        # Read weather para, lon and lat from the first line
        line = input.readline()
        line = line.replace('"', '')
        line = re.sub("[\(\[].*?[\)\]]", "", line)
        segs = line.split(',')
        segs = [seg.strip() for seg in segs]
        weather = '_'.join(segs[0].lower().split(' '))
        lat = segs[1].split('=')[1]
        lon = segs[2].split('=')[1]

        # Read station from the second line
        line = input.readline()
        line = line.replace('"', '')
        segs = line.split(':')
        segs = [seg.strip() for seg in segs]
        station_code = segs[1]
        return {'weather': weather, 'lat': lat, 'lon': lon, 'station_code': station_code}

def parser_context_line(line, weather):
    segs = line.split(',')
    segs = [seg.strip() for seg in segs]
    parse_result = {}
    time_stamp = time.strptime(segs[0], "%Y/%m/%d %H:%M:%S")
    time_stamp = time.mktime(time_stamp)
    parse_result['time'] = time_stamp
    if weather != "wind":
        parse_result[weather] = segs[2]
    else:
        parse_result['wind_speed'] = segs[2]
        parse_result['wind_direction'] = segs[3]

    return parse_result



def parse_file_to_mongodb(file_path):
    print('Start parsing file', file_path)
    file_config = extract_para_and_station(file_path)
    search_key = {
        'loc': {'$eq': [float(file_config['lon']), float(file_config['lat'])]},
        'time': {'$eq': 'time'}}

    with open(file_path) as input:
        input.readline()
        input.readline()
        input.readline()
        input.readline()
        line = input.readline()
        num = 0
        while line:
            num += 1
            if num % 1000 == 0:
                print(num, 'has been parsed!')
            parse_result = parser_context_line(input.readline(), file_config['weather'])
            search_key['time']['$eq'] = parse_result['time']

            del parse_result['time']

            sub_hour_weather_collection.find_one_and_update(search_key,
                                                            {'$set': parse_result},
                                                            upsert=True)
            line = input.readline()

def generate_panda_dataframe(file_path):

    file_config = extract_para_and_station(file_path)
    search_key = {
        'loc': {'$eq': [float(file_config['lon']), float(file_config['lat'])]},
        'time': {'$eq': 'time'}}
    print(search_key)

    # with open(file_path) as input:
    #     input.readline()
    #     input.readline()
    #     input.readline()
    #     input.readline()
    #     line = input.readline()
    #     num = 0
    #     while line:
    #         num += 1
    #         if num % 1000 == 0:
    #             print(num, 'has been parsed!')
    #         parse_result = parser_context_line(input.readline(), file_config['weather'])
    #         search_key['time']['$eq'] = parse_result['time']
    #
    #         del parse_result['time']
    #
    #         sub_hour_weather_collection.find_one_and_update(search_key,
    #                                                         {'$set': parse_result},
    #                                                         upsert=True)
    #         line = input.readline()



def parse_folders():
    files = read_all_files()
    for file in files:
        # parse_file_to_mongodb(file)
        generate_panda_dataframe(file)

if __name__ == "__main__":
    # pares_folders()
    init_collection()
    parse_folders()
    pass