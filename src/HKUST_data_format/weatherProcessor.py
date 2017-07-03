import os
import time
import json

from pymongo import MongoClient
from pymongo import GEOSPHERE
from pymongo import ASCENDING

"""
Global config, to match the data to the database
"""
data_schema_trans = {
    'Time(UTC)': 'time',
    'Obs': 'obs'
}

attr_trans = {
    'Relative_Humidity': 'relative_humidity',
    'Temperature': 'temperature',
    'Wind': 'wind',
    'Cloud_Cover': 'cloud_cover',
    'Dew_Point': 'dew_point',
    'Irradiance': 'irradiance',
    'Station_Pressure': 'station_pressure',
    'Precipitation': 'precipitation',
    'Visibility_-_SYNOP': 'visibility_synop'
}

attr_unit_trans = {
    'wind': {'m/s': 'wind_speed',
             'Degree': 'wind_direction'},
    'relative_humidity': {'%': 'relative_humidity'},
    'temperature': {'Degree Celsius': 'temperature'},
    'cloud_cover': {'None': 'cloud_cover'},
    'dew_point': {'Degree Celsius': 'dew_point'},
    'irradiance': {'w/m2': 'irradiance'},
    'station_pressure': {'Pascal': 'station_pressure'},
    'precipitation': {'mm': 'precipitation_size',
                      'Hour': 'precipitation_hour'},
    'visibility_synop': {'m': 'visibility_synop'}

}
station_code_trans = {
    'N/A': 'N/A'
}

HOST = '127.0.0.1'
PORT = 27017
DB = 'air_quality_model_hkust'
COLLECTION = 'weather_model_hkust'


class ModelProcessor:
    current_dir = os.path.dirname(os.path.abspath(__file__))
    print('os.path.abspath(__file__): ', os.path.abspath(__file__))
    print('current_dir: ', current_dir)
    data_folder = os.path.join(current_dir, '../../data/weather')
    print('data_folder: ', data_folder)

    def __init__(self):
        self.weather_attr = ''
        self.unit_flag = False
        self.unit = ''
        self.data_flag = False
        self.current_station_num = 0
        self.current_station_list = []
        self.current_latitude_list = []
        self.current_longitude_list = []

        self.first_data_flag = True
        self.__init_db()
        return

    def __init_db(self):
        self.client = MongoClient(HOST, PORT)
        self.db = self.client[DB]
        self.model_collection = self.db[COLLECTION]
        self.model_collection.create_index([('loc', GEOSPHERE)])
        self.model_collection.create_index([('time', ASCENDING)])
        return

    def __get_weather_attribute(self, line_segs):
        """
        get weather attribute
        :param line_segs: "Parameter: Wind [a_wind]"
        :return:
        """
        line_param = line_segs[0].split(' ')
        attr = '_'.join(line_param[1:len(line_param) - 1])
        self.weather_attr = attr if attr not in attr_trans else attr_trans[attr]
        # print('weather_attr: ', self.weather_attr)
        return

    def __get_station_num(self, line_segs):
        """
        get station number
        :param line_segs: "No. of Stations: 63"
        :return:
        """
        try:
            self.current_station_num = int(line_segs[0].split(' ')[-1])
        except ValueError:
            print('error in No. of stations')
            return
        # print('current_station_num: ', self.current_station_num)
        return

    def __get_unit(self, line_segs):
        """
        get the unit of weather attr
        :param line_segs:
        :return:
        """
        if self.weather_attr in attr_unit_trans and line_segs[0] in attr_unit_trans[self.weather_attr]:
            self.unit = attr_unit_trans[self.weather_attr][line_segs[0]]
        else:
            self.unit = line_segs[0]
        # print('unit: ', self.unit)
        self.unit_flag = False
        return

    def __get_station_list(self, line_segs):
        """
        get station id list
        :param line_segs:
        :return:
        """
        # self.current_station_list = [seg if not (seg in station_code_trans) else station_code_trans[seg] for seg in
        #                              line_segs[1:]]
        self.current_station_list = line_segs[1:]
        # print('current_station_list: ', self.current_station_list)
        return

    def __update_station_list(self):
        """
        update station list,  N/A -> latitude_longitude
        :return:
        """
        for idx in range(self.current_station_num):
            if self.current_station_list[idx] == 'N/A':
                self.current_station_list[idx] = '{}_{}'.format(float(self.current_longitude_list[idx]), float(self.current_latitude_list[idx]))
        return

    def __get_latitude_list(self, line_segs):
        """
        get latitude list
        :param line_segs:
        :return:
        """
        # self.current_latitude_list = [float(seg) for seg in line_segs[1:]]
        self.current_latitude_list = line_segs[1:]
        # print('current_latitude_list: ', self.current_latitude_list)
        return

    def __get_longitude_list(self, line_segs):
        """
        get longitude list
        :param line_segs:
        :return:
        """
        # self.current_longitude_list = [float(seg) for seg in line_segs[1:]]
        self.current_longitude_list = line_segs[1:]
        # update station list
        self.__update_station_list()
        # print('current_longitude_list: ', self.current_longitude_list)
        return

    def __parse_context_line(self, line):
        """
        Parse single line in the file, different types,
            parameter(attribute), No. of stations
            station ID, (latitude, longitude)
            data
        :param line: the single from the file
        :return: and dict include the type and parse result
        """
        line_segs = line.strip().split(',')
        line_segs = [seg.strip()[1:-1] if seg.strip().startswith('"') and seg.strip().endswith('"') else seg.strip() for
                     seg in line_segs]

        if 'Parameter' == line_segs[0][:len('Parameter')]:
            self.__get_weather_attribute(line_segs)
            return None

        if 'No. of Stations' == line_segs[0][:len('No. of Stations')]:
            self.__get_station_num(line_segs)
            return None

        if self.unit_flag:
            self.__get_unit(line_segs)
            return None

        if '------------------------------' == line_segs[0][:len('------------------------------')]:
            self.unit_flag = True
            self.data_flag = False
            return None

        if 'Station ID' == line_segs[0]:
            self.__get_station_list(line_segs)
            return None

        if 'Latitude' == line_segs[0]:
            self.__get_latitude_list(line_segs)
            return None

        if 'Longitude' == line_segs[0]:
            self.__get_longitude_list(line_segs)
            return None

        if 'Time' == line_segs[0]:
            self.data_flag = True
            self.first_data_flag = True
            return None

        if self.data_flag:
            assert self.current_station_num + 1 == len(line_segs)
            time_stamp = line_segs[0]
            time_stamp = time.strptime(time_stamp, "%Y/%m/%d %H:%M:%S")
            time_stamp = time.strftime("%Y-%m-%d %H:%M:%S", time_stamp)
            try:
                context = [float(seg) if float(seg) != -99999 else None for seg in line_segs[1:]]
            except ValueError:
                print('error in line context, float()')
                return
            parse_result = {'time': time_stamp, 'context': context}
            return parse_result
        return None

    def __parser_single_file(self, filename):
        """
        Parse single file
        :param filename: the file name (not path)
        :return:
        """
        # print('filename: ', filename)
        if not filename.endswith('.csv'):
            return None

        file_path = self.__get_file_path_by_name(filename)
        num = 0
        with open(file_path, 'r') as input:
            line = input.readline()
            while line:
                parse_result = self.__parse_context_line(line)
                if parse_result:
                    self.__insert_one_record(parse_result)
                line = input.readline()
                num += 1
        # after go through whole file, reset self.data_flag to False
        self.data_flag = False
        return

    def __get_file_path_by_name(self, filename):
        file_path = os.path.join(self.data_folder, filename)
        return file_path

    def __insert_one_record(self, parse_result):
        """
        Insert one record. If record existed, update with new attributes, if not, create a new record
        Search key: latitude, longitude, time
        update: self.unit
        :param parse_result:
        :return:
        """
        for idx in range(self.current_station_num):
            search_key = {'loc': [float(self.current_longitude_list[idx]), float(self.current_latitude_list[idx])],
                          'time': parse_result['time']}
            update_context = {self.unit: parse_result['context'][idx],
                              'station_code': self.current_station_list[idx]}

            self.model_collection.find_one_and_update(search_key,
                                                      {'$set': update_context},
                                                      upsert=True)
        return

    def __get_first_and_last_record_list(self, filename):
        """
        Read first and last record from each unit, get a list of first_record and last record
        :param filename:
        :return:
        """
        file_path = self.__get_file_path_by_name(filename)
        first_record_list = []
        last_record_list = []
        previous_line = None
        with open(file_path, 'r') as input:
            line = input.readline()
            while line:
                parse_result = self.__parse_context_line(line)
                if parse_result:
                    if self.first_data_flag:
                        first_record_list.append(parse_result)
                        self.first_data_flag = False
                    previous_line = parse_result
                if (not parse_result) and self.unit_flag and previous_line:
                    last_record_list.append(previous_line)
                line = input.readline()
            last_record_list.append(previous_line)

        # after go through whole file, reset self.data_flag to False
        self.data_flag = False
        return {'first_record': first_record_list,
                'last_record': last_record_list}

    def __check_file_parsed_and_saved(self, filename):
        """
        Check if one file has been parsed and saved into db. Assume all the records are sorted by the time,
        only first record and last record are checked, if all of them are found in the database, we say this file has been parsed
        :param filename:
        :return:
        """
        file_info = self.__get_first_and_last_record_list(filename)
        first_record_list = file_info['first_record']
        last_record_list = file_info['last_record']
        assert len(first_record_list) == len(last_record_list)

        for idx in range(len(first_record_list)):
            if not self.__record_exist(first_record_list[idx]):
                return False
            if not self.__record_exist(last_record_list[idx]):
                return False
        return True

    def __record_exist(self, parse_result):
        """
        Detect if an record exists in the database
        Search key: latitude, longitude, time
        update: self.unit
        :param parse_result:  {'time':'2017-05-01 00:00:00', 'context': []}
        :return: True if record existed, False not
        """
        for idx in range(self.current_station_num):
            search_key = {
                'loc': {'$eq': [float(self.current_longitude_list[idx]), float(self.current_latitude_list[idx])]},
                'time': {'$eq': parse_result['time']},
                self.unit: {'$exists': True, '$nin': [None]},
                'station_code': {'$exists': True, '$nin': [None]}
            }
            records = list(self.model_collection.find(search_key))
            if len(records) == 0:
                return False
        return True

    def parse_folder(self):
        """
        Parse all the csv files in the specific folder. If one file is detected parsed, if will be ignored
        :return:
        """
        filenames = [f for f in os.listdir(self.data_folder) if os.path.isfile(os.path.join(self.data_folder, f))]
        for filename in filenames:
            if self.__check_file_parsed_and_saved(filename):
                print('The file ', filename, 'has been parsed!')
                continue
            print("Start parsing ", filename)
            self.__parser_single_file(filename)

    def generate_config(self):
        """
        Based on the csv files in the specific folder, generate configure file.
        :return:
        """
        station_list = []
        latitude_list = []
        longitude_list = []

        filenames = [f for f in os.listdir(self.data_folder) if os.path.isfile(os.path.join(self.data_folder, f))]
        for filename in filenames:
            file_path = self.__get_file_path_by_name(filename)
            with open(file_path, 'r') as input:
                line = input.readline()
                while line:
                    line_segs = line.strip().split(',')
                    line_segs = [
                        seg.strip()[1:-1] if seg.strip().startswith('"') and seg.strip().endswith('"') else seg.strip()
                        for seg in line_segs]
                    if 'Station ID' == line_segs[0]:
                        station_list.append(line_segs[1:])
                    if 'Latitude' == line_segs[0]:
                        latitude_list.append(line_segs[1:])
                    if 'Longitude' == line_segs[0]:
                        longitude_list.append(line_segs[1:])
                    line = input.readline()

        weather_config_tmp = {}
        weather_config = []
        for attr_idx in range(len(station_list)):
            for station_idx in range(len(station_list[attr_idx])):
                lon_lat = '{}_{}'.format(longitude_list[attr_idx][station_idx], latitude_list[attr_idx][station_idx])
                station_code = station_list[attr_idx][station_idx]
                if lon_lat not in weather_config_tmp:
                    weather_config_tmp[lon_lat] = []
                    weather_config_tmp[lon_lat].append(station_code)
        # print('weather_config: ', weather_config_tmp)
        # print('len(weather_config): ', len(weather_config_tmp))
        for lon_lat in weather_config_tmp:
            station_code_set = set(weather_config_tmp[lon_lat])
            assert len(station_code_set) == 1
            station_code = list(station_code_set)[0]
            if station_code == 'N/A':
                station_code = lon_lat
            lon_lat_seg = lon_lat.split('_')
            weather_config.append({'loc': [float(lon_lat_seg[0]), float(lon_lat_seg[1])], 'station_code': station_code})

        weather_config_path = os.path.join(self.current_dir, '../config/weather_hkust_config.json')

        with open(weather_config_path, 'w') as inputfile:
            json.dump(weather_config, inputfile)
        return


if __name__ == '__main__':
    processor = ModelProcessor()
    # processor.parser_single_file('A_WIND-20170501-20170505.csv')
    processor.parse_folder()
    # processor.generate_config()
    # processor.parser_single_file('A_WIND-20160601-20170531.csv')