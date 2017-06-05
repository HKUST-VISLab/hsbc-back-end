import os
import time

from pymongo import MongoClient

"""
Global config, to match the data to the database
"""
station_name_trans = {
    'Central Western': 'Central/Western'
}
AQI_trans = {
    'PM2.5': "PM2_5"
}
data_schema_trans = {
    'Time(UTC)': 'time',
    'Obs': 'obs'
}

HOST = '127.0.0.1'
PORT = 27017
DB = 'air_quality_model_hkust'
COLLECTION = 'air_quality_model_hkust'

class ModelProcessor:
    current_dir = os.path.dirname(os.path.abspath(__file__))
    config_folder = os.path.join(current_dir, '../config/air_station_hkust_config.json')
    data_folder = os.path.join(current_dir, '../../data/AQIModel')
    def __init__(self):
        self.__read_station_config()
        self.__init_db()

    def __init_db(self):
        self.client = MongoClient(HOST, PORT)
        self.db = self.client[DB]
        self.model_collection = self.db[COLLECTION]


    def __read_station_config(self):
        """
        Read hkust station config information from the config file, initialize the station name to station code mapping
        :return: nothing
        """
        import json
        self.name_to_code = {}
        with open(self.config_folder, 'r') as input:
            config_data = json.load(input)
            for station_dict in config_data:
                station_name = station_dict['station_name']
                station_name = station_name if station_name not in station_name_trans else station_name_trans[station_name]
                station_code = station_dict['station_code'] if station_dict['station_code'] != "N/A" else '_'.join(station_name.lower().split(' '))
                self.name_to_code[station_name] = station_code

    def __get_code_by_name(self, station_name):
        """
        Read the station name and return the station code
        :param station_name:
        :return: Flase if no config file is read; None if no this station name, code is the station name is found
        """
        if not self.name_to_code:
            return False
        if station_name not in self.name_to_code:
            return None
        else:
            return self.name_to_code[station_name]

    def __parse_filename(self, filename):
        """
        This function is designed ot read the filename and extract AQI, station name, and station code from the file name
        :param filename: the name of the file, not the path
        :return: and object with AQI, station_name, station_code
        """
        if not filename.endswith('.csv'):
            return None
        AQI_and_station = filename[: -4]
        AQI_and_station_segs = AQI_and_station.split('-')
        if len(AQI_and_station_segs) != 2:
            return None
        AQI = AQI_and_station_segs[0]
        AQI = AQI if AQI not in AQI_trans else AQI_trans[AQI]

        raw_stations = AQI_and_station_segs[1]
        station_segs = raw_stations.split('_')
        station_name = ' '.join(station_segs)
        station_name = station_name if station_name not in station_name_trans else station_name_trans[station_name]
        station_code = self.__get_code_by_name(station_name)
        if station_code == None:
            print('Station ', station_name ,' not found!')
            return None
        return {
            'AQI': AQI,
            'station_code': station_code,
            'station_name': station_name
        }

    def __parse_context_line(self, line):
        """
        Parse single line in the file, three different type, param will be discarded, schema and data will be saved for other purpose
        :param line: the single from the file
        :return: and dict include the type and parse result
        """

        line_segs = line.split(',')
        line_segs = [seg.strip()[1:-1] if seg.strip().startswith('"') and seg.strip().endswith('"') else seg.strip() for seg in line_segs]
        parse_result = []
        context_type = None
        if len(line_segs) == 2:
            context_type = 'param'

        elif len(line_segs) == 5:
            if line_segs[0] == 'Time(UTC)':
                context_type = 'schema'
            else:
                context_type = 'data'
        else:
            return None

        if context_type == 'schema':

            for schema in line_segs:
                schema = schema if schema not in data_schema_trans else data_schema_trans[schema]
                schema = schema.split(':')[0]
                parse_result.append(schema)
        elif context_type == 'data':
            parse_result = line_segs

        return {
            'type': context_type,
            'context': parse_result
        }


    def __generate_record_dict_from_list(self, segs):
        """
        Parser each record (list) and form a dict {time, obs, models}
        :param segs: segs
        :return: dict
        """
        parse_result = {}
        for index in range(len(self.current_schemas)):
            schema = self.current_schemas[index]
            value = segs[index]
            if schema == 'time':
                value = time.strptime(value, "%Y/%m/%d %H:%M:%S")
                value = time.strftime("%Y-%m-%d %H:%M:%S", value)
            value = None if value == '' else value
            parse_result[schema] = value
        return parse_result

    def __parser_single_file(self, filename):
        """
        Parse single file
        :param filename: the file name (not path)
        :return:
        """
        file_path = self.__get_file_path_by_name(filename)
        filename_dict = self.__parse_filename(filename)
        if filename_dict == None:
            return None
        AQI = filename_dict['AQI']
        station_name = filename_dict['station_name']
        station_code = filename_dict['station_code']
        num = 0
        with open(file_path, 'r') as input:
            line = input.readline()
            while line:
                result = self.__parse_context_line(line)
                parse_result = {}
                if not result:
                    line = input.readline()
                    continue
                if result['type'] == 'schema':
                    self.current_schemas = result['context']
                if result['type'] == 'data':
                    parse_result = self.__generate_record_dict_from_list(result['context'])
                    num += 1
                    self.__insert_one_record(AQI,station_code, parse_result)

                line = input.readline()
            print(num)

    def __get_file_path_by_name(self, filename):
        file_path = os.path.join(self.data_folder, filename)
        return file_path


    def __get_first_and_last_records(self, filename):
        """
        Read first and last record from the file, also get AQI and station information from the filename
        :param filename:
        :return:
        """
        file_path = self.__get_file_path_by_name(filename)
        first_record = None
        last_record = None
        previous_line = None

        filename_dict = self.__parse_filename(filename)
        if filename_dict == None:
            return None
        AQI = filename_dict['AQI']
        station_name = filename_dict['station_name']
        station_code = filename_dict['station_code']

        with open(file_path, 'r') as input:
            line = input.readline()
            while line:
                if not first_record:
                    r = self.__parse_context_line(line)
                    if r['type'] == 'schema':
                        self.current_schemas = r['context']
                        line = input.readline()
                        continue
                    if r['type'] != 'data':
                        line = input.readline()
                        continue
                    first_record = self.__generate_record_dict_from_list(r['context'])
                previous_line = line
                line = input.readline()

        last_record = self.__generate_record_dict_from_list(self.__parse_context_line(previous_line)['context'])
        return {'first_record': first_record, 'last_record': last_record, 'AQI': AQI, 'station_name': station_name, 'station_code': station_code}


    def __check_file_parsed_and_saved(self, filename):
        """
        Check if one file has been parsed and saved into db. Assume all the records are sorted by the time,
        only first record and last record are checked, if all of them are found in the database, we say this file has been parsed
        :param filename:
        :return:
        """
        file_info = self.__get_first_and_last_records(filename)
        first_record = file_info['first_record']
        last_record = file_info['last_record']

        AQI = file_info['AQI']
        station_code = file_info['station_code']

        if not self.__record_exist(AQI, station_code, first_record):
            return False
        if not self.__record_exist(AQI, station_code, last_record):
            return False
        return True

    def __insert_one_record(self, AQI, station_code, record_dict):
        """
        Insert one record. If record existed, update with new attributes, if not, create a new record
        Search key: station_code, time
        :param AQI:
        :param station_code:
        :param record_dict:
        :return:
        """
        search_key = {'station_code': station_code}
        update_context = {}
        for schema in self.current_schemas:
            if schema == 'time':
                search_key[schema] = record_dict[schema]
            else:
                key = str(AQI) + '.' + schema
                update_context[key] = record_dict[schema]

        self.model_collection.find_one_and_update(search_key,
                                                  {'$set': update_context},
                                                  upsert=True)

    def __record_exist(self, AQI, station_code, record_dict):
        """
        Detect if an record exists in the database, search key: station_code, AQI.models, time
        :param AQI:
        :param station_code:
        :param record_dict:
        :return: True if record existed, False not
        """
        search_key = {
            'station_code': {'$eq': station_code}
        }
        for schema in self.current_schemas:
            if schema == 'time':
                search_key[schema] = {'$eq': record_dict[schema]}
            else:
                key = str(AQI) + '.' + schema
                search_key[key] = {'$exists': True, '$nin': [None]}
        records = list(self.model_collection.find(search_key))
        return True if len(records) != 0 else False


    def parse_folder(self):
        """
        Parse all the csv files in the specific folder. If one file is detected parsed, if will be ignored
        :return:
        """
        filenames = [f for f in os.listdir(self.data_folder) if os.path.isfile(os.path.join(self.data_folder, f))]
        for filename in filenames:
            if self.__check_file_parsed_and_saved(filename) == True:
                print('The file ', filename, 'has been parsed!')
                continue
            print("Start parsing ", filename)
            self.__parser_single_file(filename)

if __name__ == '__main__':
    processor = ModelProcessor()
    processor.parse_folder()