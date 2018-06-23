import os
import time
from pymongo import MongoClient


class TSMLocalFetcher:
    """
    This is a class used to fetch data from the local hk gov data.
    The gov data can be found: data/tsm2016/JTIS and data/tsm2016/SMP
    """
    current_path = os.path.dirname(os.path.abspath(__file__))
    relative_jtis_path = '../../../../data/tsm2016/JTIS/'
    relative_smp_path = '../../../../data/tsm2016/SMP/'
    jtis_folder_path = os.path.join(current_path, relative_jtis_path)
    smp_folder_path = os.path.join(current_path, relative_smp_path)

    def __init__(self):
        pass

    def find_link_info(self, road_id_list):
        """
        Query the region and road_type of the roads from the database
        :param road_id_list: list of records to be stored
        :return: A map with all the road
        """
        client = MongoClient('127.0.0.1', 27017)
        db = client['traffic']
        collection = db['tsm_link_info']
        all_road_info = {}
        for r_id in road_id_list:
            record = collection.find_one({'link_id': r_id})
            if record:
                road_info = {r_id: {'region': record['road_region'], 'road_type': record['road_type']}}
                all_road_info.update(road_info)
            else:
                road_info = {r_id: {'region': 'NULL', 'road_type': 'NULL'}}
            all_road_info.update(road_info)
        client.close()
        return all_road_info

    def store_tsm_data(self, records):
        """
        Store the records into the database
        :param records: list of records to be stored
        """
        client = MongoClient('127.0.0.1', 27017)
        db = client['traffic']
        collection = db['traffic_speed_map']
        collection.insert_many(records)
        client.close()

    def process_all_csv(self):
        """
        Process all csv data
        """
        for root, dirnames, filenames in os.walk(self.jtis_folder_path):
            for filename in filenames:
                self.process_csv(os.path.join(root, filename))

        for root, dirnames, filenames in os.walk(self.smp_folder_path):
            for filename in filenames:
                self.process_csv(os.path.join(root, filename))

    def process_csv(self, csv_path):
        """
        Process single csv and store into databse
        :param csv_path: path string of the csv file
        """
        current_date = time.strptime(csv_path[-12:-4], '%Y%m%d')
        seconds_of_current_date = time.mktime(current_date)
        date_format = '%Y-%m-%d %H:%M:%S'
        try:
            with open(csv_path) as file_in:
                print('Processing: ' + csv_path)
                line_list = file_in.readlines()
                col_items = line_list[0].split(',')[2:]  # Skip 'Date' and 'Time'
                roads_list = []
                for item_name in col_items:
                    roads_list.append(item_name.split(' ')[1])
                road_id_list = list(set(roads_list))
                road_id_list.sort(key=roads_list.index)
                all_road_info = self.find_link_info(road_id_list)
                record_list = []
                current_time = time.strftime(date_format, time.localtime())
                seconds_current_time = float(time.mktime(time.strptime(current_time, date_format)))
                search_start_index = 0
                for i in range(0, 49):  # [0, 48]
                    # Search data for every 30 minutes
                    seconds_of_each_interval = seconds_of_current_date + i * 30 * 60
                    if i == 48:
                        # 23:59
                        seconds_of_each_interval = seconds_of_current_date + (23 * 60 + 59) * 60
                    # print(time.strftime(date_format, time.localtime(seconds_of_each_interval)))
                    smallest_gap = 30 * 60
                    closest_line = ""
                    closest_seconds_time = 0.0
                    for index, line in enumerate(line_list[1:]):
                        if index >= search_start_index:
                            date_time_string = csv_path[-12:-4] + ' ' + line.split(',')[1]
                            seconds_of_line_time = time.mktime(time.strptime(date_time_string, '%Y%m%d %H:%M:%S'))
                            # print(seconds_of_line_time)
                            gap = abs(seconds_of_line_time - seconds_of_each_interval)
                            if gap < smallest_gap:
                                smallest_gap = gap
                                search_start_index = index
                                closest_line = line
                                closest_seconds_time = seconds_of_line_time
                    # Store into database
                    if len(closest_line):
                        line_columns = closest_line.split(',')
                        column_list = line_columns[2:]
                        for r_index in range(len(road_id_list)):
                            record = {'link_id': road_id_list[r_index],
                                      'region': all_road_info[road_id_list[r_index]]['region'],
                                      'road_type': all_road_info[road_id_list[r_index]]['road_type'],
                                      'traffic_speed': str(round(float(column_list[r_index * 3]))),
                                      'capture_date': ' '.join(closest_line.split(',')[:2]),
                                      'fetch_time': current_time,
                                      'capture_date_1970': closest_seconds_time,
                                      'fetch_time_1970': seconds_current_time,
                                      'travel_mins': column_list[r_index * 3 + 2].rstrip()}
                            if column_list[r_index * 3 + 1] == 'G':
                                record['road_saturation_level'] = 'TRAFFIC GOOD'
                            elif column_list[r_index * 3 + 1] == 'R':
                                record['road_saturation_level'] = 'TRAFFIC BAD'
                            else:
                                record['road_saturation_level'] = 'TRAFFIC AVERAGE'
                            record_list.append(record)
                self.store_tsm_data(record_list)
        except IOError as err:
            print('File error: ' + str(err))


if __name__ == '__main__':
    tsm_local = TSMLocalFetcher()
    tsm_local.process_all_csv()
