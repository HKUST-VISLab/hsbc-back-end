from urllib.error import HTTPError, URLError
import urllib
import urllib.request
from pymongo import MongoClient
from lxml import etree
from lxml.etree import XMLSyntaxError
import time
import json
import os

#  Modify: save the parsed data as local files
#          log the schedual
#          Realtime updating
#  Recent record: latest: http://resource.data.one.gov.hk/td/speedmap.xml
#  Historical record: https://api.data.gov.hk/v1/historical-archive/get-file?url=http%3A%2F%2Fresource.data.one.gov.hk%2Ftd%2Fspeedmap.xml&time=20170901-0049

REQUEST_PATH = 'http://resource.data.one.gov.hk/td/speedmap.xml'
TRAFFIC_SPEED_COLLECTION = "traffic_speed_map"

tag_map = {
    "LINK_ID": 'id',
}


class TSMFetcher:
    """This is a class used to fetch data from the hk gov data.
    default url = http://resource.data.one.gov.hk/td/speedmap.xml
    The gov data page can be found: https://data.gov.hk/en-data/dataset/hk-td-tis-traffic-speed-map
    """

    tsm_path = REQUEST_PATH
    entity_tag = "jtis_speedmap"
    current_path = os.path.dirname(os.path.abspath(__file__))

    def __init__(self):
        pass

    def fetch_TSM_data(self, path=REQUEST_PATH):
        try:
            response = urllib.request.urlopen(path)
        except HTTPError as e:
            data = str(e.code)
            print('HTTPError = ' + data + '. Air Quality AQExtractor!')
        except URLError as e:
            data = str(e.reason)
            print('URLError = ' + data + '. Air Quality AQExtractor!')
        else:
            self.page = response.read()
            return self.parse_self_xml()

    def parse_self_xml(self):
        current_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        parser = etree.XMLPullParser(events=('start',))
        records = []

        def parse_tag(raw_tag):
            return raw_tag.split('}')[1]

        try:
            parser.feed(self.page)
        except XMLSyntaxError as err:
            print('XMLSyntaxError: ' + str(err))
        else:
            for action, element in parser.read_events():
                if parse_tag(element.tag) != self.entity_tag:
                    continue
                segs = [{parse_tag(t.tag.lower()): t.text} for t in element]
                record = {}
                for seg in segs:
                    record.update(seg)

                # Check if a valid record
                if 'link_id' and 'region' and 'road_type' and 'road_saturation_level' \
                        and 'traffic_speed' and 'capture_date' in record:
                    record['fetch_time'] = current_time

                    # The time is record['CAPTURE_DATE'], to be revised
                    r_time = time.strptime(record['CAPTURE_DATE'.lower()], "%Y-%m-%dT%H:%M:%S")
                    record['CAPTURE_DATE'.lower()] = time.strftime("%Y-%m-%d %H:%M:%S", r_time)

                    # Store the seconds rom 1970
                    capture_date_1970 = float(time.mktime(r_time))
                    current_time_1970 = float(time.mktime(time.strptime(current_time, "%Y-%m-%d %H:%M:%S")))
                    record['capture_date_1970'] = capture_date_1970
                    record['fetch_time_1970'] = current_time_1970

                    records.append(record)
                else:
                    print("invalid record")
        finally:
                return records

    def fetch_TSM_save_links_file(self, start_date='20171001', relative_path='../../data/tsm_link/'):
        """
        Save the xml links to local files for everyday from start date
        :param start_date: start date in 'yyyymmdd' format
        :param relative_path: relative data path of the current file
        :return: list of new files' names
        """

        date_format = "%Y%m%d"
        current_time = time.strftime(date_format, time.localtime())
        start = int(time.mktime(time.strptime(start_date, date_format)))
        end = int(time.mktime(time.strptime(current_time, date_format)))
        # End with yesterday
        date_list = [time.strftime(date_format, time.localtime(i)) for i in range(start, end, 3600 * 24)]

        # Build folder for data storage
        link_folder_path = os.path.join(self.current_path, relative_path)
        if not os.path.exists(link_folder_path):
            os.makedirs(link_folder_path)
        os.chdir(link_folder_path)

        # Timestamps of everyday
        api_list = []
        for date_string in date_list:
            api_string = 'https://api.data.gov.hk/v1/historical-archive/list-file-versions' \
                         '?url=http://resource.data.one.gov.hk/td/speedmap.xml' \
                         '&start=' + date_string + '&end=' + date_string
            api_list.append(api_string)

        # List of new files
        new_date_list = []
        # XML links of everyday
        for index, date_string in enumerate(date_list):
            xml_list = []
            try:
                response = urllib.request.urlopen(api_list[index])
            except HTTPError as e:
                data = str(e.code)
                print('HTTPError = ' + data + '. Fetch TSM link data error!')
            except URLError as e:
                data = str(e.reason)
                print('URLError = ' + data + '. Fetch TSM link data error!')
            else:
                list_file_json = json.loads(response.read())
                for timestamp in list_file_json['timestamps']:
                    xml_string = 'https://api.data.gov.hk/v1/historical-archive/get-file' \
                                 '?url=http%3A%2F%2Fresource.data.one.gov.hk%2Ftd%2Fspeedmap.xml' \
                                 '&time=' + timestamp
                    xml_list.append(xml_string)

            # Save xml links file for everyday
            if not os.path.exists(date_string):
                print('Saving xml links of ' + date_string + ' ...')
                new_date_list.append(date_string)
                try:
                    with open(date_string, 'w') as file_out:
                        separate = '\n'
                        file_string = separate.join(xml_list)
                        file_out.write(file_string)
                except IOError as err:
                    print('File error: ' + str(err))
            else:
                print('File: ' + date_string + ' already exists.')

        return new_date_list

    def fetch_all_TSM_xml_from_link_file(self, file_list, save_xml=False, store_database=False):
        """
        Collect ALL historical xml records from the links in local file.
        Default setting of saving the xml files is FALSE.
        Default setting of storing into the database is FALSE.
        :param file_list: list of date files to be processed,
        :param save_xml: if save each xml to local file
        :param store_database: if parse and store in database
        :return:
        """

        # current_path: ~/workspace/hsbc-back-end/src/tsm_fetcher/tsm_fetcher_helper.py
        xml_folder_path = os.path.join(self.current_path, '../../../../data/full_tsm_xml/')
        link_folder_path = os.path.join(self.current_path, '../../../../data/full_tsm_link/')

        # Read link files in link folder
        os.chdir(link_folder_path)
        for date_file in file_list:
            # date_file example: 20171001
            try:
                with open(date_file) as file_in:
                    link_list = file_in.readlines()
                    for link in link_list:
                        date_time_string = link[-14:-1]
                        if link == link_list[-1]:
                            # Without '\n'
                            date_time_string = link[-13:]

                        # xml filename example: 20171001_0000.xml
                        xml_filename = date_time_string.replace('-', '_') + ".xml"
                        try:
                            response = urllib.request.urlopen(link)
                        except HTTPError as e:
                            data = str(e.code)
                            print('HTTPError = ' + data + '. Fetch TSM xml data error!')
                        except URLError as e:
                            data = str(e.reason)
                            print('URLError = ' + data + '. Fetch TSM xml data error!')
                        else:
                            self.page = response.read()
                            if save_xml:
                                xml_date_folder = os.path.join(xml_folder_path, date_file + '/')
                                if not os.path.exists(xml_date_folder):
                                    os.makedirs(xml_date_folder)
                                try:
                                    with open(xml_date_folder + xml_filename, 'wb') as xml_file_out:
                                        xml_file_out.write(self.page)
                                        print('Saving: ' + xml_date_folder + xml_filename + ' successfully.')
                                except IOError as err:
                                    print('File error: ' + str(err))
                            if store_database:
                                # Store in database
                                xml_record = self.parse_self_xml()
                                if len(xml_record):
                                    # valid record
                                    self.store_TSM_data(xml_record)
                                    print('Parsing: ' + xml_filename + ' and storing in database successfully')
            except IOError as err:
                print('File error: ' + str(err))

    def fetch_TSM_xml_from_link_file(self, file_list, save_xml=False, store_database=False):
        """
        Collect historical xml records with 30-min interval from the links in local file.
        Default setting of saving the xml files is FALSE.
        Default setting of storing into the database is FALSE.
        :param file_list: list of date files to be processed,
        :param save_xml: if save each xml to local file
        :param store_database: if parse and store in database
        :return:
        """

        xml_folder_path = os.path.join(self.current_path, '../../data/tsm_xml/')
        link_folder_path = os.path.join(self.current_path, '../../data/tsm_link/')

        # Read link files in link folder
        os.chdir(link_folder_path)
        for date_file in file_list:
            # date_file example: 20171001
            current_date_time = time.strptime(date_file, "%Y%m%d")
            seconds_of_current_date = time.mktime(current_date_time)
            # print(seconds_of_current_date)
            date_format = "%Y%m%d-%H%M"
            try:
                with open(date_file) as file_in:
                    link_list = file_in.readlines()
                    search_start_index = 0
                    for i in range(0, 49):
                        # Search data for every 30 minutes
                        seconds_of_each_interval = seconds_of_current_date + i * 30 * 60
                        if i == 48:
                            # 23:59
                            seconds_of_each_interval = seconds_of_current_date + (23 * 60 + 59) * 60
                        # print(time.strftime(date_format, time.localtime(seconds_of_each_interval + i * 30 * 60)))
                        smallest_gap = 30 * 60
                        closest_link = ""
                        closest_date_time_string = ""
                        for index, link in enumerate(link_list):
                            if index >= search_start_index:
                                date_time_string = link[-14:-1]
                                if link == link_list[-1]:
                                    # Without '\n'
                                    date_time_string = link[-13:]

                                seconds_of_link = time.mktime(time.strptime(date_time_string, date_format))
                                gap = abs(seconds_of_link - seconds_of_each_interval)
                                if gap < smallest_gap:
                                    smallest_gap = gap
                                    search_start_index = index
                                    closest_link = link
                                    closest_date_time_string = date_time_string

                        if len(closest_link):
                            # xml filename example: 20171001_0000.xml
                            xml_filename = closest_date_time_string.replace('-', '_') + ".xml"
                            try:
                                response = urllib.request.urlopen(closest_link)
                            except HTTPError as e:
                                data = str(e.code)
                                print('HTTPError = ' + data + '. Fetch TSM xml data error!')
                            except URLError as e:
                                data = str(e.reason)
                                print('URLError = ' + data + '. Fetch TSM xml data error!')
                            else:
                                self.page = response.read()
                                if save_xml:
                                    xml_date_folder = os.path.join(xml_folder_path, date_file + '/')
                                    if not os.path.exists(xml_date_folder):
                                        os.makedirs(xml_date_folder)
                                    try:
                                        with open(xml_date_folder + xml_filename, 'wb') as xml_file_out:
                                            xml_file_out.write(self.page)
                                            print('Saving: ' + xml_date_folder + xml_filename + ' successfully.')
                                    except IOError as err:
                                        print('File error: ' + str(err))
                                if store_database:
                                    # Store in database
                                    xml_record = self.parse_self_xml()
                                    if len(xml_record):
                                        # valid record
                                        self.store_TSM_data(xml_record)
                                        print('Parsing: ' + xml_filename + ' and storing in database successfully')
            except IOError as err:
                print('File error: ' + str(err))

    def store_TSM_data(self, records):
        """
        Store the records into the database
        Bad code
        :param records:
        :return:
        """

        client = MongoClient('127.0.0.1', 27017)
        db = client['traffic']
        collection = db[TRAFFIC_SPEED_COLLECTION]
        collection.insert_many(records)
        client.close()

    def find_latest_record(self):
        client = MongoClient('127.0.0.1', 27017)
        db = client['traffic']
        collection = db[TRAFFIC_SPEED_COLLECTION]
        records = list(collection.find().sort([('fetch_time', -1)]).limit(1))
        if len(records) == 0:
            print('No traffic speed data in current database')
            return None
        else:
            client.close()
            return records[0]

    def find_recent_records(self):
        """
        Collection the recent records(according to fetch time,
        :return:
        """

        client = MongoClient('127.0.0.1', 27017)
        db = client['traffic']
        collection = db[TRAFFIC_SPEED_COLLECTION]
        latest_record = self.find_latest_record()
        if latest_record is None:
            print('No traffic speed data in current database')
            client.close()
            return None
        else:
            return list(collection.find({'fetch_time': latest_record['fetch_time']}))

    def fetch_and_store(self):
        """
        This function is used to fetch and store the Recent Record.
        :return:
        """
        records = self.fetch_TSM_data()
        old_records = self.find_recent_records()

        if old_records is None:
            print('No record in database!')
        elif self.time_cover(records, old_records):
            print('Covered.')
            return

        self.store_TSM_data(records)

    def time_cover(self, old_records, new_records):
        """
        Decides if two records are overlapped, if overlapped return true and will not insert data
        :param old_records: existing records,
        :param new_records: new coming records
        :return: True if overlapped(no insert), False if not overlapped(insert)
        """

        old_time_list = list(set([r['capture_date_1970'] for r in old_records]))
        old_time_list = sorted(old_time_list, key=lambda x: x, reverse=False)
        new_time_list = list(set([r['capture_date_1970'] for r in new_records]))
        new_time_list = sorted(new_time_list, key=lambda x: x, reverse=False)
        if len(old_time_list) == 0 or len(new_time_list) == 0:
            return False

        [old_time_earliest, old_time_latest] = [old_time_list[0], old_time_list[-1]]
        [new_time_earliest, new_time_latest] = [new_time_list[0], new_time_list[-1]]
        print([old_time_earliest, old_time_latest], [new_time_earliest, new_time_latest])
        if old_time_earliest > new_time_latest or new_time_earliest > old_time_latest:
            return False
        else:
            return True


if __name__ == '__main__':
    tsm_fetcher = TSMFetcher()
    # print(tsm_fetcher.find_recent_records())

    # Store all new TSM data from 2016-12-01
    new_file_list = tsm_fetcher.fetch_TSM_save_links_file('20180501', '../../../../data/full_tsm_link/')
    tsm_fetcher.fetch_all_TSM_xml_from_link_file(new_file_list, True, False)

    """
    # Traverse existing files
    exist_file_list = []
    link_folder_path = os.path.join(tsm_fetcher.current_path, '../../data/tsm_link/')
    if os.path.isdir(link_folder_path):
        for (root, dirs, files) in os.walk(link_folder_path):
            exist_file_list = files
    tsm_fetcher.fetch_TSM_xml_from_link_file(exist_file_list, True, True)
    """

    # tsm_fetcher.fetch_and_store()

    # records = tsm_fetcher.find_recent_records()
    # records2 = tsm_fetcher.fetch_TSM_data()
    # result = tsm_fetcher.time_cover(records, records2)
    # print(result)
