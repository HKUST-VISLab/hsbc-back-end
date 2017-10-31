from urllib.error import HTTPError, URLError
import urllib
import urllib.request
from pymongo import MongoClient
from lxml import etree
import time
import json
import os
import glob

#  Modify: save the parsed data as local files
#          log the schedual
#          Realtime updating
#  Recent record: latest: http://resource.data.one.gov.hk/td/speedmap.xml
#  Historical record: https://api.data.gov.hk/v1/historical-archive/get-file?url=http%3A%2F%2Fresource.data.one.gov.hk%2Ftd%2Fspeedmap.xml&time=20170901-0049
#

REQUESTPATH = 'http://resource.data.one.gov.hk/td/speedmap.xml'

tag_map = {
    "LINK_ID": 'id',
}

traffic_speed_collection = "traffic_speed_map"

class TSMFetcher:
    """This is a class used to fetch data from the hk gov data.
    default url = http://resource.data.one.gov.hk/td/speedmap.xml
    The gov data page can be found: https://data.gov.hk/en-data/dataset/hk-td-tis-traffic-speed-map
    """

    tsm_path = REQUESTPATH
    entity_tag = "jtis_speedmap"
    current_path = os.path.dirname(os.path.abspath(__file__))

    def __init__(self):
        pass

    def fetch_TSM_data(self, default_path = REQUESTPATH):
        try:
            response = urllib.request.urlopen(REQUESTPATH)

        except HTTPError as e:
            data = str(e.code)
            print('HTTPError = ' + data + '. Air Quality AQExtractor!')

        except URLError as e:
            data = str(e.reason)
            print('URLError = ' + data + '. Air Quality AQExtractor!')
        else:
            self.page = response.read()
            return self.fetch_tsm_once()

    def fetch_tsm_once(self):
        # Do we need to save the xml into local file?
        current_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        def parse_tag(raw_tag):
            return raw_tag.split('}')[1]
        parser = etree.XMLPullParser(events=('start', ))
        parser.feed(self.page)
        records = []
        for action, element in parser.read_events():
            if parse_tag(element.tag) != self.entity_tag:
                continue
            segs = [{parse_tag(t.tag.lower()): t.text} for t in element]
            record = {}
            for seg in segs:
                record.update(seg)
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

        return records

    def fetch_tsm_save_link_file(self, start_date='20171001'):
        """
        Save the xml links for everyday from start date
        :param start_date: start date in 'yyyymmdd' format
        :return:
        """
        date_format = "%Y%m%d"
        current_time = time.strftime("%Y%m%d", time.localtime())
        start = int(time.mktime(time.strptime(start_date, date_format)))
        end = int(time.mktime(time.strptime(current_time, date_format)))
        # End with yesterday
        date_list = [time.strftime(date_format, time.localtime(i)) for i in range(start, end, 3600 * 24)]

        # Build folder for data storage
        link_folder_path = os.path.join(self.current_path, '../../data/tsm-link/')
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
                try:
                    with open(date_string, 'w') as file_out:
                        separate = '\n'
                        file_string = separate.join(xml_list)
                        file_out.write(file_string)
                except IOError as err:
                    print('File error: ' + str(err))

    def fetch_tsm_save_xml_file(self):
        """
        Collect historical xml records from the links in local file.
        :return:
        """
        xml_folder_path = os.path.join(self.current_path, '../../data/tsm-xml/')
        # Read link files in link folder
        link_folder_path = os.path.join(self.current_path, '../../data/tsm-link/')
        os.chdir(link_folder_path)
        for date_file in glob.glob('*'):
            try:
                with open(date_file) as file_in:
                    link_list = file_in.readlines()
                    for link in link_list:
                        # Example: 20171001-0000.xml
                        xml_filename = link[-14:-1] + ".xml"
                        if link == link_list[-1]:
                            # Without '\n'
                            xml_filename = link[-13:] + ".xml"
                        try:
                            response = urllib.request.urlopen(link)
                        except HTTPError as e:
                            data = str(e.code)
                            print('HTTPError = ' + data + '. Fetch TSM xml data error!')
                        except URLError as e:
                            data = str(e.reason)
                            print('URLError = ' + data + '. Fetch TSM xml data error!')
                        else:
                            xml_date_folder = os.path.join(xml_folder_path, date_file + '/')
                            if not os.path.exists(xml_date_folder):
                                os.makedirs(xml_date_folder)
                            try:
                                with open(xml_date_folder + xml_filename, 'wb') as xml_file_out:
                                    xml_file_out.write(response.read())
                            except IOError as err:
                                print('File error: ' + str(err))
            except IOError as err:
                print('File error: ' + str(err))

    def fetch_local_tsm_data(self):
        """
        Parse historical xml from local file.
        """
        xml_folder_path = os.path.join(self.current_path, '../../data/tsm-xml/')
        os.chdir(xml_folder_path)
        for root, dirs, files in os.walk(xml_folder_path):
            for name in files:
                xml_filename = os.path.join(root, name)
                try:
                    with open(xml_filename) as xml_in:
                        self.page = xml_in.read()
                        xml_record = self.fetch_tsm_once()
                        print('Parsing: ' + name)
                        self.store_tsm_data(xml_record)
                except IOError as err:
                    print('File error: ' + str(err))

    def store_tsm_data(self, records):
        """
        Store the records into the database
        Bad code
        :param records:
        :return:
        """

        client = MongoClient('127.0.0.1', 27017)
        db = client['traffic']
        collection = db[traffic_speed_collection]
        collection.insert_many(records)
        client.close()

    def find_latest_record(self):
        client = MongoClient('127.0.0.1', 27017)
        db = client['traffic']
        collection = db[traffic_speed_collection]
        records = list(collection.find().sort([('fetch_time', -1)]).limit(1))
        if len(records) == 0:
            print('No traffic speed data in current database')
            return None
        else:
            return records[0]
        client.close()


    def find_recent_records(self):
        """
        Collection the recent records(according to fetch time,
        :return:
        """

        client = MongoClient('127.0.0.1', 27017)
        db = client['traffic']
        collection = db[traffic_speed_collection]
        latest_record = self.find_latest_record()
        if latest_record == None:
            print('No traffic speed data in current database')
            client.close()
            return None
        else:
            records = collection.find({'fetch_time': latest_record['fetch_time']})
        return list(records)



    def fetch_and_store(self, arg = None):
        """
        This function is used to fetch and store the Recent Record.
        :param arg:
        :return:
        """
        records = self.fetch_TSM_data()
        old_records = self.find_recent_records()
        if self.time_cover(records, old_records):
            print('cover')
            return
        self.store_tsm_data(records)

    def time_cover(self, records1, records2):
        """
        Decides if two records are overalpped, if overlapped return true and will not insert data
        :param records1: first records,
        :param records2: second records
        :return: True if overlapped(no insert), False if not overlapped(insert)
        """

        time_list1 = list(set([time.mktime(time.strptime(r['capture_date'], "%Y-%m-%d %H:%M:%S")) for r in records1]))
        time_list1 = sorted(time_list1, key=lambda x: x, reverse=False)
        time_list2 = list(set([time.mktime(time.strptime(r['capture_date'], "%Y-%m-%d %H:%M:%S")) for r in records2]))
        time_list2 = sorted(time_list2, key=lambda x: x, reverse=False)

        if len(time_list2) == 0 or len(time_list2) == 0:
            return False
        [s1, l1] = [time_list1[0], time_list1[-1]]
        [s2, l2] = [time_list2[0], time_list2[-1]]
        print([s1, l1], [s2, l2])
        return False if s1>l2 or s2> l1 else True


if __name__ == '__main__':
    tsm_fetcher = TSMFetcher()
    #tsm_fetcher.fetch_tsm_save_link_file('20161201')
    #tsm_fetcher.fetch_tsm_save_xml_file()
    #tsm_fetcher.fetch_local_tsm_data()

    print(tsm_fetcher.find_recent_records())
    # tsm_fetcher.fetch_and_store()

    # records = tsm_fetcher.fetch_recent_records()
    # records2 = tsm_fetcher.fetch_TSM_data()
    # result = tsm_fetcher.time_cover(records, records2)
    # print(result)