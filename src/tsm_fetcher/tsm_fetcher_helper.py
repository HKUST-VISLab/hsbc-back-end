from urllib.error import HTTPError, URLError
import urllib
import urllib.request
from pymongo import MongoClient
import pymongo
from lxml import etree
import time

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

traffic_speed_collection = "traffic_speed"

class TSMFetcher:
    """This is a class used to fetch data from the hk gov data.
    default url = http://resource.data.one.gov.hk/td/speedmap.xml
    The gov data page can be found: https://data.gov.hk/en-data/dataset/hk-td-tis-traffic-speed-map
    """

    tsm_path = REQUESTPATH
    entity_tag = "jtis_speedmap"
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
            records.append(record)
        return records

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
    print(tsm_fetcher.find_recent_records())
    # tsm_fetcher.fetch_and_store()
    # records = tsm_fetcher.fetch_recent_records()
    # records2 = tsm_fetcher.fetch_TSM_data()
    # result = tsm_fetcher.time_cover(records, records2)
    # print(result)