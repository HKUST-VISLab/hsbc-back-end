
from urllib.error import HTTPError, URLError
import urllib
import urllib.request
from lxml import etree
import time

REQUESTPATH = 'http://resource.data.one.gov.hk/td/speedmap.xml'

tag_map = {
    "LINK_ID": 'id',
}
class TSMFetcher:
    """This is a class used to fetch data from the hk gov data.
    default url = http://resource.data.one.gov.hk/td/speedmap.xml
    The gov data page can be found: https://data.gov.hk/en-data/dataset/hk-td-tis-traffic-speed-map
    """

    tsm_path = REQUESTPATH
    entity_tag = "jtis_speedmap"
    def __init__(self):
        pass

    def fetch_TSM_data(self):

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
            return self.pares_xml_once()


    def pares_xml_once(self):
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
        from pymongo import MongoClient

        client = MongoClient('127.0.0.1', 27017)
        db = client['traffic']
        collection = db['traffic_speed']
        collection.insert_many(records)
        client.close()


    def fetch_and_store(self):
        records = self.fetch_TSM_data()
        print('record', records)
        self.store_tsm_data(records)

if __name__ == '__main__':
    tsm_fetcher = TSMFetcher()
    tsm_fetcher.fetch_and_store()