"""
Class for fetch and parse air quality data from hk government pages
"""
from datetime import datetime
import urllib
from urllib import request
from urllib.error import HTTPError, URLError
from bs4 import BeautifulSoup
from src.config import Config
from src.utils import _logger


class AQExtractor:
    """This is a class used to extract air quality information from specific government url.
    default url = http://www.aqhi.gov.hk/en/aqhi/pollutant-and-aqhi-distribution.html

    Attributes:
        weather_url: the url from which the air quality information will be extracted.
        page: urllib page, the whole html page
        page_soup： beautiful soup module
        table_head: the table head of html page []
        air_quality: the air quality information [{attr1: val1; att2, val2}]
        update_tiem: the recent update time on the webpages
    """

    weather_url = 'http://www.aqhi.gov.hk/en/aqhi/pollutant-and-aqhi-distribution.html'
    page = None
    page_soup = None
    table_head = ['StationName', 'NO2', 'O3', 'SO2', 'CO', 'PM10', 'PM2.5', 'AQHI']
    air_quality = []
    update_time = -1

    def __init__(self, path=None):
        """Init the path and parse url

        :param path: the path of url, default page are 'weather url
        """
        if(path != None):
            self.weather_url = path
        self.__parse_table()

    def __parse_table(self):
        """Automatically parse page and extract the air quality tables

        """
        try:
            response = urllib.request.urlopen(self.weather_url)
        except HTTPError as e:
            data = str(e.code)
            print('HTTPError = ' + data + '. Air Quality AQExtractor!')

        except URLError as e:
            data = str(e.reason)
            print('URLError = ' + data + '. Air Quality AQExtractor!')

        else:
            self.page = response
            self.page_soup = BeautifulSoup(self.page, "html.parser")
            self.air_quality = self.__extract_airquality_from_tables()

    def get_recent_update_time(self):
        """Return the recent update of the air quality webpage

        :return: time of the recent update,in format  %Y-%m-%d %H:%M:%S (2016-11-16 13:00:00)
        """
        return self.time_string

    def get_air_quality(self):
        """Get the air quality data

        :return: air quality data(from __extract_airquality_from_tables)
        """
        return self.air_quality

    def __extract_airquality_from_tables(self):
        """Extract the air quality data from the tables in the page

        :return: The air quality data
        """
        self.time_string = self.__extract_update_time()
        tables = self.page_soup.find_all('table', {'class': 'tblPollutant'})
        rows = []
        for table in tables:
            for row in table.findAll('tr'):
                row_elements = [td.text for td in row.findAll('td')]
                if len(row_elements) != 8:
                    continue
                rows.append(row_elements)

        if(len(rows) == 0):
            print('No information detected!')
            return -1

        structured_records = []
        for row in rows:
            row_structure = {}
            for index in range(len(self.table_head)):
                attr_name = self.table_head[index]
                if attr_name in row_structure:
                    print('Attributes existed! Check the pages!')

                if index == 0:
                    station_id = self.__get_station_id_from_name(attr_name)
                    row_structure[attr_name] = row[index]
                else:
                    value = row[index]
                    value = float(value) if isfloat(value) else None

                    row_structure[attr_name] = value

            row_structure['station_id'] = station_id
            row_structure['update_time'] = self.time_string
            row_structure['PM2_5'] = row_structure.pop('PM2.5') # Mongo DB recommend not using '.' in keys
            structured_records.append(row_structure)
        return structured_records

    def __extract_update_time(self):
        """ Extract update time from the page

        :return: the recent update time as the following format %Y-%m-%d %H:%M:%S (2016-11-16 13:00:00)
        """
        time_ele = self.page_soup.find('div', {'id': 'distributionField'})
        if time_ele:
            time_str = time_ele.find('p').text
            try:
                date_object = datetime.strptime(time_str, '(At %Y-%m-%d %H:%M)')
            except ValueError as e:
                print(e)
            else:
                return date_object.__str__()
        else:
            print('No time detected!')
            return None

    def __get_station_id_from_name(self, str):
        """This function should be the one in the config class"""
        return None

def isfloat(value):
    try:
        float(value)
        return True
    except ValueError:
        return False

def fetch_air_quality():
    aq = AQExtractor()
    quality = aq.get_air_quality()
    return quality

def fetch_and_store_air_quality():
    data = fetch_air_quality()
    current_time = data[0]['update_time']
    if AQExtractor.update_time != current_time:
        AQExtractor.update_time = current_time
        collection = Config.get_collection_handler('air_quality')
        collection.insert_many(data)
        _logger.info('Air quality data of time: ' + current_time + ' fetched and stored successfully.')
    else:
        _logger.info('Fetched data of time: '+current_time+'. No need to update air quality data.')



if __name__ == '__main__':
    # data = fetch_air_quality()
    # for d in data:
    #     print(d)
    fetch_and_store_air_quality()
