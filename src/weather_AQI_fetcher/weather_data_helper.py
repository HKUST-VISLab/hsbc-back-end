from time_processing import TimeStrUnmatchedError, format_time
import time

from time_processing import TimeStrUnmatchedError, format_time
import time
import urllib
from urllib.error import HTTPError, URLError
import urllib.request
import re
from pymongo import MongoClient

"""
Raised Error:
HTTPError
URLError
TimeStrUnmatchedError
FeatureTypeError
"""


class FeatureTypeError(Exception):
    """
    Raised if time is not matched!
    TODO: to be done
    """
    pass


class DataFetcher():
    url = 'http://envf.ust.hk/dataview/obs4lstm/current/'
    """
    TODO:
    AQs, metes,AQ_folder,mete_folder should be find in a config file
    """
    "----------------------------------------------------------------------"
    AQs = ['NO2', 'O3', 'SO2', 'CO', 'PM25', 'PM10']
    metes = ['Temp', 'RH', 'Wind', 'Pressure-Station', 'Pressure-SeaLevel', 'DewPt', 'CloudCover']
    AQ_folder = '/home/hsbc/data/realtime/AQ/'
    mete_folder = '/home/hsbc/data/realtime/mete/'
    AQ_folder = AQ_folder + 'hourly/'
    mete_folder = mete_folder + 'hourly/'
    "----------------------------------------------------------------------END"

    """Database operation"""
    "----------------------------------------------------------------------"
    #     ['AQ_pred_history', 'mete_pred_history']
    db = MongoClient('127.0.0.1', 27017)['HSBC_realtime_prediction']
    meteorology_collection_history = db['mete_pred_history']
    aq_collection_history = db['AQ_pred_history']
    "----------------------------------------------------------------------END"

    def __init__(self):
        pass

    def _dump_data(self, feature_type, start_time, end_time, data):
        """
        Construct an url used to fetch data from ENVF
        @params:
            feature_type: air pollutant or meteorology(string);
            start_time/end_time: the four time format defined   
                                    time_format_compact: "201805310000"
                                    time_format_standard = "2017-01-02 11:00:00"   
                                    time_format_seconds = 1451577600.0
                                    time_format_url = "20180531-0000"
        """

        _folder = self.AQ_folder if feature_type in self.AQs else self.mete_folder
        filename = _folder + '{}_{}_{}.csv'.format(feature_type, format_time(start_time, 'url'),
                                                   format_time(end_time, 'url'))

        with open(filename, 'wb') as input_file:
            input_file.write(data)
        # print(filename, 'finished!')
        return filename

    def _construct_fetch_url(self, feature_type, start_time, end_time):
        """
        Construct an url used to fetch data from ENVF
        @params:
            feature_type: air pollutant or meteorology(string);
            start_time/end_time: the four time format defined   
                                    time_format_compact: "201805310000"
                                    time_format_standard = "2017-01-02 11:00:00"   
                                    time_format_seconds = 1451577600.0
                                    time_format_url = "20180531-0000"
        """
        start_time = format_time(start_time, 'url')
        end_time = format_time(end_time, 'url')
        if start_time == None or end_time == None:
            return None
        _url = self.url + '?start_time={}&end_time={}&type={}'.format(start_time, end_time, feature_type)
        return _url

    def check_file_exists(self, feature_type, start_time, end_time):
        """
        Check if the downloaded files exist in the folder
        TODO: if necessary
        @params: 
            feature_type: air pollutant or meteorology(string);
            start_time/end_time: 2017-01-02 11:00:00      
        """
        pass

    def fetch_data(self, feature_type, start_time, end_time):

        """
        Fetch data from url given by ENVF
        @params: 
            feature_type: air pollutant or meteorology(string);
            start_time/end_time: the four time format defined   
                                    time_format_compact: "201805310000"
                                    time_format_standard = "2017-01-02 11:00:00"   
                                    time_format_seconds = 1451577600.0
                                    time_format_url = "20180531-0000"
        """
        st = time.time()
        if feature_type not in self.AQs and feature_type not in self.metes:
            raise FeatureTypeError('Feature ' + feature_type + ' is invalid')

        _url = self._construct_fetch_url(feature_type, start_time, end_time)

        try:
            response = urllib.request.urlopen(_url)
        except HTTPError as e:
            data = str(e.code)
            print('HTTPError = ' + data + '. DataFetcher!')
        except URLError as e:
            data = str(e.reason)
            print('URLError = ' + data + '. DataFetcher!')
        else:
            data = response.read()
            file_path = self._dump_data(feature_type, start_time, end_time, data)
            self._input_file_db(file_path, feature_type)
            print('Finish process ', feature_type, start_time, end_time, time.time() - st)

    def _input_file_db(self, file_path, feature_type):
        if feature_type in self.AQs:
            _collection = self.aq_collection_history
        elif feature_type in self.metes:
            _collection = self.meteorology_collection_history
        else:
            print('Wrong in feature_type: ', feature_type)

        group = []
        with open(file_path, 'r') as input_file:
            input_file.readline()
            line = input_file.readline()
            if not line:
                return
            while line:
                try:
                    line_segs = line.split(',')
                    loc_segs = [float(_e[1:-1]) if _e.startswith('"') else float(_e) for _e in line_segs[:2]]
                    time_segs = [_e[1:-1] if _e.startswith('"') else _e for _e in line_segs[3:5]]
                    time_str = ' '.join(time_segs)
                    sec_time = time.mktime(time.strptime(time_str, "%Y-%m-%d %H:%M:%S"))

                    val = float(line_segs[6])
                    _dict = {'type': feature_type,
                             'time': sec_time,
                             'loc': loc_segs,
                             'val': val,
                             'rid': '_'.join([feature_type, str(int(sec_time))] + [str(c) for c in loc_segs])}

                    if feature_type == 'Wind' or feature_type == 'AQI':
                        val2 = float(line_segs[7])
                        _dict['val2'] = val2
                    _collection.update_one({'rid': _dict['rid']}, {'$set': _dict}, upsert=True)

                # print(_dict)
                #                     group.append(_dict)
                #                     if len(group) >= 100000:
                #                         collection.insert_many(group)
                #                         group = []

                except Exception as e:
                    print('Something error', e)

                line = input_file.readline()

                #             if len(group) != 0:
                #                 print('group')
                #                 print(group)
                # #                 collection.insert_many(group)
                #                 group = []

    def fetch_all_features(self, start_time='2018-05-11 00:00:00', end_time='2018-05-11 01:00:00'):
        """
        Download the 
        TODO: if necessary
        @params: 
            feature_type: air pollutant or meteorology(string);
            start_time/end_time: 2017-01-02 11:00:00      
        """
        for aq in self.AQs:
            self.fetch_data(aq, start_time, end_time)
        for mete in self.metes:
            self.fetch_data(mete, start_time, end_time)

