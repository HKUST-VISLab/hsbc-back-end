"""
This file initializes the configuration of the stations from hkust
"""

URL = "http://envf.ust.hk/dataview/stnplot/current/get_stations.py?lat__float=22.5&lat_r__float=0.7&lon__float=114.0&lon_r__float=0.7&from_date__YMD=20170529&to_date__YMD=20170604&tz__int=8&submit%3Astring=+Query+"
import urllib
from urllib import request
from bs4 import BeautifulSoup
import os

HOST = '127.0.0.1'
PORT = 27017
DB = 'hk_weather_data'
STATION_COLLECTION = 'air_stations_hkust'

id_map = {"Station Name": "station_name",
          "ID": "station_code",
          "Number": "number",
          "State": "state",
          "Country": "country",
          "Latitude": "latitude",
          "Longitude": "longitude",
          "Height": "height",
          "Plots": "plots"}

def readTestFile(path = "test.html"):
    with open(path, 'r') as input:
        return input.read()

def read_head(table):
    ths = table.findAll('th')
    return [th.text.strip() for th in ths]

def check_head(ths, check_ids):
    for th in ths:
        if th not in check_ids:
            return False
    return True

def is_head(tr):
    if len(tr.findAll('th')) > 0 and len(tr.findAll('td')) == 0:
        return True
    else:
        return False

def is_context(tr):
    if len(tr.findAll('td')) > 0 and len(tr.findAll('th')) == 0:
        return True
    else:
        return False

def table_to_dict_from_url(url):
    response = request.urlopen(url)
    html = response.read()
    # CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
    # CONFIG_DIR = os.path.join(CURRENT_DIR, '../../data')
    #
    # html = readTestFile(CONFIG_DIR + '/hkust_stations.html')

    soup = BeautifulSoup(html, 'lxml')
    tables = soup.findAll('table')
    stations = []
    for table in tables:
        ths = read_head(table)
        if check_head(ths, check_ids=id_map) == False:
            continue
        trs = table.findAll('tr')
        for tr in trs:
            if is_context(tr):
                tds = tr.findAll('td')
                td_context = [td.text.strip() for td in tds]
                dict_context = {}
                for index in range(len(ths)):
                    key = id_map[ths[index]]
                    value = td_context[index]
                    dict_context[key] = value
                lat = dict_context.pop('latitude')
                lon = dict_context.pop('longitude')
                dict_context['loc'] = [float(lat), float(lon)]
                stations.append(dict_context)
    return stations

def create_hkust_station_config():
    import json
    from pymongo import MongoClient

    CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
    hkust_station_config_path = os.path.join(CURRENT_DIR, '../config/air_station_hkust_config.json')
    stations = table_to_dict_from_url(URL)
    print(stations)
    with open(hkust_station_config_path, 'w') as inputfile:
        json.dump(stations, inputfile)

    client = MongoClient(HOST, PORT)
    db = client[DB]
    collection = db[STATION_COLLECTION]
    collection.remove({})
    collection.insert_many(stations)
    client.close()

if __name__ == '__main__':
    create_hkust_station_config()