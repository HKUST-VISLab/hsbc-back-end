import csv
from urllib.error import HTTPError, URLError
import urllib.request
import json
from pymongo import MongoClient


def get_tsm_link_node_info(csv_path):
    """
    Get the records from official info csv file
    :param csv_path: path of the csv file
    :return: link_records and node_records
    """

    link_dict_list = []
    try:
        with open(csv_path, newline='') as csv_file:
            reader = csv.DictReader(csv_file)
            for row in reader:
                # print(row)
                link_dict_list.append(row)
    except IOError as err:
        print('File error: ' + str(err))

    link_records = []
    node_records = []
    node_id_list = []
    for link_dict in link_dict_list:
        start_id = link_dict['Start Node']
        end_id = link_dict['End Node']

        link_record = {'link_id': link_dict['Link ID'], 'road_region': link_dict['Region'],
                       'road_type': link_dict['Road Type'], 'start_id': start_id, 'end_id': end_id}
        link_records.append(link_record)

        if start_id not in node_id_list:
            node_id_list.append(start_id)
            coordinates = convert_coordinates(link_dict['Start Node Eastings'], link_dict['Start Node Northings'])
            node_record = {'node_id': start_id, 'lat': coordinates['wgsLat'], 'long': coordinates['wgsLong']}
            print('Converted start_id: ' + start_id + '. Lat: ' + str(coordinates['wgsLat'])
                  + ', Long: ' + str(coordinates['wgsLong']))
            node_records.append(node_record)

        if end_id not in node_id_list:
            node_id_list.append(end_id)
            coordinates = convert_coordinates(link_dict['End Node Eastings'], link_dict['End Node Northings'])
            node_record = {'node_id': end_id, 'lat': coordinates['wgsLat'], 'long': coordinates['wgsLong']}
            print('Converted end_id: ' + end_id + '. Lat: ' + str(coordinates['wgsLat'])
                  + ', Long: ' + str(coordinates['wgsLong']))
            node_records.append(node_record)

    return link_records, node_records


def convert_coordinates(eastings, northings):
    """
    Convert HK 1980 Grid Coordinate to WGS 84 with Decimal Degree unit
    :param eastings: eastings of a node in HK 1980 Grid Coordinate
    :param northings: northings of a node in HK 1980 Grid Coordinate
    :return: a list contain ['wgsLat'] and ['wgsLong']
    """

    transform_path = 'http://www.geodetic.gov.hk/transform/v2/?inSys=hkgrid&outSys=wgsgeog&outUnit=decDeg' \
                   '&e=' + eastings + '&n=' + northings
    try:
        response = urllib.request.urlopen(transform_path)
    except HTTPError as e:
        data = str(e.code)
        print('HTTPError = ' + data)
    except URLError as e:
        data = str(e.reason)
        print('URLError = ' + data)
    else:
        return json.loads(response.read())


def store_tsm_link_node_info(link_records, node_records):
    """
    Store the records of road links and nodes into the database
    :return:
    """

    client = MongoClient('127.0.0.1', 27017)
    db = client['traffic']
    link_collection = db['tsm_link_info']
    link_collection.insert_many(link_records)
    node_collection = db['tsm_node_info']
    node_collection.insert_many(node_records)
    client.close()


if __name__ == '__main__':
    link_records, node_records = get_tsm_link_node_info('../../tsm_link_and_node_info_v2.csv')
    # print(link_records)
    # print(node_records)
    store_tsm_link_node_info(link_records, node_records)

