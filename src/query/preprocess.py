import csv
from urllib.error import HTTPError, URLError
import urllib.request
import json
from pymongo import MongoClient
import psycopg2
from psycopg2 import extras


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


def get_mongodb_osm_links():
    """
    Get osm links information from MongoDB
    """

    client = MongoClient('127.0.0.1', 27018)
    db = client['local']
    osm_link_collection = db['osm_links']
    osm_links = list(osm_link_collection.find())
    client.close()
    return osm_links


def process_osm_links(osm_links):
    """
    Process and organize the osm link result from MongoDB
    :param osm_links: a list of osm links queried from MongoDB
    :return: a list of processed dicts with link information
    """

    link_dict_list = []
    for link in osm_links:
        # print(link['path'])
        parent_id = link['id']
        # Build edges
        for nid in range(len(link['path']) - 1):
            start_node = link['path'][nid]
            end_node = link['path'][nid + 1]
            link_id = start_node['id'] + "-" + end_node['id']
            # print(link_id)
            start_node_long = str(start_node['coordinates'][0])
            start_node_lat = str(start_node['coordinates'][1])
            end_node_long = str(end_node['coordinates'][0])
            end_node_lat = str(end_node['coordinates'][1])
            line_string = 'LINESTRING(' + start_node_long + ' ' + start_node_lat + ', ' \
                          + end_node_long + ' ' + end_node_lat + ')'
            # print(line_string)
            dict = {'link_id': link_id, 'parent_id': parent_id,
                    'start_long': start_node_long, 'start_lat': start_node_lat,
                    'end_long': end_node_long, 'end_lat': end_node_lat, 'line_string': line_string}
            link_dict_list.append(dict)
            print(dict)

    return tuple(link_dict_list)


def create_hk_osm_link_postgis_database(link_dicts):
    """
    Create hk_osm_link PostGIS database
    :param link_dicts: dicts contain link information from MongoDB
    :return:
    """

    try:
        # Create connection to ices database
        postgresql_connection = psycopg2.connect(dbname='gis', host='127.0.0.1', port='5432',
                                                 user='postgres', password='manage')
    except psycopg2.OperationalError as error:
        print('OperationalError: ' + str(error))
    else:
        # Create a cursor object using dict output format
        cursor = postgresql_connection.cursor(cursor_factory=psycopg2.extras.DictCursor)

        create_database_sql = """
        CREATE TABLE hk_osm_link (link_id text, parent_id text, 
        start_long text, start_lat text, end_long text, end_lat text, link GEOMETRY(LINESTRING, 4326));
        """

        insert_sql = """
        INSERT INTO hk_osm_link(link_id, parent_id, start_long, start_lat, end_long, end_lat, link) 
        VALUES (%(link_id)s, %(parent_id)s, 
        %(start_long)s, %(start_lat)s, %(end_long)s, %(end_lat)s, ST_GeomFromText(%(line_string)s, 4326));
        """

        create_index_sql = """
        CREATE INDEX hk_osm_link_index ON hk_osm_link USING GIST (link);
        """
        try:
            cursor.execute(create_database_sql)    # Create database
            cursor.executemany(insert_sql, link_dicts)    # Insert osm links
            cursor.execute(create_index_sql)    # Create index
            postgresql_connection.commit()    # Commit the changes
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if postgresql_connection is not None:
                cursor.close()
                postgresql_connection.close()


if __name__ == '__main__':
    # Store TSM node and link info into MongoDB
    # link_records, node_records = get_tsm_link_node_info('../../tsm_link_and_node_info_v2.csv')
    # store_tsm_link_node_info(link_records, node_records)

    # Store OSM links from TSM links into PostGis
    osm_links = get_mongodb_osm_links()
    link_dicts = process_osm_links(osm_links)
    create_hk_osm_link_postgis_database(link_dicts)
