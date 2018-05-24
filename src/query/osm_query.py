"""
A class for querying OSM data from PostGIS and MongoDB
"""
import psycopg2
from psycopg2 import extras
from pymongo import MongoClient

DATABASE = "gis"
HOST = "127.0.0.1"
PORT = "5432"
USER = "postgres"
PASSWORD = "manage"


class OSMQuery:
    def __init__(self):
        self.host = HOST
        self.port = PORT
        self.database = DATABASE
        self.user = USER
        self.password = PASSWORD

    def query_point_distance_from_road(self, point_longitude, point_latitude, way_id, print_sql=False):
        """
        Get the nearest distance between a point and a road
        :param point_latitude: the latitude of a query point
        :param point_longitude: the longitude of a query point
        :param way_id: the way_id(osm_id) of the road
        :param dict_cursor: if use dict data format for the query result
        :param print_sql: if print the sql code
        :return: the nearest distance between a point a the road or None
        """
        try:
            # Create connection to ices database
            postgresql_connection = psycopg2.connect(dbname=self.database, host=self.host, port=self.port,
                                                     user=self.user, password=self.password)
        except psycopg2.OperationalError as e:
            print("dbname=" + self.database + ", host=" + self.host + ", port=" + self.port
                  + " , user=" + self.user + ", password=" + self.password)
            print("OperationalError: " + str(e))
        else:
            # Create a cursor object using dict output format
            cursor = postgresql_connection.cursor(cursor_factory=psycopg2.extras.DictCursor)

            sql_query = """
            SELECT osm_id, highway, name, ref, 
            ST_X(ST_ClosestPoint(ST_Transform(r.way,4326), point.geom)), 
            ST_Y(ST_ClosestPoint(ST_Transform(r.way,4326), point.geom)), 
            ST_Distance_Sphere(ST_ClosestPoint(ST_Transform(r.way,4326), point.geom), point.geom) 
            FROM planet_osm_roads r, (SELECT ST_SetSRID(ST_MakePoint(%s, %s), 4326) as geom) point 
            WHERE osm_id=%s;
            """
            if print_sql:
                print(sql_query)

            try:
                cursor.execute(sql_query, (str(point_longitude), str(point_latitude), str(way_id)))
            except psycopg2.DatabaseError as e:
                print("DatabaseError: " + str(e))

            result = cursor.fetchone()
            cursor.close()
            postgresql_connection.close()

            # Return the distance
            return result['st_distance_sphere']

    def query_road_list_from_point(self, point_longitude, point_latitude, distance, print_sql=False):
        """
        Get the list of roads from a point within a distance
        :param point_latitude: the latitude of a query point
        :param point_longitude: the longitude of a query point
        :param distance: search roads within a distance (unit: meter)
        :param print_sql: if print the sql code
        :return: road results in DictRow format
        """
        try:
            # Create connection to ices database
            postgresql_connection = psycopg2.connect(dbname=self.database, host=self.host, port=self.port,
                                                     user=self.user, password=self.password)
        except psycopg2.OperationalError as e:
            print("dbname=" + self.database + ", host=" + self.host + ", port=" + self.port
                  + " , user=" + self.user + ", password=" + self.password)
            print("OperationalError: " + str(e))
        else:
            # Create a cursor object using dict DictRow format
            cursor = postgresql_connection.cursor(cursor_factory=psycopg2.extras.DictCursor)

            sql_query = """
            SELECT * FROM
            (SELECT osm_id, highway, name, ref,
            ST_X(ST_ClosestPoint(ST_Transform(r.way,4326), point.geom)),
            ST_Y(ST_ClosestPoint(ST_Transform(r.way,4326), point.geom)),
            ST_Distance_Sphere(ST_ClosestPoint(ST_Transform(r.way,4326), point.geom), point.geom)
            FROM planet_osm_roads r,
            (SELECT ST_SetSRID(ST_MakePoint(%s, %s),4326) as geom) point
            ORDER BY 7 ASC) AS A
            WHERE A.st_distance_sphere < %s AND A.highway IS NOT NULL;
            """
            if print_sql:
                print(sql_query)

            try:
                cursor.execute(sql_query, (str(point_longitude), str(point_latitude), str(distance)))
            except psycopg2.DatabaseError as e:
                print("DatabaseError: " + str(e))
            else:
                result = cursor.fetchall()
                cursor.close()
                postgresql_connection.close()
                # Return the distance
                return result

    def query_osm_link_list_from_point(self, point_longitude, point_latitude, distance, print_sql=False):
        """
        Get the list of osm links from a point within a distance
        :param point_latitude: the latitude of a query point
        :param point_longitude: the longitude of a query point
        :param distance: search roads within a distance (unit: meter)
        :param print_sql: if print the sql code
        :return: osm link results in DictRow format
        """
        try:
            # Create connection to ices database
            postgresql_connection = psycopg2.connect(dbname=self.database, host=self.host, port=self.port,
                                                     user=self.user, password=self.password)
        except psycopg2.OperationalError as e:
            print("dbname=" + self.database + ", host=" + self.host + ", port=" + self.port
                  + " , user=" + self.user + ", password=" + self.password)
            print("OperationalError: " + str(e))
        else:
            # Create a cursor object using DictRow output format
            cursor = postgresql_connection.cursor(cursor_factory=psycopg2.extras.DictCursor)

            sql_query = """
            SELECT * FROM
            (SELECT link_id, parent_id, ST_Length(ST_Transform(link,4326), false), 
            ST_Distance_Sphere(ST_ClosestPoint(ST_Transform(l.link,4326), point.geom), point.geom)
            FROM hk_osm_link l,
            (SELECT ST_SetSRID(ST_MakePoint(%s, %s),4326) as geom) point
            ORDER BY parent_id, st_distance_sphere ASC) AS A
            WHERE A.st_distance_sphere < %s;
            """
            if print_sql:
                print(sql_query)

            try:
                cursor.execute(sql_query, (str(point_longitude), str(point_latitude), str(distance)))
            except psycopg2.DatabaseError as e:
                print("DatabaseError: " + str(e))
            else:
                result = cursor.fetchall()
                cursor.close()
                postgresql_connection.close()
                # Return the distance
                return result


def query_road_link_from_mongodb(link_id):
    """
    Query a road with link id stored in MongoDB
    :param link_id: string of two indexes (example: "3006-30069")
    :return: records of the road from all time
    """

    client = MongoClient("127.0.0.1", 27017)
    db = client["traffic"]
    tsm_collection = db["traffic_speed_map"]
    road_records = list(tsm_collection.find({"link_id": link_id}))
    # road_records = list(tsm_collection.find({"link_id": link_id}).sort([('capture_date', -1)]).limit(1))
    client.close()

    if len(road_records) == 0:
        print("No traffic speed data of this road in current database")
        return None
    else:
        return road_records


def update_latest_road_link_info_from_mongodb(link_list):
    """
    Query and update the latest information for the link list
    :param link_list: a list of links queried from MongoDB
    :return: an updated link list
    """

    client = MongoClient("127.0.0.1", 27017)
    db = client["traffic"]
    tsm_collection = db["traffic_speed_map"]

    for link in link_list:
        link_id = link['parent_id']
        # Transfer the cursor to a list (TODO: May update according to the requirements)
        latest_link_record = list(tsm_collection.find({"link_id": link_id}).sort([('capture_date_1970', -1)]).limit(1))
        # print(latest_link_record[0])
        link_saturation_level = latest_link_record[0]['road_saturation_level']
        link_traffic_speed = int(latest_link_record[0]['traffic_speed'])
        link.append(link_saturation_level)
        link.append(link_traffic_speed)

    client.close()

    return link_list


if __name__ == '__main__':
    osm_query = OSMQuery()
    # distance = osm_query.query_point_distance_from_road(114.26, 22.33, 157424143)
    # print("Nearest distance between (114.26, 22.33) and the road (way-id 157424143): " + str(distance) + "\n")

    link_list = osm_query.query_osm_link_list_from_point(114.20, 22.34, 1000)
    print("Links(link_id, parent_id, st_length, st_distance_sphere) from (114.20, 22.34) within 1000 meters: ")
    for link in link_list:
        print(link)

    link_list = update_latest_road_link_info_from_mongodb(link_list)
