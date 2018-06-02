"""
A class for querying OSM data from PostGIS and MongoDB
"""
import psycopg2
from psycopg2 import extras
from pymongo import MongoClient
import os
import json


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
            return result["st_distance_sphere"]

    def query_road_list_from_point(self, point_longitude, point_latitude, distance=1000, print_sql=False):
        """
        Get the list of roads from a point within a distance
        :param point_longitude: the longitude of a query point
        :param point_latitude: the latitude of a query point
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

    def query_osm_link_list_from_point(self, point_longitude, point_latitude, distance=1000, print_sql=False):
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

    def update_latest_link_info_from_mongodb(self, link_list, time_second, query_gap_second=3600):
        """
        Query and update the latest information for the link list
        :param link_list: a list of links queried from MongoDB
        :param time_second: seconds since the epoch
        :param query_gap_second: the time gap for query
        :return: a list of updated link dicts
        """

        client = MongoClient("127.0.0.1", 27017)
        db = client["traffic"]
        tsm_collection = db["traffic_speed_map"]

        start_time_second = time_second - (query_gap_second + 1800)
        query_data_size = int(query_gap_second / 1800)

        link_dict_list = []
        for link in link_list:
            link_dict = {}
            link_id = link["parent_id"]
            # Transfer the cursor to a list
            latest_link_record_list = list(tsm_collection
                                           .find({"link_id": link_id,
                                                  "capture_date_1970": {"$lt": time_second, "$gte": start_time_second}})
                                           .limit(query_data_size))
            # print(latest_link_record_list)

            # Traffic information lists
            link_saturation_level = []
            link_traffic_speed = []

            def convert_saturation_string(saturation_string):
                saturation_level = 0
                if saturation_string == "TRAFFIC GOOD":
                    saturation_level = 3
                elif saturation_string == "TRAFFIC AVERAGE":
                    saturation_level = 2
                elif saturation_string == "TRAFFIC BAD":
                    saturation_level = 1
                return saturation_level

            if len(latest_link_record_list) == 0:
                pass
            elif len(latest_link_record_list) == 1:
                link_saturation_level_string = latest_link_record_list[0]["road_saturation_level"]
                link_saturation_level.append(convert_saturation_string(link_saturation_level_string))

                link_traffic_speed.append(float(latest_link_record_list[0]["traffic_speed"]))
            else:
                # two links in the list
                first_saturation_level_string = latest_link_record_list[0]["road_saturation_level"]
                first_saturation_level = convert_saturation_string(first_saturation_level_string)
                second_saturation_level_string = latest_link_record_list[1]["road_saturation_level"]
                second_saturation_level = convert_saturation_string(second_saturation_level_string)
                link_saturation_level.append(first_saturation_level)
                link_saturation_level.append(second_saturation_level)

                first_traffic_speed = float(latest_link_record_list[0]["traffic_speed"])
                second_traffic_speed = float(latest_link_record_list[1]["traffic_speed"])
                link_traffic_speed.append(first_traffic_speed)
                link_traffic_speed.append(second_traffic_speed)

            link_dict["link_id"] = link["link_id"]
            link_dict["parent_id"] = link_id
            link_dict["length"] = link["st_length"]
            link_dict["distance"] = link["st_distance_sphere"]
            link_dict["saturation_level"] = link_saturation_level
            link_dict["traffic_speed"] = link_traffic_speed
            print(link_dict)
            link_dict_list.append(link_dict)

        client.close()
        return link_dict_list

    def query_traffic_api(self, point_longitude, point_latitude, time_second, query_gap_second, query_range=1000):
        """
        API for the traffic information nearby
        :param point_longitude: the longitude of a query point
        :param point_latitude: the latitude of a query point
        :param time_second: seconds since the epoch
        :param query_gap_second: the time gap for query
        :param query_range: search roads within a range (unit: meter)
        :return: json format data of complete traffic information
        """

        link_dict_list = []
        link_list = self.query_osm_link_list_from_point(point_longitude, point_latitude, query_range)
        if len(link_list):
            link_dict_list = self.update_latest_link_info_from_mongodb(link_list, time_second, query_gap_second)

        center = [point_longitude, point_latitude]
        json_traffic_info = {"center": center, "range": float(query_range), "time": float(time_second),
                             "gap": float(query_gap_second), "results": link_dict_list}

        return json_traffic_info


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
    # road_records = list(tsm_collection.find({"link_id": link_id}).sort([("capture_date", -1)]).limit(1))
    client.close()

    if len(road_records) == 0:
        print("No traffic speed data of this road in current database")
        return None
    else:
        return road_records


def save_link_info_json(json_dict):
    """
    Create a folder and output a JSON file of nearby traffic information
    :param json_dict: a json data of compelete query
    :return:
    """

    current_path = os.path.dirname(os.path.abspath(__file__))
    relative_path = "../../data/json/"
    json_folder_path = os.path.join(current_path, relative_path)
    if not os.path.exists(json_folder_path):
        os.makedirs(json_folder_path)
    os.chdir(json_folder_path)

    try:
        with open("area-traffic-info.json", "w") as json_file:
            json_file.write(json.dumps(json_dict))
    except IOError as error:
        print("File error: " + str(error))


if __name__ == "__main__":
    osm_query = OSMQuery()
    # distance = osm_query.query_point_distance_from_road(114.26, 22.33, 157424143)
    # print("Nearest distance between (114.26, 22.33) and the road (way-id 157424143): " + str(distance) + "\n")

    # Example
    print("Traffic information of (114.26, 22.33) within (1000) meters for the (1515167795 second since epoch): ")
    json_traffic_information = osm_query.query_traffic_api(114.20, 22.34, 1515167795, 3600, 1000)
    print(json_traffic_information)

    # save_link_info_json(json_traffic_information)
