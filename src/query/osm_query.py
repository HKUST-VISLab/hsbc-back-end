"""
A class for querying OSM data from PostGIS database
"""
import psycopg2
from psycopg2 import extras

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
        :param
        point_latitude: the latitude of a query point
        point_longitude: the longitude of a query point
        way_id: the way_id(osm_id) of the road
        dict_cursor: if use dict data format for the query result
        print_sql: if print the sql code
        :return: the nearest distance between a point a the road or None
        """
        try:
            # Create connection to ices database
            postgresql_connection = psycopg2.connect(dbname=self.database, host=self.host, port=self.port,
                                                     user=self.user, password=self.password)
        except psycopg2.OperationalError as e:
            print("dbname=" + self.database + ", host=" + self.host + ", port=" + self.port
                  + " , user=" + self.user + ", password=" + self.password)
            print("OperationalError: " + e)
            return None

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
            print("DatabaseError: " + e)

        result = cursor.fetchone()
        cursor.close()
        postgresql_connection.close()

        # Return the distance
        return result['st_distance_sphere']

    def query_road_list_from_point(self, point_longitude, point_latitude, distance, print_sql=False):
        """
        Get the nearest distance between a point and a road
        :param
        point_latitude: the latitude of a query point
        point_longitude: the longitude of a query point
        distance: search roads within a distance (unit: meter)
        dict_cursor: if use dict data format for the query result
        print_sql: if print the sql code
        :return: None
        """
        try:
            # Create connection to ices database
            postgresql_connection = psycopg2.connect(dbname=self.database, host=self.host, port=self.port,
                                                     user=self.user, password=self.password)
        except psycopg2.OperationalError as e:
            print("dbname=" + self.database + ", host=" + self.host + ", port=" + self.port
                  + " , user=" + self.user + ", password=" + self.password)
            print("OperationalError: " + e)
            return None

        # Create a cursor object using dict output format
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
            print("DatabaseError: " + e)

        result = cursor.fetchall()
        cursor.close()
        postgresql_connection.close()

        # Return the distance
        return result


if __name__ == '__main__':
    osm_query = OSMQuery()

    distance = osm_query.query_point_distance_from_road(114.26, 22.33, 157424143)
    print("Nearest distance between (114.26, 22.33) and the road (way-id 157424143): " + str(distance) + "\n")

    road_list =osm_query.query_road_list_from_point(114.26, 22.33, 500)
    for road in road_list:
        print(road)
