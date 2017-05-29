"""
Helpers that build sample points of highways to assist fast query.
Notes:
Method main() build up the sample points from the existing road collection 'highway'
Method query_nearest_road() is a testing helper that queries the nearest highway given a coordinates.
See test() on the usage of query_nearest_road()
"""

from math import sqrt, sin, cos, ceil
import time

from pymongo import MongoClient, IndexModel, HASHED, GEO2D, errors

from src.osm_parser.OSMParser import DB, CLIENT, PORT, type_config


# BOUNDS = 'bounds'
SAMPLE = 'sample'
WAY = 'highway'
NODE = 'node'
LOCATION = 'location'

epsilon = 10  # delta distance in meters
R = 6371000.0  # delta distance in meters


def get_collection(db, collection_name):
    """A helper function that get collection from database using collection name.
    It proxies the OSMParser.type_config"""
    if collection_name in type_config:
        return db[type_config[collection_name]['collection']]
    else:
        return db[collection_name]


def sphere_distance(lon1, lat1, lon2, lat2):
    """Calculate the approximate distance on the earth sphere"""
    d_lon = lon1 - lon2
    x = d_lon * cos((lat1 + lat2)/2)
    y = lat1 - lat2
    return R * sqrt(x**2 + y**2)


def interpolate(x1, y1, x2, y2, x3):
    """Interpolate the y value given x3 between two points"""
    return (y2 - y1) * (x3 - x1) / (x2 - x1) + y1


def cal_y(k, b, x):
    return k * x + b


def uniform_partition(x1, y1, x2, y2, num):
    """
    Given coordinates of two points (x1, y1), (x2, y2),
    uniformly partition the segment into num parts (adding num-1 points)
    :param x1, y1: coordinates of the start point
    :param x2, y2: coordinates of the end point
    :param num: the number of partition
    :return: a generator of the partition points
    """
    if num == 0:
        return []
    dx = (x2 - x1) / (num)
    dy = (y2 - y1) / (num)
    for i in range(1, num):
        yield x1 + dx*i, y1 + dy*i


def query_node_of_road(node_collection, road):
    """
    Get the nodes from the node collection given a road record
    :param node_collection: the collection with name "node"
    :param road: a road document
    :return: a list of node documents in the same order as they stored in the road
    """
    # node_ids = [nd['ref'] for nd in road['nd']]
    nodes = []
    for node_id in road['nd']:
        result = node_collection.find_one({'id': node_id})
        assert result is not None, "no matching node {:d} of road {:d} in db!".format(node_id, road["id"])
        nodes.append(result)
    return nodes


def build_points_from_road(node_collection, road):
    """
    Build sample points of a given road.
    Note that interconnecting node are shared among several roads, we do not add them in the points index collection
    :param road:
    :return:
    """
    nodes = query_node_of_road(node_collection, road)
    points = []
    # for start, end in zip(nodes[:-1], nodes[1:]):
    for i in range(0, len(nodes) - 1):
        start = nodes[i]
        end = nodes[i + 1]
        # distance = euclid_distance(start['lon'], start['lat'], end['lon'], start['lat'])
        lon1, lat1 = start[LOCATION]
        lon2, lat2 = end[LOCATION]
        distance = sphere_distance(lon1, lat1, lon2, lat2)
        partition_num = int(distance / epsilon)
        for lon, lat in uniform_partition(lon1, lat1, lon2, lat2, partition_num):
            points.append({LOCATION: [lon, lat],  # 2d location
                           WAY: road['id'],  # the road id the location is on
                           'start_node': i  # the index of starting node of this segment on the road
                           })
    return points


def query_nearest_road(db, coordinates):
    sample_collection = db[SAMPLE]
    return sample_collection.find_one({'location': {'$near': coordinates}})


class GeoIndexBuilder(object):

    def __init__(self, buffer_size=100000):
        self.buffer_size = buffer_size
        self.buffer = []
        self.client = MongoClient(CLIENT, PORT)
        self.db = self.client[DB]
        self.road_collection = get_collection(self.db, WAY)
        self.node_collection = get_collection(self.db, NODE)
        # self.bound_collection = get_collection(self.db, BOUNDS)
        self.sample_collection = get_collection(self.db, SAMPLE)

    def __del__(self):
        self.client.close()

    # def create_index(self, field='id'):
    #     for collection in [self.road_collection, self.node_collection]:
    #         collection.create_index(field, unique=True)

    def write_points(self, points, force=False):
        self.buffer += points
        if len(self.buffer) == 0:
            return
        if force or len(self.buffer) >= self.buffer_size:
            self.sample_collection.insert_many(self.buffer)
            self.buffer = []

    def build_points(self, force=False, verbose=True):
        """
        A procedure that build up the points indexing of roads in the db
        :return: a pair (n_roads, n_points) specifying how many roads and points are built.
            If the collection is not empty and the `force` parameter is set to False, this method will return None
        """

        # Avoid duplicate build
        if self.sample_collection.find({}).count() != 0:
            print("The sample collection should be empty!")
            if force:
                print("Force to drop the sample collection...")
                self.sample_collection.drop()
            else:
                print("Set force=True if you want to rebuild the sample collection")
                return None

        verbose_freq = 100
        n_roads = n_points = 0
        # Start building
        print("Start building the sample point collection. This should take some time.")
        start_time = time.time()
        for road in self.road_collection.find({}):
            n_roads += 1
            if verbose and n_roads % verbose_freq == 0:
                print("{:d} sample points of {:d} roads have been built, time elapsed: {:.2f}s"
                      .format(n_points, n_roads, time.time() - start_time))
                start_time = time.time()
            points = build_points_from_road(self.node_collection, road)
            n_points += len(points)
            self.write_points(points)
        self.write_points([], force=True)
        return n_roads, n_points

    def create_sample_index(self):
        nodes = self.node_collection.find({})
        print('Creating 2d index for sample points...')
        nodes = [n['location'] for n in nodes]
        # print(len(nodes))
        bounds_min = min([min([n[i] for n in nodes]) - 1e-7 for i in range(2)])
        bounds_max = max([max([n[i] for n in nodes]) + 1e-7 for i in range(2)])
        print('Bounds min:', bounds_min)
        print('Bounds max:', bounds_max)
        self.sample_collection.create_index([('location', GEO2D)],
                                       min=bounds_min,
                                       max=bounds_max)
        print('2d index created.')


def main():
    indexer = GeoIndexBuilder()
    results = indexer.build_points(force=True)
    if results is not None:
        print("A total of {:d} points of {:d} roads are built.".format(results[1], results[0]))
        indexer.create_sample_index()


def test():
    indexer = GeoIndexBuilder()
    # indexer.create_sample_index()
    print('Testing query')
    p1 = [22.35678, 114.1732841]
    p2 = [22.3465615, 114.1814336]
    print('node 0 of road 157652772:', p1)
    print('node 1 of road 157652772:', p2)
    x3 = (2 * p1[0] + p2[0]) / 3.0
    y3 = interpolate(p1[0], p1[1], p2[0], p2[1], x3)
    print('querying', [x3, y3])
    print(query_nearest_road(indexer.db, [x3, y3]))


if __name__ == '__main__':
    # main()
    # query_nearest_road()
    # indexer = GeoIndexBuilder()
    # indexer.create_sample_index()
    test()
