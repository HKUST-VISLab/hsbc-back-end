from osmParser.OSMConfigGenerator import generate_highway_config_file
from osmParser.OSMParser import OSMParser
import osmParser.OSMConfigGenerator as g

def generate_osm_config():
    generate_highway_config_file('../data/osm_description.html', 'config/highway_config.json')

def parse_osm_to_mongo():
    parser = OSMParser('../data/HongKong.osm')
    parser.dump_all_to_db()

if __name__ == '__main__':
   # print(g.read_config('config/highway_config.json'))
    # generate_osm_config()
   parse_osm_to_mongo()