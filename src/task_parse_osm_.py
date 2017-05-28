from src.osm_parser.OSMConfigGenerator import generate_highway_config_file
from src.osm_parser.OSMParser import OSMParser
import src.osm_parser.OSMConfigGenerator as g

def generate_osm_config():
    generate_highway_config_file('../data/osm_description.html', 'config/highway_config.json')

def parse_osm_to_mongo():
    parser = OSMParser('../data/HongKong.osm')
    parser.dump_all_to_db()

if __name__ == '__main__':
   parse_osm_to_mongo()