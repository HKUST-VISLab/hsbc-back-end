from src.osmParser.OSMConfigGenerator import generate_highway_config_file
from src.OSMParser.OSMParser import OSMParser


def generate_osm_config():
    generate_highway_config_file('data/osm_description.html', 'config/highway_config.json')

def parse_osm_to_mongo():
    parser = OSMParser('data/HongKong.osm')
    parser.dump_to_db()

if __name__ == '__main__':
    generate_osm_config()