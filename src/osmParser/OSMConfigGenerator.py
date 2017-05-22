from lxml import etree
import urllib
import json
from bs4 import BeautifulSoup
types = ['Roads', "Link roads"]

group_replace_table = {
    "These are the principal tags for the road network. They range from the most to least important.": "principal",
    "Link roads": "link",
    "Special road types": "special",
    "When cycleway is drawn as its own way (see Bicycle)": "cycleway",
    "Other highway features": "others"
}

def readTestFile(path = "test.html"):
    with open(path) as input:
        context = input.read()
        return context

def parse_osm_config(config_path):
    """
    Generate config file from the html
    :param config_path: http://wiki.openstreetmap.org/wiki/Key:highway, which is download to data file
    :return:
    """
    highway_config = {}
    text = readTestFile(config_path)
    soup = BeautifulSoup(text, 'lxml')
    tables = soup.findAll('table')
    current_group = None
    importance = 0
    for table in tables:

        trs = table.findAll('tr')
        for tr in trs:
            # If th exists in tr, this tr is type
            ths = tr.findAll('th')
            if len(ths) != 0:
                if len(ths) == 1:
                    current_group = ths[0].text.strip()
                    current_group = group_replace_table[current_group] if current_group in group_replace_table else current_group
                    importance = 0
                continue

            # If td exists in tr, this tr is context
            tds = tr.findAll('td')
            highways = [td.text.strip() for td in tds]
            if len(highways) >= 4 and highways[0] == "highway":
                highway_config[highways[1]] = {
                    "description": highways[3],
                    "level": importance,
                    'group': current_group
                }
                importance += 1

    return highway_config

def generate_highway_config_file(input_html, output_json):
    """
    This is a function to generate highway config file which is json format
    :return: no return
    """
    highway_config = parse_osm_config(input_html)
    with open(output_json, 'w') as output:
        json.dump(highway_config, output)

if __name__ == '__main__':
    pass
