import xml.etree.ElementTree as ET
from pymongo import MongoClient

CLIENT = "127.0.0.1"
PORT = 27017
DB = "hongkong"
POI_COLLECTION = "poi"


class POIClassifier:
    def __init__(self, input_file, poi_file):
        """
        :param input_file: The input file of OSM
        """
        self.osm_file = input_file
        self.poi_classification_file = poi_file
        self.osm_dict = {}

    def parse_POI_xml(self):
        """
        parse POI classification file and store
        """

        tree = ET.ElementTree(file=self.poi_classification_file)
        root = tree.getroot()
        # root[0] = <hierarchy> in <doc-part>
        for category_element in root[0]:
            print("Level 1: " + category_element.attrib['name'])
            for second_level_element in category_element:
                if second_level_element.tag == 'fref':
                    # OSM value in Level 2 with only 1 category
                    self.osm_dict[second_level_element.attrib['ref']] = {'category': category_element.attrib['name']}
                    print(self.osm_dict[second_level_element.attrib['ref']])

                if second_level_element.tag == 'group':
                    print("Level 2: " + second_level_element.attrib['name'])
                    for third_level_element in second_level_element:
                        # OSM value in Level 3 with 2 levels categories
                        if third_level_element.tag == 'fref':
                            self.osm_dict[third_level_element.attrib['ref']] = {
                                'category': category_element.attrib['name'],
                                'category_1': second_level_element.attrib['name']}
                            print(self.osm_dict[third_level_element.attrib['ref']])

                        if third_level_element.tag == 'group':
                            for forth_level_element in third_level_element:
                                if forth_level_element.tag == 'fref':
                                    # Ignore the third level
                                    self.osm_dict[forth_level_element.attrib['ref']] = {
                                        'category': category_element.attrib['name'],
                                        'category_1': second_level_element.attrib['name']}
                                    print(self.osm_dict[forth_level_element.attrib['ref']])

        print("Total POI values: " + str(len(self.osm_dict)))
        print(self.osm_dict)

    def parse_OSM(self):
        """
        Parse OSM and combine the POI classification
        :return: a list of nodes containing POI
        """

        node_list = []
        for event, element in ET.iterparse(self.osm_file, events=('start',)):
            if element.tag == 'node':
                tag_list = []
                for child_tag in element:
                    tag_list.append(child_tag.attrib)
                if len(tag_list):
                    node = {}
                    contain_POI = False
                    for tag_dict in tag_list:
                        if tag_dict['v'] in self.osm_dict:
                            contain_POI = True
                            # For location data
                            lat = None
                            lon = None
                            if ('lat' in element.attrib) and ('lon' in element.attrib):
                                lat = element.attrib['lat']
                                lon = element.attrib['lon']
                            # For category data
                            category = self.osm_dict[tag_dict['v']]['category']
                            category_1 = None
                            if 'category_1' in self.osm_dict[tag_dict['v']]:
                                category_1 = self.osm_dict[tag_dict['v']]['category_1']
                            # Store in a node
                            node['id'] = element.attrib['id']
                            node['osm_value'] = tag_dict['v']
                            node['category'] = category
                            node['category_1'] = category_1
                            node['version'] = element.attrib['version']
                            node['location'] = [float(lon), float(lat)]

                    if contain_POI:
                        tags = {}
                        for tag_dict in tag_list:
                            tags[tag_dict['k']] = tag_dict['v']
                        node['tags'] = tags
                        # print(node)
                        node_list.append(node)
            element.clear()
        return node_list

    def dump_to_db(self, poi_list):
        """
        Store POI nodes into the database
        """

        client = MongoClient(CLIENT, PORT)
        db = client[DB]
        collection = db[POI_COLLECTION]
        collection.remove({})

        number = 0
        buffer = []
        buffer_size = 10000
        for record in poi_list:
            number += 1
            if number % 10000 == 0:
                print(str(number) + ' of all nodes has been parsed!')

            buffer.append(record)
            if len(buffer) >= buffer_size:
                collection.insert_many(buffer)
                buffer = []

        if len(buffer) != 0:
            collection.insert_many(buffer)

        collection.create_index('id', unique=True)
        client.close()


if __name__ == '__main__':
    parser = POIClassifier('../../data/hong_kong.osm', '../../data/poi-classification.xml')
    parser.parse_POI_xml()
    poi_list = parser.parse_OSM()
    parser.dump_to_db(poi_list)
