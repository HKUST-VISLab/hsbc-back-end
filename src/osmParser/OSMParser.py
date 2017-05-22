from lxml import etree
from pymongo import MongoClient

CLIENT = "127.0.0.1"
DB = "map"
PORT = 27017
NODECOLLECTION = "node_test"
WAYCOLLECTION = "way_test"
HIGHWAY = 'highway'

type_config = {
    'node': {'collection': 'node'},
    'way': {'collection': 'way'},
    'highway': {'collection': 'highway'}
}


class OSMParser:
    def __init__(self, input_file):
        """
        :param input_file: The input file of OSM
        """
        self.osm_file = input_file
        self.tags = []
        self.set_tags(['node', 'way', 'highway'])
        self.blocks = {}
        self.init()

    def init(self):
        self.containers = {}
        for tag in self.tags:
            if tag != 'highway':
                self.containers[tag] = etree.iterparse(self.osm_file, events=('start', ), tag=tag)
            else:
                self.containers[tag] = etree.iterparse(self.osm_file, events=('start',), tag="way")
        print(self.containers)
    def set_tags(self, tags):
        """
        Set tags
        :param tags: the tags want to be added
        :return:
        """
        self.tags += tags
        self.tags = list(set(self.tags))

    def block_generator(self, tag):
        """
        A block generator, collection of the node with specific tag
        :param tag: the node tag in the osm, like way, node
        :return: if not reach the end, the current node will the return and the index will move to the next one,
                 if reach the end ,None will be returned
        """
        for e in self.containers[tag]:
            yield e
        yield None

    def read_element(self, tag):
        """
        Each time return an element according to the tag and the index will be move to the next element.
        If reach the last element, the return value will be None
        :param tag:
        :return: the element of tag, None for the end
        """
        if tag not in self.blocks:
            self.blocks[tag] = self.block_generator(tag)
        ele = self.blocks[tag].next()
        ele = None if ele == None else ele[1]
        return ele

    def parse_attr(self, elem):
        """
        Extract the attrib information of the node and way
        :param elem: elem, the element of lxml element
        :return: the attrs parsed from the node and way
        """
        lat, lon = None, None
        if('id' not in elem.attrib):
            print('Attrs are not matched!')
        if ('lat' in elem.attrib) and ('lon' in elem.attrib):
            lat, lon = elem.attrib['lat'], elem.attrib['lon']

        return {'id': elem.attrib['id']} if lat == None else{
            'id': elem.attrib['id'],
            'location': [elem.attrib['lat'], elem.attrib['lon']]
        }

    def parse_children(self, elem):
        """
        The children of node and way will be parsed, including way and nd
        :param elem:
        :return:
        """
        children = elem.getchildren()
        if len(children) == 0:
            return None
        tag_map = {'nd':[], 'tag': {}}
        for child in children:
            type = child.tag
            if type == 'tag':
                tag_map['tag'][child.attrib['k']] = child.attrib['v']

            if type == 'nd':
                if 'ref' not in child.attrib:
                    print('Error for node element')
                tag_map['nd'].append(child.attrib['ref'])

        return tag_map

    def dump_to_db(self, type):
        """
        Extract nodes and ways and storage them into the database
        :return: None
        """
        if type not in type_config:
            print('No this type', type)
            return
        client = MongoClient(CLIENT, PORT)
        db = client[DB]
        collection = db[type_config[type]['collection']]
        collection.remove({})

        number = 0
        if type == 'node':
            node = self.read_element('node')
            while node != None:
                if number % 10000 == 0:
                    print(number, 'of all nodes has been parsed!')
                number += 1
                node_attr = self.parse_attr(node)
                children = self.parse_children(node)
                if children != None:
                    tag = children['tag']
                    node_attr['tag'] = tag
                collection.insert(node_attr)
                node.clear()
                node = self.read_element('node')

        if type == 'way':
            way = self.read_element('way')
            while way != None:
                if number % 1000 == 0:
                    print(number, 'of all ways has been parsed!')
                way_attr = self.parse_attr(way)
                children = self.parse_children(way)
                if children != None:
                    tag = children['tag']
                    way_attr['tag'] = tag
                    nds = children['nd']
                    way_attr['nd'] = nds
                collection.insert(way_attr)
                way.clear()
                way = self.read_element('way')
                number += 1

        if type == 'highway':
            highway = self.read_element('highway')
            while highway != None:
                if number % 1000 == 0:
                    print(number, 'of all highways has been parsed!')
                number += 1
                way_attr = self.parse_attr(highway)
                children = self.parse_children(highway)

                if children != None:
                    tag = children['tag']
                    if 'highway' in tag:
                        way_attr['tag'] = tag
                        nds = children['nd']
                        way_attr['nd'] = nds
                        collection.insert(way_attr)
                highway.clear()
                highway = self.read_element('way')

        client.close()

    def dump_all_to_db(self):
        """
        Extract nodes and ways and storage them into the database
        :return: None
        """
        types = ['way', 'highway']
        for type in types:
            self.dump_to_db(type)

if __name__ == '__main__':
    parser = OSMParser('../../data/HongKong.osm')
    # parser.dump_all_to_db()
    parser.dump_to_db('highway')