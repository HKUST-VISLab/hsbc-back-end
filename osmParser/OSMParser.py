from lxml import etree
from pymongo import MongoClient

CLIENT = "127.0.0.1"
DB = "map"
PORT = 27017
NODECOLLECTION = "node"
WAYCOLLECTION = "way"

class OSMParser:
    def __init__(self, input_file):
        """
        :param input_file: The input file of OSM
        """
        self.osm_file = input_file
        self.tags = []
        self.set_tags(['node', 'way'])
        self.blocks = {}
        self.init()

    def init(self):
        self.containers = {}
        for tag in self.tags:
            self.containers[tag] = etree.iterparse(self.osm_file, events=('start', ), tag=tag)

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

    def dump_to_db(self):
        """
        Extract nodes and ways and storage them into the database
        :return: None
        """
        client = MongoClient(CLIENT, PORT)
        db = client[DB]
        node_colle = db[NODECOLLECTION]
        way_colle = db[WAYCOLLECTION]
        node_colle.remove({})
        way_colle.remove({})
        number = 0
        node = parser.read_element('node')
        while node != None:
            if number % 10000 == 0:
                print(number, 'of all nodes has been parsed!')
            node_attr = parser.parse_attr(node)
            children = parser.parse_children(node)
            if children != None:
                tag = children['tag']
                node_attr['tag'] = tag
            node_colle.insert(node_attr)
            node.clear()
            node = parser.read_element('node')
            number += 1

        way = parser.read_element('way')
        number = 0
        while way != None:
            if number % 1000 == 0:
                print(number, 'of all ways has been parsed!')
            way_attr = parser.parse_attr(way)
            children = parser.parse_children(way)
            if children != None:
                tag = children['tag']
                way_attr['tag'] = tag
                nds = children['nd']
                way_attr['nd'] = nds
            way_colle.insert(way_attr)
            way.clear()
            way = parser.read_element('way')
            number += 1

if __name__ == '__main__':
    parser = OSMParser('HongKong.osm')
    parser.dump_to_db()
