# read the HongKong.osm and write it to mongodb
# wqw 2017-05-16 
import xml.etree.ElementTree as ET
from pymongo import MongoClient

c = MongoClient("localhost", 27017)
db = c['hongkong']

def level_one(child):
    tag = child.tag
    collection = db[tag]
    attr = child.attrib
    for grandchild in child:
        level_two(attr, grandchild)
    collection.insert_one(attr)

def level_two(attr, grandchild):
    try:
        attr[grandchild.tag].append(grandchild.attrib)
    except KeyError:
        attr.update({grandchild.tag: [] })
        attr[grandchild.tag].append(grandchild.attrib)

if __name__=="__main__":
    tree = ET.parse('HongKong.osm')
    root = tree.getroot()
    for child in root:
        level_one(child)
    print('done')
