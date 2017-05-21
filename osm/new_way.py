# change the data format in way collection

import copy
from pymongo import MongoClient
def main():
    c = MongoClient("localhost", 27017)
    db = c['hongkong']
    org_collection = db['way_orgi']
    new_collection = db['way']
    for way in org_collection.find():
        new_way = copy.deepcopy(way)

        new_way['nd'] = []
        for nd in way['nd']:
            new_way['nd'].append(nd['ref'])

        # extract everything in tag and delete "tag"
        try:
            tags = way['tag']
            new_way.pop('tag')
        except KeyError:
            tags = []
        for tag in tags:
            new_way[tag['k']] = tag['v']

        # only extract name from tag
        # try:
        #     tags = way['tag']
        # except KeyError:
        #     tags = []
        # for tag in tags:
        #     if tag['k'] == 'name':
        #         new_way[tag['k']] = tag['v']

        new_collection.insert_one(new_way)

if __name__ == "__main__":
    main()
