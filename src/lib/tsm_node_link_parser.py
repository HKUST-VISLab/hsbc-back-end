# from geoProjector import hk1980_to_wgs84
import requests
DB = 'geo_maps'
COLLECTIONS = [{'id': 'node', 'c_name': 'hk_node'}, {'id': 'link', 'c_name': 'hk_link'}]

def hk1980_to_wgs84(north, east):
    """
    Transform hk1980 to wgs84
    :param north:
    :param east:
    :return:
    """
    r = requests.get("https://api.data.gov.hk/v1/coordinate-conversion",
                     params={'hk80-northing': north, 'hk80-easting': east})
    status_code = r.status_code
    if status_code != 200:
        print(status_code, 'not found')
        return None
    json_data = r.json()
    obj = {
        'lat': json_data['wgs84-latitude'],
        'lon': json_data['wgs84-longitude']
    }
    return [obj['lat'], obj['lon']]

def read_nodes_from_seg(seg_obj):
    """
    Extract source node and target node from one link
    :param seg_obj:
    :return: array for the two elements
    """
    p1 = hk1980_to_wgs84(float(seg_obj["Start_Node_Northings"])
                                    , float(seg_obj["Start_Node_Eastings"]))
    first_node = {
        'id': seg_obj['Start_Node'],
        'hk80': [seg_obj["Start_Node_Northings"], seg_obj["Start_Node_Eastings"]],
        'position': [p1[1], p1[0]]
    }

    p2 = hk1980_to_wgs84(float(seg_obj["End_Node_Northings"])
                                    , float(seg_obj["End_Node_Eastings"]))
    second_node = {
        'id': seg_obj['End_Node'],
        'hk80': [seg_obj["End_Node_Northings"], seg_obj["End_Node_Eastings"]],
        'position': [p2[1], p1[0]]
    }

    return [first_node, second_node]

def parser_link(link_path):
    """
    parse a the tsm node and link csv file, which can be found http://www.gov.hk/en/theme/psi/datasets/tsm_link_and_node_info_v2.xls
    :param link_path: the local csv file
    :return: {nodes, links} pair
    """
    with open(link_path, 'r') as input:
        line = input.readline()
        schemas = line.split(',')
        schemas = [schema.strip() for schema in schemas]
        schemas = ['_'.join(schema.split(' ')) for schema in schemas]
        print(schemas)
        line = input.readline()
        records = []
        nodes = []
        links = []
        node_id_map = {}
        num = 0
        while line:
            # if num > 5:
            #     break
            print(num, ' of records has been parsed')
            num += 1
            segs = line.split(',')
            segs = [seg.strip() for seg in segs]
            line = input.readline()
            obj = {}
            for i in range(0, len(segs)):
                obj[schemas[i]] = segs[i]

            records.append(obj)
            _nodes = read_nodes_from_seg(obj)
            for n in _nodes:
                if n['id'] not in node_id_map:
                    node_id_map[id] = n
                    nodes.append(n)
            links.append({
                'id': obj['Link_ID'],
                'source': obj['Start_Node'],
                'target': obj['End_Node'],
                'source_position': _nodes[0]['position'],
                'target_position': _nodes[1]['position'],
                'region': obj['Region'],
                'type': obj['Road_Type']
            })
        return {
            'node': nodes,
            'link': links
        }
def dump_tsm_node_link_to_db(path):
    """
    Dump the data into db
    :param path: local csv file, the tsm_link_and_node_info.csv
    :return:
    """
    from pymongo import MongoClient
    results = parser_link(path)
    client = MongoClient('127.0.0.1', 27017)
    db = client['geo_maps']
    for c in COLLECTIONS:
        collection = db[c['c_name']]
        collection.remove({})
        records = results[c['id']]
        collection.insert_many(records)

if __name__ == '__main__':
    result = dump_tsm_node_link_to_db('../../data/tsm_link_and_node_info.csv')

