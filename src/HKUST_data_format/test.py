HOST = '127.0.0.1'
PORT = 27017
DB = 'air_quality_model_hkust'


def insert_into_mongo(records):
    from pymongo import MongoClient
    client = MongoClient(HOST, PORT)
    db = client[DB]
    collection = db['air_quality_model_hkust']
    for record in records:
        time = record['time'] if 'time' in record else None
        station_id = record['station_id'] if 'station_id' in record else None

        if (not time) or (not station_id):
            continue
        value = record['value'] if 'value' in record else None
        collection.find_one_and_update({'time': time,
                                        'station_id': station_id,
                                        'value.a': 1},
                                       {'$set': {'value.c': value}},
                                       upsert=True)

if __name__ == '__main__':
    pass
    # insert_into_mongo([{
    #     'time': 1,
    #     'station_id': 1,
    #     'value': 123
    #
    # }, {
    #     'time': 2,
    #     'station_id': 1,
    #     'value': {
    #         'a': 1,
    #         'b': 'y'
    #     }
    # }, {
    #     'time': 3,
    #     'station_id': 1
    # }, {
    #     'time': 4,
    #     'station_id': 1
    # }])