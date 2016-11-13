"""
A wrapper class for manipulating mongodb
"""

from .document_db import DocumentDB
from pymongo import MongoClient


class MongoDB(DocumentDB):
    """
    A wrapper class for manipulating mongodb
    """
    def __init__(self, db_name, host='localhost', port=27017):
        super().__init__(db_name,host,port)
        self.__client = MongoClient(host, port)
        self.__db = self.__client[db_name]

    def create_collection(self, collection_name):
        """
        Create collection with specific name
        :param collection_name: string
        :return: None
        """
        self.__db.create_collection(collection_name)

    def collection_names(self, include_system_collections=True):
        """
        Get a list of all the collection names in this database.
        :param include_system_collections: whether includes system collections or not
        :return: a list of collection names
        """
        return self.__db.collection_names(include_system_collections)

    def drop_collection(self, collection_name):
        """
        Drop a collection with collection name
        :param collection_name:
        :return:
        """
        self.__db.drop_collection(collection_name)

    def get_collection(self, collection_name):
        """
        Get a collection by name
        :param collection_name: string
        :return: a collection instance wrapped by MongoCollection
        """
        return MongoCollection(self.__db, self.__db[collection_name])


class MongoCollection:

    def __init__(self, db, collection):
        self.__db = db
        self.__collection = collection
        self._index_names = None

    def insert_one(self, document):
        self.__collection.insert_one(document)

    def insert_many(self, documents):
        """Insert an iterable of documents
        """
        self.__collection.insert_many(documents)

    def delete_one(self, document_id):
        """Delete a single document according to its id
        """
        self.__collection.delete_one({"_id": document_id})

    def delete_many(self, query):
        """Delete all documents find by the query
        """
        self.__collection.delete_many(query)

    def delete_one_then_return(self, document_id, projection=None):
        """Deletes a single document then return the document.
        """
        return self.__collection.find_one_and_delete({"_id": document_id}, projection)

    def update_one(self, document_id, update_data, upsert=False):
        """Update a single document according to its id
        """
        self.__collection.update_one({"_id": document_id}, update_data, upsert)

    def update_one_then_return(self, document_id, update_data, upsert=False, projection=None):
        """Update a single document then return the updated document
        """
        self.__collection.find_one_and_update(
            {"_id": document_id}, update_data, projection, upsert=upsert)

    def update_many(self, query, update_data, upsert=False):
        """Update all documents find by the query
        """
        self.__collection.update_many(query, update_data, upsert)

    def replace_one(self, query, replace_data, upsert=False):
        """Replace one document find by the query
        """
        self.__collection.replace_one(query, replace_data, upsert)

    def find(self, query, projection, limit=None, skip=None, sort=None):
        """Find documents according to the query
        """
        return self.__collection.find(query, projection, skip, limit, sort=sort)

    def find_one(self, query):
        """Find one document aoccording to the query
        """
        return self.__collection.find_one(query)

    def count(self, query, limit=None, skip=None):
        """Get the number of documents in this collection.
        """
        return self.__collection.count(query, limit=limit, skip=skip)

    def distinct(self, key, query):
        """Get a list of distinct values for key among all documents in this collection.
        """
        return self.__collection.distinct(key, query)

    def create_index(self, keys, **kwargs):
        """Creates an index on this collection.
        """
        return self.__collection.create_index(keys, **kwargs)

    def drop_index(self, index_name):
        """Drops the specified index on this collection.
        """
        self.__collection.drop_index(index_name)

    def reindex(self):
        """Rebuilds all indexes on this collection.
        """
        self.__collection.reindex()

    def get_index_names(self):
        """Get the list of index name
        """
        return list(self.__collection.index_information().keys())

    def get_index(self, name):
        """Get a index according to its name
        """
        pass


if __name__ == '__main__':
    pass
