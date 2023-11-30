from pymongo import MongoClient
from bson.objectid import ObjectId

class MongoDBHandler:
    def __init__(self, database_url, database_name):
        self.client = MongoClient(database_url)
        self.db = self.client[database_name]

    def create_document(self, collection_name, data):
        collection = self.db[collection_name]
        result = collection.insert_one(data)
        return result.inserted_id

    def read_documents(self, collection_name, query=None):
        collection = self.db[collection_name]
        if query:
            documents = collection.find(query)
        else:
            documents = collection.find()
        return list(documents)

    def read_document_by_id(self, collection_name, document_id):
        collection = self.db[collection_name]
        document = collection.find_one({"_id": ObjectId(document_id)})
        return document

    def update_document(self, collection_name, document_id, data):
        collection = self.db[collection_name]
        result = collection.update_one({"_id": ObjectId(document_id)}, {"$set": data})
        return result.modified_count

    def delete_document(self, collection_name, document_id):
        collection = self.db[collection_name]
        result = collection.delete_one({"_id": ObjectId(document_id)})
        return result.deleted_count

    def close_connection(self):
        self.client.close()


