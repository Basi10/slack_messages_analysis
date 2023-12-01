import os
import json
from src.mongo_db import MongoDBHandler


def determine_collection_name(file_path):
    # Example: Use the parent directory name as the collection name
    parent_directory = os.path.basename(os.path.dirname(file_path))
    return f"{parent_directory}"

def instantiate_mongo_client():
    db = MongoDBHandler('mongodb://localhost:27017','slack_data') #Assuming we are using local host
    return db

def insert_data():
    """
    Insert data into collection
    """
    for root, dirs, files in os.walk('../anonymized/'):
        for file_name in files:
            if file_name.endswith('.json'):
                file_path = os.path.join(root, file_name)
                collection_name = determine_collection_name(file_path)
                with open(file_path, 'r') as json_file:
                    json_content = json.load(json_file)
                    if type(json_content) == list: 
                        for i in json_content:
                            client_msg = i.get('client_msg_id')
                            text = i.get('text')
                            ts = i.get('ts')
                            user = i.get('user')
                            block = i.get('blocks')

                            dictonary = {
                                 'client_msg_id': client_msg,
                                    'text': text,
                                    'ts': ts,
                                    'user': user,
                                    'blocks': block
                            }
                            channel_data = {
                                "name": collection_name,
                                "messages": dictonary
                            }
                            instantiate_mongo_client().create_document(collection_name,channel_data)

if __name__ == "__main__":
    insert_data()