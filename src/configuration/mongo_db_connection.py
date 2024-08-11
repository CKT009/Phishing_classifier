import sys
import os

import certifi
import pymongo

from src.constant import *
from src.exception import CustomException

ca = certifi.where()

class MongoClient():
    client = None
    
    def __init__(self, database_name=MONGO_DATABASE_NAME)-> None:
        try:
            if MongoClient.client is None:
                mongo_db_url = os.getenv("MONGP_DB_URL")
                if mongo_db_url is None:
                    raise Exception("Enviorment key: MONGO_DB_URL is not set.")
                
                MongoClient.client = pymongo.MongoClient(mongo_db_url, tlsCAFiles=ca)
            self.client = MongoClient.client
            self.database = self.client(database_name)
            self.database_name = database_name
            
        except Exception as e:
            raise CustomException(e, sys)