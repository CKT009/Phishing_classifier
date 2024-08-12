import sys
import os
from typing import List
import numpy as np
import pandas as pd
from pymongo import MongoClient
from src.exception import CustomException

class PhisingData:
    """
    This class helps to export entire MongoDB records as pandas dataframes.
    """

    def __init__(self, database_name: str):
        """
        Initialize the PhisingData object with database name and MongoDB URL.
        """
        try:
            self.database_name = database_name
            self.mongo_url = os.getenv("MONGO_DB_URL")

            # Check if the environment variable is set
            if not self.mongo_url:
                raise CustomException("MONGO_DB_URL environment variable is not set", sys)
        except Exception as e:
            raise CustomException(e, sys)

    def get_collection_names(self) -> List[str]:
        """
        Retrieve the collection names from the MongoDB database.
        """
        try:
            # Test MongoDB connection
            mongo_db_client = MongoClient(self.mongo_url)
            # Check if the connection is successful
            mongo_db_client.server_info()  # This will raise an exception if the connection fails
            
            collection_names = mongo_db_client[self.database_name].list_collection_names()
            return collection_names
        except Exception as e:
            raise CustomException(f"Failed to connect to MongoDB or retrieve collection names: {e}", sys)

    def get_collection_data(self, collection_name: str) -> pd.DataFrame:
        """
        Retrieve data from a specified collection and return as a pandas DataFrame.
        """
        try:
            mongo_db_client = MongoClient(self.mongo_url)
            collection = mongo_db_client[self.database_name][collection_name]
            df = pd.DataFrame(list(collection.find()))

            if "_id" in df.columns:
                df = df.drop(columns=["_id"])
            df = df.replace({"na": np.nan})
            return df
        except Exception as e:
            raise CustomException(f"Failed to retrieve data from collection '{collection_name}': {e}", sys)

    def export_collections_as_dataframe(self) -> pd.DataFrame:
        """
        Export all collections as dataframes and yield them one by one.
        """
        try:
            collections = self.get_collection_names()
            for collection_name in collections:
                df = self.get_collection_data(collection_name)
                yield collection_name, df
        except Exception as e:
            raise CustomException(e, sys)
