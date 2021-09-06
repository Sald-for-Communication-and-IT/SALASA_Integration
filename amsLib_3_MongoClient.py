from pymongo import MongoClient
from random import randint

class Mongo_Client:
    db: None
    
    def __init__(self):
        #Step 1: Connect to MongoDB - Note: Change connection string as needed
        client = MongoClient(port=27017)
        self.db=client.integration

    def sync_Insert_RobotsWMS_Data(self, jsonData):
        x = self.db.RobotsWMS.delete_many({})
        if isinstance(jsonData,list) and len(jsonData) > 0:
            result=self.db.RobotsWMS.insert_many(jsonData)
        else:
            result=self.db.RobotsWMS.insert_one({},jsonData, upsert=True)
    
    def sync_Insert_WMS_Task_data(self, jsonData):
        x = self.db.WMStask.delete_many({})
        #print(x.deleted_count, " documents deleted.")
        if isinstance(jsonData,list) and len(jsonData) > 0:
            result=self.db.WMStask.insert_many(jsonData)
            #print(len(result.inserted_ids), " documents inserted_count.")
        else:
            result=self.db.WMStask.insert_one(jsonData)
            #print(result.inserted_id, " documents inserted_ID.")

    def sync_Insert_WMS_State_data(self, jsonData):
        x = self.db.WMSstate.delete_many({})
        if isinstance(jsonData,list) and len(jsonData) > 0:
            result=self.db.WMSstate.insert_many(jsonData)
        else:
            result=self.db.WMSstate.insert_one(True,jsonData, upsert=True)

    def sync_Insert_RobotsFMS_data(self, jsonData):
        x = self.db.RobotsFMS.delete_many({})
        if isinstance(jsonData,list) and len(jsonData) > 0:
            result=self.db.RobotsFMS.insert_many(jsonData)
        else:
            result=self.db.RobotsFMS.insert_one(True,jsonData, upsert=True)
