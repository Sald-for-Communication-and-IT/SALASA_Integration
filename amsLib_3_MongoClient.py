from pymongo import MongoClient
from random import randint

class Mongo_Client:
    db: None
    
    def __init__(self):
        #Step 1: Connect to MongoDB - Note: Change connection string as needed
        #client = MongoClient(port=27017)
        #self.db=client.integration
        client = MongoClient("mongodb://ams:asdasd@cluster0-shard-00-00.6jgxe.mongodb.net:27017,cluster0-shard-00-01.6jgxe.mongodb.net:27017,cluster0-shard-00-02.6jgxe.mongodb.net:27017/myFirstDatabase?ssl=true&replicaSet=atlas-vp4p7q-shard-0&authSource=admin&retryWrites=true&w=majority")
        self.db = client.ws

    def get_dbsetting(self):
        arr_setting = []
        rows = self.db.setting.find()
        for row in rows:
            arr_setting.append({"id": row["id"], "key": row["key"], "data": row["data"]})
        return arr_setting

    def update_DB_key(self,db_Key, db_data):
        myquery = { "key": db_Key }
        newvalues = { "$set": { "data": db_data } }
        self.db.setting.update_one(myquery, newvalues)

    def get_max_val(self,strTable,strKey):
        #max_mod_ts = self.db.RobotsWMS_Log.find({},{"_id":0 ,"mod_ts":1}).sort("mod_ts", -1).limit(1)
        tbl = self.db.RobotsWMS_Log
        if (strTable.lower() == "allocation_Log".lower()):
            tbl = self.db.allocation_Log
        max_mod_ts = tbl.find_one(sort=[(strKey, -1)])
        if(bool(max_mod_ts)):
            max_mod_ts = max_mod_ts[strKey]
        return max_mod_ts
        
    def get_RobotsWMS_Log(self):
        arr_data = []
        rows = self.db.RobotsWMS_Log.find()
        for row in rows:
            arr_data.append({"ID_N": row["ID_N"], "id": row["id"], "task_nbr": row["task_nbr"], "cart_nbr": row['cart_nbr'], "location": row["location"], "status_id": row["status_id"], "mod_ts": row["mod_ts"], "ts": row["ts"]})
        return arr_data

    def get_allocation_Log(self):
        arr_data = []
        rows = self.db.allocation_Log.find()
        for row in rows:
            arr_data.append({"FK_N": row["FK_N"], "id": row["id"], "create_ts": row["create_ts"], "order_dtl_id": row["order_dtl_id"], "status_id": row["status_id"], "type": row["type"], "wave_id": row["wave_id"], "wave_nbr": row["wave_nbr"], "task_id": row["task_id"], "task_seq_nbr": row["task_seq_nbr"], "mhe_system_id": row["mhe_system_id"], "pick_user": row["pick_user"], "picked_ts": row["picked_ts"], "pick_locn_str": row["pick_locn_str"], "is_picking_flg": row["is_picking_flg"], "ts": row["ts"]})
        return arr_data

    def insert_RobotsWMS_Log(self,mydict):
        x = self.db.RobotsWMS_Log.insert_one(mydict)
        return x

    def insert_allocation_Log(self,mydict):
        x = self.db.allocation_Log.insert_one(mydict)
        return x

    ################################ Revision ######################################################
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
    ###########################################################################################