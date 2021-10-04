from pymongo import MongoClient
from random import randint

import hashlib
import certifi

class Mongo_Client:
    db: None
    
    def __init__(self):
        #Step 1: Connect to MongoDB - Note: Change connection string as needed
        #client = MongoClient(port=27017)
        #self.db=client.integration
        client = MongoClient("mongodb+srv://ams:asdasd@cluster0.6jgxe.mongodb.net/test?authSource=admin&replicaSet=atlas-vp4p7q-shard-0&readPreference=primary&appname=MongoDB%20Compass&ssl=true",tlsCAFile=certifi.where())
        #client = MongoClient(host='mongodb+srv://cluster0.6jgxe.mongodb.net', port=27017, connect=True,username="ams", password="asdasd")
        #client = MongoClient("mongodb://ams:asdasd@cluster0-shard-00-00.6jgxe.mongodb.net:27017,cluster0-shard-00-01.6jgxe.mongodb.net:27017,cluster0-shard-00-02.6jgxe.mongodb.net:27017/myFirstDatabase?ssl=true&replicaSet=atlas-vp4p7q-shard-0&authSource=admin&retryWrites=true&w=majority")
        #client = MongoClient("mongodb://ams:asdasd@cluster0.6jgxe.mongodb.net/test?authSource=admin&replicaSet=atlas-vp4p7q-shard-0&readPreference=primary&appname=MongoDB%20Compass&ssl=true")
        self.db = client.ws

    #######################################
    def hash_password(self,password):
        hash_object = hashlib.md5(password.encode())
        return '$' + hash_object.hexdigest()


    def Compare_password(self,password,hsah_password):
        hash_object = hashlib.md5(password.encode())
        return (hsah_password == '$' + hash_object.hexdigest())
    #######################################

    def get_users(self,sEmail:str = None,iID:int = None):
        arr_users = []
        myquery = { "id": iID } if iID != None else ({ "Email": sEmail } if sEmail != None else {})
        rows = self.db.users.find(myquery)
        for row in rows:
            arr_users.append({"id": row["id"], "Role": row["Role"], "Email": row["Email"], "First Name": row["First Name"], "Last Name": row["Last Name"], "Password": row["Password"]})
        return arr_users
    
    def delete_users(self,sEmail:str = None,iID:int = None):
        x = 1
        myquery = { "Email": sEmail } if sEmail != None else ({ "id": iID } if iID != None else {})
        rows = self.db.users.remove(myquery)
        #x = rows.deleted_count
        return x

    def upsert_users(self,dUser:dict):
        iret = 1
        try:
            id = dUser["id"]
            if("Email" in dUser):
                dUser["Email"] = dUser["Email"].lower()
            if("Password" in dUser):
                dUser["Password"] = self.hash_password(dUser["Password"])

            sKeys = ('id', 'Role','Email','First Name','Last Name','Password')

            if (id == -1):
                id = 1 + self.get_max_val("users","id")
                dUser["id"] = id
                if all(name in sKeys for name in dUser) and all(name in dUser for name in sKeys):
                    user_dict = {"id": dUser["id"], "Role": dUser["Role"], "Email": dUser["Email"], "First Name": dUser["First Name"], "Last Name": dUser["Last Name"], "Password": dUser["Password"]}
                    x = self.db.users.insert_one(user_dict)
                else:
                    iRet = 0
            else:
                if all(name in sKeys for name in dUser):
                    myquery = { "id": id }
                    newvalues = { "$set": dUser }
                    self.db.users.update_one(myquery, newvalues)
                else:
                    iRet = 0
        except Exception as e: 
            iRet = 0
        return iret

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
        """
        tbl = self.db.RobotsWMS_Log
        if (strTable.lower() == "allocation_Log".lower()):
            tbl = self.db.allocation_Log
        elif (strTable.lower() == "users".lower()):
            tbl = self.db.users
        else:
        """
        tbl = self.db[strTable]

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