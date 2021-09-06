import requests
import datetime

class Milvus_FMS_Data:
    v_host: str = ""
    v_robots_params: str = ""
    auth_type: int = 1
    auth_user: str = ""
    auth_pass: str = ""
    auth_key: str = ""
    RobotsFMS_ar: None
    RobotsFMS_ts: None
    Robots_State: dict = {"itemes": 0, "page_nbr": 0,
                        "page_count": 0, "Progress": 0,
                        "Robots_Total": 0,
                        "Robots_Operational": 0,
                        "Robots_Offline": 0,
                        "Robots_Ready": 0,
                        "Robots_On_Missions": 0,
                        "Error_Message": None
                        }
    Robots_State_0: None


    def __init__(self, host, robots_params, auth_type, auth_user, auth_pass, auth_key):
        self.v_host = host
        self.v_robots_params = robots_params
        self.auth_type = auth_type
        self.auth_user = auth_user
        self.auth_pass = auth_pass
        self.auth_key = auth_key
        self.RobotsFMS_ar = []
        self.RobotsFMS_ts = None
        self.Robots_State_0 = self.Robots_State
        
    def sync_FMS_Robots_processor(self):
        iRet = 1
        s_URI_Params = ""
        try:
            s_URI_Params = self.v_robots_params +s_URI_Params
            sUrl = self.v_host + '/robots' + s_URI_Params
        
            headers = {'Content-Type': 'application/json'}

            arr_data = []
            self.Robots_State_0["Robots_Total"] = 0
            self.Robots_State_0["Robots_Operational"] = 0
            self.Robots_State_0["Robots_Offline"] = 0
            self.Robots_State_0["Robots_Ready"] = 0
            self.Robots_State_0["Robots_On_Missions"] = 0
            self.Robots_State_0["Error_Message"] = None

            def consume_task_API(sUrl):
                auth = None
                if (self.auth_type == '1'):
                    auth=( self.auth_user, self.auth_pass)
                else:
                    headers['x-api-key'] = self.auth_key

                request = requests.get(sUrl, auth = auth, headers = headers)
                rData = request.json()
                if ("data" not in rData):
                    return

                idata=len(rData['data'])
                for i in range(idata):
                    dd ={'id': rData['data'][i]['_id']
                    ,'name': rData['data'][i]['name']
                    ,'updatedAt': datetime.datetime.strptime(rData['data'][i]['updatedAt'], "%Y-%m-%dT%H:%M:%S.%fz").strftime('%Y-%m-%d %H:%M:%S.%f')
                    ,'connected': rData['data'][i]['onboardData']['connected'] if(rData['data'][i]['onboardData'] != None) else False
                    ,'localized': rData['data'][i]['onboardData']['localized'] if(rData['data'][i]['onboardData'] != None) else False
                    ,'operational': rData['data'][i]['onboardData']['operational'] if(rData['data'][i]['onboardData'] != None) else False
                    ,'onMission': rData['data'][i]['onboardData']['onMission'] if(rData['data'][i]['onboardData'] != None) else False
                    ,'jobId': rData['data'][i]['onboardData']['jobId'] if(rData['data'][i]['onboardData'] != None) else None
                    ,'pose_x': rData['data'][i]['onboardData']['pose']['x'] if(rData['data'][i]['onboardData'] != None and rData['data'][i]['onboardData']['pose'] != None) else 0
                    ,'pose_y': rData['data'][i]['onboardData']['pose']['y'] if(rData['data'][i]['onboardData'] != None and rData['data'][i]['onboardData']['pose'] != None) else 0
                    ,'pose_yaw': rData['data'][i]['onboardData']['pose']['yaw'] if(rData['data'][i]['onboardData'] != None and rData['data'][i]['onboardData']['pose'] != None) else 0
                    }
                    arr_data.append(dd)
                    if(not bool(self.RobotsFMS_ts) or dd['updatedAt'] > self.RobotsFMS_ts):
                        self.RobotsFMS_ts = dd['updatedAt']

                    self.Robots_State_0["Robots_Total"] += 1
                    if (bool(dd['connected']) and bool(dd['operational'])):
                        self.Robots_State_0["Robots_Operational"] += 1
                    if (not bool(dd['connected']) or  not bool(dd['operational'])):
                        self.Robots_State_0["Robots_Offline"] += 1
                    if (bool(dd['operational']) and not bool(dd['onMission'])):
                        self.Robots_State_0["Robots_Ready"] += 1
                    if (bool(dd['operational']) and bool(dd['onMission'])):
                        self.Robots_State_0["Robots_On_Missions"] += 1

                self.Robots_State_0["itemes"] = len(arr_data),
                self.Robots_State_0["page_nbr"] = 1 #rData["page_nbr"]
                self.Robots_State_0["page_count"] = 1 #rData["page_count"]
                self.Robots_State_0["Progress"] = 100 #100*int(rData["page_nbr"])/int(rData["page_count"])

            consume_task_API(sUrl)
            self.RobotsFMS_ar = arr_data
            self.Robots_State = self.Robots_State_0
            #print(("Robots# {},\t {}").format(self.RobotsFMS_ar,len(self.RobotsFMS_ar)))
        except Exception as e: 
            iRet = 0
            self.Robots_State["Error_Message"] = str(e)
            print(e)
        return iRet