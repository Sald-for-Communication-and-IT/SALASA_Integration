import requests
import datetime

class Oracle_WMS_Data:
    v_host: str = ""
    v_task_params: str = ""
    v_threshold_N: int = 0
    v_threshold_Type: str = "s"
    auth_type: int = 1
    auth_user: str = ""
    auth_pass: str = ""
    auth_key: str = ""
    tGMT: int = 0

    RobotsWMS_ar: None
    RobotsWMS_ts: None
    WMS_Task_data: None
    WMS_allocation_data: None
    task_State: dict = {"itemes": 0, "page_nbr": 0,
                        "page_count": 0, "Progress": 0,
                        "Task_Total": 0,
                        "Task_Ready": 0,
                        "Task_Operated": 0,
                        "Task_Completed": 0,
                        "Task_Cancelled": 0,
                        "Error_Message": None
                        }
    allocation_State: dict = {"itemes": 0, "page_nbr": 0,
                              "page_count": 0, "Progress": 0,
                              "Error_Message": None
                             }

    Task_stateList: dict = { "Total": 0,
            "Ready":{ "Total": 0, "0": 'Created', "5": 'HELD State', "10": 'Ready'},
            "Operated":{ "Total": 0, "15": 'In Drop/Between Zones', "30": 'Processing started'}, 
            "Completed":{ "Total": 0, "90": 'Completed'},
            "Cancelled":{ "Total": 0, "99": 'Cancelled'}
    }

    def __init__(self, host, task_params, threshold_N, threshold_Type, auth_type, auth_user, auth_pass, auth_key, tGMT):
        self.v_host = host
        self.v_task_params = task_params
        self.auth_type = auth_type
        self.auth_user = auth_user
        self.auth_pass = auth_pass
        self.auth_key = auth_key        
        self.v_threshold_N = threshold_N
        self.v_threshold_Type = threshold_Type
        self.RobotsWMS_ar = []
        self.RobotsWMS_ts = None
        self.WMS_Task_data = []
        #self.task_State = ""
        self.WMS_allocation_data = []
        #self.allocation_State = ""
        self.tGMT = tGMT

    def sync_WMS_Task_processor(self):
        iRet = 1
        try:
            threshold_N = 0
            s_URI_Params = ""
            if (self.v_threshold_Type == "s"):
                threshold_N = self.v_threshold_N
            elif (self.v_threshold_Type == "m"):
                threshold_N = 60 * self.v_threshold_N
            elif (self.v_threshold_Type == "h"):
                threshold_N = 3600 * self.v_threshold_N
            if(threshold_N > 0):
                #WMS TS format: 2021-07-03T00:00:00.497373
                s_URI_Params = '&mod_ts__gt=' + (datetime.datetime.now() + datetime.timedelta(hours=self.tGMT) - datetime.timedelta(0,threshold_N)).strftime('%Y-%m-%dT%H:%M:%S.%f')
        
            s_URI_Params = self.v_task_params +s_URI_Params
            sUrl = self.v_host + '/lgfapi/v10/entity/task' + s_URI_Params

            #print(sUrl)
            arr_data = []
            self.Task_stateList["Total"] = 0
            self.Task_stateList["Ready"]["Total"] = 0
            self.Task_stateList["Operated"]["Total"] = 0
            self.Task_stateList["Completed"]["Total"] = 0
            self.Task_stateList["Cancelled"]["Total"] = 0

            headers = {'Content-Type': 'application/json'}

            def consume_task_API(sUrl):
                auth = None
                if (self.auth_type == '1'):
                    auth=( self.auth_user, self.auth_pass)
                else:
                    headers['api-key'] = self.auth_key

                request = requests.get(sUrl, auth = auth, headers = headers)
                results = request.json()
                if ("results" not in results):
                    return

                rCount=len(results['results'])
                for i in range(rCount):
                    dd ={'id': results['results'][i]['id']
                    ,'task_nbr': results['results'][i]['task_nbr']
                    ,'cart_nbr': results['results'][i]['cart_nbr']
                    ,'status_id': results['results'][i]['status_id']
                    ,'next_location': results['results'][i]['next_location_id']['key'] if results['results'][i]['next_location_id'] != None else None
                    ,'curr_location': results['results'][i]['curr_location_id']['key'] if results['results'][i]['curr_location_id'] != None else None
                    ,'location': results['results'][i]['next_location_id']['key'] if results['results'][i]['next_location_id'] != None else (results['results'][i]['curr_location_id']['key'] if results['results'][i]['curr_location_id'] != None else None)
                    ,'wave_id': results['results'][i]['wave_id']
                    ,'assigned_user': results['results'][i]['assigned_user']
                    ,'create_user': results['results'][i]['create_user']
                    ,'create_ts': datetime.datetime.strptime(results['results'][i]['create_ts'], "%Y-%m-%dT%H:%M:%S.%f%z").strftime('%Y-%m-%d %H:%M:%S.%f')
                    ,'mod_user': results['results'][i]['mod_user']
                    ,'mod_ts': datetime.datetime.strptime(results['results'][i]['mod_ts'], "%Y-%m-%dT%H:%M:%S.%f%z").strftime('%Y-%m-%d %H:%M:%S.%f')
                    ,'facility_id': results['results'][i]['facility_id']['id'] if results['results'][i]['facility_id'] != None else None
                    ,'facility': results['results'][i]['facility_id']['key'] if results['results'][i]['facility_id'] != None else None
                    ,'curr_task_type_exec_seq': results['results'][i]['curr_task_type_exec_seq']
                    ,'destination_zone': results['results'][i]['destination_zone_id']['key'] if results['results'][i]['destination_zone_id'] != None else None
                    }
                    arr_data.append(dd)
                    self.WMS_Task_data = arr_data

                    self.Task_stateList["Total"] += 1
                    _status_id = str(dd['status_id'])
                    if (_status_id in self.Task_stateList["Ready"]):
                        self.Task_stateList["Ready"]["Total"] += 1
                    elif (_status_id in self.Task_stateList["Operated"]):
                        self.Task_stateList["Operated"]["Total"] += 1
                    elif (_status_id in self.Task_stateList["Completed"]):
                        self.Task_stateList["Completed"]["Total"] += 1
                    elif (_status_id in self.Task_stateList["Cancelled"]):
                        self.Task_stateList["Cancelled"]["Total"] += 1

                    #r0 = next((item for item in self.RobotsWMS_ar if item["cart_nbr"] == dd['cart_nbr'] and len(item["cart_nbr"]) > 0), None)
                    r0 = list(filter(lambda item: item['cart_nbr'] == dd['cart_nbr'], self.RobotsWMS_ar))
                    r0 = r0[0] if len(r0)>0 else None
                    #Carfull with date cahnge may cause problems
                    if(bool(r0) and dd['mod_ts'] > r0['mod_ts']):
                        r0['id'] = dd['id']
                        r0['task_nbr'] = dd['task_nbr']
                        r0['location'] = dd['location']
                        r0['status_id'] = dd['status_id']
                        r0['mod_ts'] = dd['mod_ts']
                        if(not bool(self.RobotsWMS_ts) or dd['mod_ts'] > self.RobotsWMS_ts):
                            self.RobotsWMS_ts = dd['mod_ts']
                    elif(not bool(r0)):
                        r1 = {'id':	dd['id'],'task_nbr': dd['task_nbr'],'cart_nbr': dd['cart_nbr'],'location':	dd['location'],'status_id': dd['status_id'],'mod_ts': dd['mod_ts']}
                        self.RobotsWMS_ar.append(r1)                
                        self.RobotsWMS_ar = sorted(self.RobotsWMS_ar, key=lambda k: k['cart_nbr'])
                        if(not bool(self.RobotsWMS_ts) or dd['mod_ts'] > self.RobotsWMS_ts):
                            self.RobotsWMS_ts = dd['mod_ts']

                self.task_State = {"itemes": len(arr_data), "page_nbr": results["page_nbr"],
                "page_count": results["page_count"], "Progress": 100*int(results["page_nbr"])/int(results["page_count"]),
                "Task_Total": self.Task_stateList["Total"],
                "Task_Ready": self.Task_stateList["Ready"]["Total"],
                "Task_Operated": self.Task_stateList["Operated"]["Total"],
                "Task_Completed": self.Task_stateList["Completed"]["Total"],
                "Task_Cancelled": self.Task_stateList["Cancelled"]["Total"],
                "Error_Message": None
                }

                #self.task_State = "itemes = {}, page_nbr: {}, page_count: {}, Progress: {Prog:.2f} %".format(len(arr_data), results["page_nbr"], results["page_count"],Prog = 100*int(results["page_nbr"])/int(results["page_count"]))
                #dtt = datetime.datetime.strptime("2021-08-03T13:36:11.497373+03:00", "%Y-%m-%dT%H:%M:%S.%f%z")
                #print(dtt)
                #print(self.task_State)
                #asyncio.get_event_loop().run_until_complete(broadcast(self.task_State))
                if(results["next_page"] != None):
                    sUrl = results["next_page"]
                    consume_task_API(sUrl)

            consume_task_API(sUrl)
            self.WMS_Task_data = arr_data
            #print(len(arr_data))
            #sHTML += '<tr>'+ dd['task_nbr'] +'</th> <tr>'+ dd['cart_nbr'] +'</th> <tr>'+ dd['curr_location'] +'</th> <tr>'+ dd['mod_ts'] +'</th>'
            #return arr_data
        except Exception as e: 
            iRet = 0
            print(e)
            self.task_State["Error_Message"] = repr(e) #str(e)
        return iRet

    def sync_WMS_allocation_processor(self):
        iRet = 1
        try:
            s_URI_Params = ",".join([str(element["id"]) for element in self.WMS_Task_data])
        
            s_URI_Params = '?ordering=task_id,task_seq_nbr,create_ts&task_id__id__in=' + s_URI_Params
            sUrl = self.v_host + '/lgfapi/v10/entity/allocation' + s_URI_Params

            #print(sUrl)
            arr_data = []
            headers = {'Content-Type': 'application/json'}

            def consume_allocation_API(sUrl):
                auth = None
                if (self.auth_type == '1'):
                    auth=( self.auth_user, self.auth_pass)
                else:
                    headers['api-key'] = self.auth_key

                request = requests.get(sUrl, auth = auth, headers = headers)
                results = request.json()
                if ("results" not in results):
                    return

                rCount=len(results['results'])
                for i in range(rCount):
                    dd ={'id': results['results'][i]['id']
                    ,'create_ts': datetime.datetime.strptime(results['results'][i]['create_ts'], "%Y-%m-%dT%H:%M:%S.%f%z").strftime('%Y-%m-%d %H:%M:%S.%f')
                    ,'order_dtl_id': results['results'][i]['order_dtl_id']['id'] if results['results'][i]['order_dtl_id'] != None else None
                    ,'status_id': results['results'][i]['status_id']
                    ,'type': results['results'][i]['type_id']['key'] if results['results'][i]['type_id'] != None else None
                    ,'wave_id': results['results'][i]['wave_id']['id'] if results['results'][i]['wave_id'] != None else None
                    ,'wave_nbr': results['results'][i]['wave_id']['key'] if results['results'][i]['wave_id'] != None else None
                    ,'task_id': results['results'][i]['task_id']['id'] if results['results'][i]['task_id'] != None else None
                    ,'task_seq_nbr': results['results'][i]['task_seq_nbr']
                    ,'mhe_system_id': results['results'][i]['mhe_system_id']
                    ,'pick_user': results['results'][i]['pick_user']
                    ,'picked_ts': datetime.datetime.strptime(results['results'][i]['picked_ts'], "%Y-%m-%dT%H:%M:%S.%f%z").strftime('%Y-%m-%d %H:%M:%S.%f') if results['results'][i]['picked_ts'] != None else None
                    ,'pick_locn_str': results['results'][i]['pick_locn_str']
                    ,'is_picking_flg': results['results'][i]['is_picking_flg']
                    }
                    arr_data.append(dd)
                    self.WMS_allocation_data = arr_data

                self.allocation_State = {"itemes": len(arr_data), "page_nbr": results["page_nbr"],
                "page_count": results["page_count"], "Progress": 100*int(results["page_nbr"])/int(results["page_count"]),
                "Error_Message": None
                }
                #self.allocation_State = "itemes = {}, page_nbr: {}, page_count: {}, Progress: {Prog:.2f} %".format(len(arr_data), results["page_nbr"], results["page_count"],Prog = 100*int(results["page_nbr"])/int(results["page_count"]))
                #print(self.allocation_State)
                #asyncio.get_event_loop().run_until_complete(broadcast(self.allocation_State))
                if(results["next_page"] != None):
                    sUrl = results["next_page"]
                    consume_allocation_API(sUrl)

            consume_allocation_API(sUrl)
            self.WMS_allocation_data = arr_data
            #print(len(arr_data))
            #sHTML += '<tr>'+ dd['task_nbr'] +'</th> <tr>'+ dd['cart_nbr'] +'</th> <tr>'+ dd['curr_location'] +'</th> <tr>'+ dd['mod_ts'] +'</th>'
            #return arr_data
        except Exception as e: 
            iRet = 0
            print(e)
            self.allocation_State["Error_Message"] = repr(e)
        return iRet

    def sync_WMS_active_location(self):
        arr_data = []
        iRet = {"data":arr_data}
        try:
            #s_URI_Params = ",".join([str(element["id"]) for element in self.WMS_Task_data])
        
            s_URI_Params = '?ordering=area,-alloc_zone,locn_str&fields=id,locn_str,alloc_zone,area,type_id&area=AL1'
            sUrl = self.v_host + '/lgfapi/v10/entity/active_location' + s_URI_Params

            #print(sUrl)
            headers = {'Content-Type': 'application/json'}

            def consume_location_API(sUrl):
                auth = None
                results = {}
                try:
                    if (self.auth_type == '1'):
                        auth=( self.auth_user, self.auth_pass)
                    else:
                        headers['api-key'] = self.auth_key

                    request = requests.get(sUrl, auth = auth, headers = headers)
                    results = request.json()
                    if ("results" not in results):
                        return iRet

                    rCount=len(results['results'])
                    for i in range(rCount):
                        dd ={'id': results['results'][i]['id']
                        ,'locn_str': results['results'][i]['locn_str']
                        ,'type_id': results['results'][i]['type_id']['id'] if results['results'][i]['type_id'] != None else None
                        ,'type': results['results'][i]['type_id']['key'] if results['results'][i]['type_id'] != None else None
                        ,'area': results['results'][i]['area']
                        ,'alloc_zone': results['results'][i]['alloc_zone']
                        }
                        arr_data.append(dd)
                except Exception as e: 
                    print(e)
                finally:
                    pass

                if(results["next_page"] != None):
                    sUrl = results["next_page"]
                    consume_location_API(sUrl)

            consume_location_API(sUrl)
            iRet = {"data":arr_data}
        except Exception as e: 
            print(e)
        return iRet