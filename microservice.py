import asyncio
from asyncio.tasks import sleep
from typing import Optional
from hypercorn.config import Config
from hypercorn.asyncio import serve
from datetime import datetime, time, timedelta

from json import dumps
import os
#from fastapi.routing import APIRoute
#from quart import abort, flash, g, Quart, redirect, render_template, request, session, url_for, websocket

from fastapi import FastAPI, Request, Response
from fastapi.params import Form
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import JSONResponse,RedirectResponse,HTMLResponse

#from sqlite3 import dbapi2 as sqlite3

import requests
from starlette.types import Message

from amsLib_1_Oracle_WMS_Data import Oracle_WMS_Data
from amsLib_2_Milvus_FMS_Data import Milvus_FMS_Data
from amsLib_3_MongoClient import Mongo_Client
from amsLib_4_GeoTagGenerator import GeoTagGenerator

#app = FastAPI(root_path="/templates")
app = FastAPI()
app.mount("/templates", StaticFiles(directory= os.path.dirname(os.path.abspath(__file__)) + '/' +'templates', html = True))
"""
app.include_router(
    APIRoute(path="/templates/",endpoint=app),
    prefix="/api",
)
"""
templates = Jinja2Templates(directory=os.path.dirname(os.path.abspath(__file__)) + '/' +'templates')

app_config = {
    #'DATABASE': os.path.dirname(os.path.abspath(__file__)) + '/' + 'ws.db',
    'DEBUG': True,
    'SECRET_KEY': 'development key',
    'USERNAME': 'admin',
    'PASSWORD': 'admin',
}

#######################################
settings_data = {}
WMS_API_sleep = 1
porcess_i = 0
__Message = ""

obj_WMS = None
obj_FMS = None
tGMT = 0
ws_Days = {"0": 1, "1": 1, "2": 1, "3": 1, "4": 1, "5": 1, "6": 1}
ws_Time = {"From": "00:00", "To": "00:00"}
objMongoClient = Mongo_Client()
WMS_RobotsWMS_ts = str(datetime(2000, 1, 1))
#FMS_RobotsFMS_ts = str(datetime(2000, 1, 1))
Service_State = {"start": False, "cancle": True, "start_ts": (datetime.now() + timedelta(hours=tGMT)).strftime('%Y-%m-%d %H:%M:%S.%f'), "duration": time().strftime('%m-%d %H:%M:%S.%f'), "timestamp": (datetime.now() + timedelta(hours=tGMT)).strftime('%Y-%m-%d %H:%M:%S.%f'), "response": time().strftime('%H:%M:%S.%f'), "WMS_Connected": False, "FMS_Connected": False}
#######################################

#######################################
def get_keyData(sKey):
    return list(filter(lambda item: item['key'] == sKey, settings_data))[0]['data']

#######################################
#@app.before_serving
@app.on_event("startup")
async def create_job():
    Service_State['start'] = True
    #asyncio.ensure_future(Run_Dummy_Service())
    asyncio.ensure_future(Run_Service())
    #print("startup")
            
async def Run_Dummy_Service():
    # Runs in this event loop
    while True:
        try:
            #print("Run_Dummy_Service")
            request = requests.get('https://amsherokudummy2021.herokuapp.com')
            results = request.json()
            #print(results)
        finally:
            z = 0
        await sleep(60)

def check_WS_DateTime(ws_Days, ws_Time):
    Dict_days = {"0": 1, "1": 1, "2": 1, "3": 1, "4": 1, "5": 1, "6": 1}
    Dict_Times = {"From": "00:00", "To": "00:00"}
    ret = True
    t_Now = datetime.today()+ timedelta(hours=tGMT)
    if(ws_Days != Dict_days):
        iDay = str(t_Now.weekday())
        ret = (ws_Days[iDay] == 1)
    if(ws_Time != Dict_Times):
        ret = (t_Now.time().strftime("%H:%M") >= ws_Time["From"] and t_Now.time().strftime("%H:%M") <= ws_Time["To"])
    return ret

async def Run_Service():
    # Runs in this event loop
    global settings_data,WMS_API_sleep, porcess_i, Service_State, tGMT,ws_Time,ws_Days
    global obj_WMS, obj_FMS, obj_DB_Client, WMS_RobotsWMS_ts
    settings_data = objMongoClient.get_dbsetting()

    #print("Run_Service")
    try:
        host = get_keyData('wms_host')
        task_params = "?" + get_keyData('wms_task_params')
        threshold_N = int(get_keyData('wms_threshold'))
        threshold_Type = get_keyData('wms_threshold_type')
        WMS_API_sleep = int(get_keyData('wms_ws_sleep'))
        wms_auth_type = get_keyData('wms_auth_type')
        wms_auth_user = get_keyData('wms_auth_user')
        wms_auth_pass = get_keyData('wms_auth_pass')
        wms_auth_key = get_keyData('wms_auth_key')

        fms_host = get_keyData('fms_host')
        fms_Robots_params = "?" + get_keyData('fms_Robots_params')
        fms_auth_type = get_keyData('fms_auth_type')
        fms_auth_user = get_keyData('fms_auth_user')
        fms_auth_pass = get_keyData('fms_auth_pass')
        fms_auth_key = get_keyData('fms_auth_key')

        tGMT = int(get_keyData('t_gmt'))
        ws_Days = get_keyData('ws_Days')
        ws_Time = get_keyData('ws_Time')

        obj_WMS = Oracle_WMS_Data(host,task_params,threshold_N,threshold_Type, wms_auth_type, wms_auth_user, wms_auth_pass, wms_auth_key, 0)
        obj_FMS =  Milvus_FMS_Data(fms_host,fms_Robots_params, fms_auth_type, fms_auth_user, fms_auth_pass, fms_auth_key)

        max_mod_ts = objMongoClient.get_max_val("RobotsWMS_Log","mod_ts")
        if(bool(max_mod_ts)):
            WMS_RobotsWMS_ts = max_mod_ts

        Service_State['start_ts'] = (datetime.now() + timedelta(hours=tGMT)).strftime('%Y-%m-%d %H:%M:%S.%f')

        if(check_WS_DateTime(ws_Days, ws_Time)):
            await async_FMS_Robots_processor()
        #Service_State['cancle'] = False
        tTime: datetime

        while Service_State['start']:
            tStart = datetime.now() + timedelta(hours=tGMT)
            if(obj_WMS.task_State != '?' and check_WS_DateTime(ws_Days, ws_Time)):
                Service_State['cancle'] = False
                #porcess_i = porcess_i + 1
                await async_WMS_Task_processor()
                #obj_DB_Client.sync_Insert_WMS_Task_data(obj_WMS.WMS_Task_data)
                #print(("i: {}, sleep: {},data: {}").format(porcess_i,WMS_API_sleep,obj_WMS.WMS_Task_data))
                if(bool(WMS_API_sleep)):
                    await sleep(WMS_API_sleep)
                asyncio.ensure_future(async_FMS_Robots_processor())
                    #async_FMS_Robots_processor()
                tTime = datetime.now() + timedelta(hours=tGMT)
                Service_State["duration"] = str(tTime - datetime.strptime(Service_State["start_ts"], '%Y-%m-%d %H:%M:%S.%f'))
                Service_State["response"] =  str(tTime - tStart)
                Service_State["RobotsWMS_ts"] = WMS_RobotsWMS_ts
                Service_State["RobotsFMS_ts"] = obj_FMS.RobotsFMS_ts
            else:
                tTime = datetime.now() + timedelta(hours=tGMT)
                Service_State['cancle'] = True
                await sleep(1)
            Service_State["timestamp"] = tTime.strftime('%Y-%m-%d %H:%M:%S.%f')
    finally:
        Service_State['start'] = False
        Service_State['cancle'] = True

@app.route('/page-login', methods=['GET', 'POST'])
@app.route('/page-login.html', methods=['GET', 'POST'])
async def login(request: Request):
    error = None
    response = JSONResponse()
    if request.method == 'POST':
        form = await request.form()
        #print("{}, {}".format(form['username'],form['password']))
        Dict_User = objMongoClient.get_users(form['username'])
        if len(Dict_User) == 0 or str(form['username']).lower() != Dict_User[0]["Email"]:
            error = 'Invalid username'
        elif not objMongoClient.Compare_password(form['password'],Dict_User[0]["Password"]):
            error = 'Invalid password'
        else:
            rr = RedirectResponse(app.url_path_for("index"), status_code=303)
            rr.set_cookie(key="logged_in", value="True")
            rr.headers["X-Auth1"] = "{}/{}".format(form['username'],form['password'])
            rr.headers.append(key="X-Auth2",value="{}/{}".format(form['username'],form['password']))
            return rr
    return templates.TemplateResponse("page-login.html", {"request": request, "response": response, "error": error})

@app.route('/')
@app.route("/index")
@app.route("/index.html")
async def index(request: Request):
    r = None
    #print(request.headers._list)
    if not ("logged_in" in request.cookies and bool(request.cookies['logged_in'])):
        r = RedirectResponse(app.url_path_for("login"), status_code=302)
    else:
        context = {"request": request}
        r = templates.TemplateResponse("index.html", context)
    return r

@app.route('/settings')
@app.route('/settings.html')
async def settings(request: Request):
    r = None
    if not ("logged_in" in request.cookies and bool(request.cookies['logged_in'])):
        r = RedirectResponse(app.url_path_for("login"), status_code=302)
    else:
        data = {"wms_host": get_keyData("wms_host"), "wms_task_params": get_keyData("wms_task_params"), "wms_threshold": get_keyData("wms_threshold"), 
                "wms_threshold_type": get_keyData("wms_threshold_type"), "wms_ws_sleep": get_keyData("wms_ws_sleep"), "wms_auth_type": get_keyData("wms_auth_type"), 
                "wms_auth_user": get_keyData("wms_auth_user"), "wms_auth_pass": get_keyData("wms_auth_pass"), "wms_auth_key": get_keyData("wms_auth_key"), 
                "fms_host": get_keyData("fms_host"), "fms_Robots_params": get_keyData("fms_Robots_params"), "fms_auth_type": get_keyData("fms_auth_type"), 
                "fms_auth_user": get_keyData("fms_auth_user"), "fms_auth_pass": get_keyData("fms_auth_pass"), "fms_auth_key": get_keyData("fms_auth_key"),
                "t_gmt": int(get_keyData("t_gmt")), "ws_Days": get_keyData("ws_Days"), "ws_Time": get_keyData("ws_Time")}

        context = {"request": request, "data": data, "message": get_flash_Message()}
        r = templates.TemplateResponse("settings.html", context)
    return r

@app.route("/logs")
@app.route("/logs.html")
async def logs(request: Request):
    r = None
    if not ("logged_in" in request.cookies and bool(request.cookies['logged_in'])):
        r = RedirectResponse(app.url_path_for("login"), status_code=302)
    else:
        r = templates.TemplateResponse("logs.html", {"request": request})
    return r

@app.route("/users")
@app.route("/users.html")
async def logs(request: Request):
    r = None
    if not ("logged_in" in request.cookies and bool(request.cookies['logged_in'])):
        r = RedirectResponse(app.url_path_for("login"), status_code=302)
    else:
        r = templates.TemplateResponse("users.html", {"request": request})
    return r

@app.get("/Users_Delete")
async def getUsers_Delete(request: Request, id:int = None):
    r = None
    if not ("logged_in" in request.cookies and bool(request.cookies['logged_in'])):
        r = RedirectResponse(app.url_path_for("login"), status_code=302)
    else:
        Dict_User = objMongoClient.get_users(iID=id)
        if len(Dict_User) > 0 or str(form['username']).lower() != Dict_User[0]["Email"]:
            data = {"id": Dict_User[0]["id"], "Email": Dict_User[0]["Email"]}
        else:
            data = {"id": 0, "Email": ""}
        context = {"request": request, "data": data}
        r = templates.TemplateResponse("Users_Delete.html", context)
    return r

@app.post("/Users_Delete")
async def postUsers_Delete(request: Request):
    form = await request.form()
    if(bool(form["val-user_id"])):
        objMongoClient.delete_users(iID=int(form['val-user_id']))
    return {"success": True}

@app.get("/Users_Upsert")
async def getUsers_Upsert(request: Request, id:int = None):
    r = None
    if not ("logged_in" in request.cookies and bool(request.cookies['logged_in'])):
        r = RedirectResponse(app.url_path_for("login"), status_code=302)
    else:
        if id != None and id > 0:
            Dict_User = objMongoClient.get_users(iID=id)        
            if  len(Dict_User) > 0:
                data = {"id": Dict_User[0]["id"], "Role": Dict_User[0]["Role"], "Email": Dict_User[0]["Email"],"First_Name": Dict_User[0]["First Name"],"Last_Name": Dict_User[0]["Last Name"],"Password": "?????"}
        else:
            data = {"id": -1, "Role": "","Email": "","First_Name": "","Last_Name": "","Password": ""}
        context = {"request": request, "data": data}
        r = templates.TemplateResponse("users_upsert.html", context)
    return r

@app.post("/Users_Upsert")
async def postUsers_Upsert(request: Request):
    form = await request.form()
    if(bool(form["val-user_id"])):
        user_dict = {"id": int(form["val-user_id"]), "Role": form["val-Role"], "Email": form["val-Email"], "First Name": form["val-First_Name"], "Last Name": form["val-Last_Name"]}
        pw = form["val-Password"].strip()
        if(pw.find("?") < 0 and len(pw) > 0):
            user_dict["Password"] = form["val-Password"]
        objMongoClient.upsert_users(user_dict)
    return {"success": True}

#######################################

#######################################
@app.post('/settings_update')
async def settings_update(request: Request):
    #if not session.get('logged_in'):
    #    abort(401)
    #db = get_db()
    form = await request.form() #await request.form

    if(bool(form["val-ws_Days"])):
        Arr_days = {"0": 0, "1": 0, "2": 0, "3": 0, "4": 0, "5": 0, "6": 0}
        ws = form.getlist("val-ws_Days")
        for iDay in ws:
            Arr_days[iDay] = 1
        if(Arr_days != get_keyData('ws_Days')):
            objMongoClient.update_DB_key('ws_Days', Arr_days)
    if(bool(form["val-ws_Time_From"]) and bool(form["val-ws_Time_To"])):
        Arr_Times = {"From": "00:00", "To": "00:00"}
        res = True
        try:
            Arr_Times["From"] = form["val-ws_Time_From"]
            Arr_Times["To"] = form["val-ws_Time_To"]
            res = bool(datetime.strptime(Arr_Times["From"], "%H:%M")) and bool(datetime.strptime(Arr_Times["To"], "%H:%M"))
        except ValueError:
            res = False
        if(res and Arr_Times != get_keyData('ws_Time')):
            objMongoClient.update_DB_key('ws_Time', Arr_Times)
    if(bool(form["val-wms_ws_sleep"]) and form["val-wms_ws_sleep"] != get_keyData('wms_ws_sleep')):
        objMongoClient.update_DB_key('wms_ws_sleep', form['val-wms_ws_sleep'])
    if(bool(form["val-t_gmt"]) and form["val-t_gmt"] != get_keyData('t_gmt')):
        objMongoClient.update_DB_key('t_gmt', form['val-t_gmt'])

    if(bool(form["val-wms_host"]) and form["val-wms_host"] != get_keyData('wms_host')):
        objMongoClient.update_DB_key('wms_host', form['val-wms_host'])
    if(bool(form["val-wms_task_params"]) and form["val-wms_task_params"] != get_keyData('wms_task_params')):
        objMongoClient.update_DB_key('wms_task_params', form['val-wms_task_params'])
    if(bool(form["val-wms_threshold"]) and form["val-wms_threshold"] != get_keyData('wms_threshold')):
        objMongoClient.update_DB_key('wms_threshold', form['val-wms_threshold'])
    if(bool(form["val-wms_threshold_type"]) and form["val-wms_threshold_type"] != get_keyData('wms_threshold_type')):
        objMongoClient.update_DB_key('wms_threshold_type', form['val-wms_threshold_type'])
    if(bool(form["val-wms_auth_type"]) and form["val-wms_auth_type"] != get_keyData('wms_auth_type')):
        objMongoClient.update_DB_key('wms_auth_type', form['val-wms_auth_type'])
    if(bool(form["val-wms_auth_user"]) and form["val-wms_auth_user"] != get_keyData('wms_auth_user')):
        objMongoClient.update_DB_key('wms_auth_user', form['val-wms_auth_user'])
    if(bool(form["val-wms_auth_pass"]) and form["val-wms_auth_pass"] != get_keyData('wms_auth_pass')):
        objMongoClient.update_DB_key('wms_auth_pass', form['val-wms_auth_pass'])
    if(bool(form["val-wms_auth_key"]) and form["val-wms_auth_key"] != get_keyData('wms_auth_key')):
        objMongoClient.update_DB_key('wms_auth_key', form['val-wms_auth_key'])
 
    if(bool(form["val-fms_host"]) and form["val-fms_host"] != get_keyData('fms_host')):
        objMongoClient.update_DB_key('fms_host', form['val-fms_host'])
    if(bool(form["val-fms_Robots_params"]) and form["val-fms_Robots_params"] != get_keyData('fms_Robots_params')):
        objMongoClient.update_DB_key('fms_Robots_params', form['val-fms_Robots_params'])
    if(bool(form["val-fms_auth_type"]) and form["val-fms_auth_type"] != get_keyData('fms_auth_type')):
        objMongoClient.update_DB_key('fms_auth_type', form['val-fms_auth_type'])
    if(bool(form["val-fms_auth_user"]) and form["val-fms_auth_user"] != get_keyData('fms_auth_user')):
        objMongoClient.update_DB_key('fms_auth_user', form['val-fms_auth_user'])
    if(bool(form["val-fms_auth_pass"]) and form["val-fms_auth_pass"] != get_keyData('fms_auth_pass')):
        objMongoClient.update_DB_key('fms_auth_pass', form['val-fms_auth_pass'])
    if(bool(form["val-fms_auth_key"]) and form["val-fms_auth_key"] != get_keyData('fms_auth_key')):
        objMongoClient.update_DB_key('fms_auth_key', form['val-fms_auth_key'])

    #db.commit()
    global settings_data,tGMT,ws_Time,ws_Days
    settings_data = objMongoClient.get_dbsetting()
    tGMT = int(get_keyData('t_gmt'))
    ws_Days = get_keyData('ws_Days')
    ws_Time = get_keyData('ws_Time')
    #await flash('data was successfully updated')
    #return redirect(url_for('settings'))
    #context = {"request": request}
    #return templates.TemplateResponse("settings.html", context)
    set_flash_Message('data was successfully updated')
    return RedirectResponse(app.url_path_for("settings"), status_code=303)

#------------------
@app.get("/data/")
async def api_data(request: Request):
    try:
        params = str(request.query_params)
        url = f'http://some.other.api/{params}'
        headers = {'Authorization': "some_long_key"}
        r = requests.get(url, headers=headers)
        return Response(content=r.content)
    except Exception as e:
        print(e)
        return repr(e)

#------------------

#######################################

@app.get('/stop_service')
async def Stop_Service():
    global Service_State
    Service_State['start'] = False
    Service_State["WMS_Connected"] = False
    Service_State["FMS_Connected"] = False
    ret = await getService_State()
    return ret

@app.get('/start_service')
async def Start_Service():
    global Service_State

    if (not Service_State['start']):
        while not Service_State['cancle']:
            await sleep(0.5)
        Service_State['start'] = True
        asyncio.ensure_future(Run_Service())
        #Run_Service()
    ret = await getService_State()
    return ret

@app.get('/service_state')
async def getService_State():
    global Service_State
    tTime = datetime.now() + timedelta(hours=tGMT)
    if(Service_State['start']):
        Service_State["duration"] = str(tTime - datetime.strptime(Service_State["start_ts"], '%Y-%m-%d %H:%M:%S.%f'))
    else:
        Service_State["duration"] = str(tTime - tTime)
    return Service_State

@app.get("/getusers")
async def getusers():
    ret = {"data": objMongoClient.get_users()}
    return  ret

@app.get("/wmstasks")
async def wmstasks():
    ret = {"data": obj_WMS.WMS_Task_data}
    return  ret

@app.get("/wmallocation")
async def wmallocation():
    ret = {"data": obj_WMS.WMS_allocation_data}
    return  ret

@app.get("/fmsrobots")
async def fmsrobots():
    ret = {"data": obj_FMS.RobotsFMS_ar}
    return  ret

@app.get("/wmstask_state")
async def wmstask_state():
    ret = obj_WMS.task_State
    return  ret

@app.get("/fmsrobots_state")
async def fmsrobots_state():
    ret = obj_FMS.Robots_State
    return  ret

@app.get("/getsettings")
async def getsettings():
    ret = settings_data
    return  ret

@app.get("/logfmsrobots")
async def logfmsrobots():
    arr_data = objMongoClient.get_RobotsWMS_Log()
    return  {"data": arr_data}

@app.get("/logallocations")
async def logallocations():
    arr_data = objMongoClient.get_allocation_Log()
    return  {"data": arr_data}

@app.get("/activelocation")
async def activelocation():
    return obj_WMS.sync_WMS_active_location()

@app.get('/generategeotags')
async def generategeotags(vAreaLevel:Optional[str] = 'AL', vLevel:Optional[int] = 1, v_ZNo:Optional[int] = 1, vOrigin_X:Optional[float] = 0, vOrigin_Y:Optional[float] = 0, vyaw:Optional[float] = 0, 
                          vResolution:Optional[float] = 0.0161, vImage_Height:Optional[float] = 4722, vImage_Width:Optional[float] = 2547, vMapId:Optional[str] = '60d1d06b682c63fce6345bb7', 
                          vAisle_Total_Z1:Optional[int] = 12, vAisle_Total_Z2:Optional[int] = 24, vBaysPerAisle:Optional[int] = 16, vMaxSections1:Optional[int] = 4, vMaxSections2:Optional[int] = 5,
                          vMaxShelves:Optional[int] = 5, vd_Aisle:Optional[float] = 0, vZoneYardLength:Optional[float] = 5.1, vFirstBaywidth:Optional[float] = 0.6, vAislesWdth:Optional[float] = 1.2, 
                          vBay_Length_1:Optional[float] = 1.76, vBay_Length_2:Optional[float] = 2.17, vBay_Width_z1:Optional[float] = 1.2, vBay_Width_z2:Optional[float] = 0.8, vdX:Optional[float] = 0, 
                          vdY:Optional[float] = 0.2, vJSON:Optional[int] = 1):
    GeoTags = GeoTagGenerator()
    ret = GeoTags.salasGeotags(vAreaLevel, vLevel, v_ZNo, vOrigin_X, vOrigin_Y, vyaw, vResolution, vImage_Height, vImage_Width, 
                                                                                      vMapId, vAisle_Total_Z1, vAisle_Total_Z2, vBaysPerAisle, vMaxSections1, vMaxSections2, 
                                                                                      vMaxShelves, vd_Aisle, vZoneYardLength, vFirstBaywidth, vAislesWdth, vBay_Length_1, vBay_Length_2, 
                                                                                      vBay_Width_z1, vBay_Width_z2, vdX, vdY, vJSON)
    print(ret)
    return ret

##-------- AMS Work ---------

async def async_WMS_Task_processor():
    #global obj_WMS
    iret = await asyncio.get_running_loop().run_in_executor(None, obj_WMS.sync_WMS_Task_processor)
    asyncio.ensure_future(async_WMS_allocation_processor())
    Service_State["WMS_Connected"] = (iret == 1 and bool(Service_State["start"]))

async def async_WMS_allocation_processor():
    global obj_WMS, WMS_RobotsWMS_ts
    iret = await asyncio.get_running_loop().run_in_executor(None, obj_WMS.sync_WMS_allocation_processor)

    if(WMS_RobotsWMS_ts != obj_WMS.RobotsWMS_ts and obj_WMS.RobotsWMS_ts != None):
        rows = list(filter(lambda item: item['mod_ts'] > WMS_RobotsWMS_ts, obj_WMS.RobotsWMS_ar))
        if(len(rows) > 0):
            ts = datetime.now() + timedelta(hours=tGMT)
            ID_N = objMongoClient.get_max_val("RobotsWMS_Log","ID_N")
            ID_N = ID_N if bool(ID_N) else 0 
            for row in rows:
                ID_N += 1
                mydict = {"ID_N": ID_N, 'id': row['id'], 'task_nbr': row['task_nbr'], 'cart_nbr': row['cart_nbr'], 'location': row['location'], 'status_id': row['status_id'], 'mod_ts': row['mod_ts'], 'ts': ts}
                objMongoClient.insert_RobotsWMS_Log(mydict)
                
                FK_N = ID_N
                rows2 = list(filter(lambda item: item['task_id'] == row['id'], obj_WMS.WMS_allocation_data))
                ID_N2 = objMongoClient.get_max_val("allocation_Log","ID_N")
                ID_N2 = ID_N2 if bool(ID_N2) else 0 
                for row2 in rows2:
                    ID_N2 += 1
                    mydict = {"ID_N": ID_N2, 'FK_N':FK_N, 'id': row2['id'], 'create_ts': row2['create_ts'], 'order_dtl_id': row2['order_dtl_id'], 'status_id': row2['status_id'], 'type': row2['type'], 'wave_id': row2['wave_id'], 'wave_nbr': row2['wave_nbr'], 'task_id': row2['task_id'], 'task_seq_nbr': row2['task_seq_nbr'], 'mhe_system_id': row2['mhe_system_id'], 'pick_user': row2['pick_user'], 'picked_ts': row2['picked_ts'], 'pick_locn_str': row2['pick_locn_str'], 'is_picking_flg': row2['is_picking_flg'], 'ts': ts}
                    objMongoClient.insert_allocation_Log(mydict)
        if obj_WMS.RobotsWMS_ts != None:
            WMS_RobotsWMS_ts = obj_WMS.RobotsWMS_ts

async def async_FMS_Robots_processor():
    #global obj_FMS
    iret = await asyncio.get_running_loop().run_in_executor(None, obj_FMS.sync_FMS_Robots_processor)
    Service_State["FMS_Connected"] = (iret == 1 and bool(Service_State["start"]))

def set_flash_Message(msg):
    global __Message
    __Message = msg

def get_flash_Message():
    global __Message
    msg = __Message
    __Message = ""
    return msg



##-------- AMS Work ---------

#from threading import Thread


def main():
    port = int(os.environ.get('PORT', 5000))
    #app.run(host='0.0.0.0', port=port)
    #uvicorn.run("microservice:app",host='0.0.0.0', port=port, reload=True, debug=True, workers=3) 
    #uvicorn.run("microservice:app",host='0.0.0.0', port=port)
    config = Config()
    config.bind = ["0.0.0.0:" + str(port)]
    asyncio.run(serve(app, config))

if __name__ == '__main__':
    main()
