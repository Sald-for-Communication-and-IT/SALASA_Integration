
class GeoTagGenerator:
    vAreaLevel:str = "AL"
    vLevel: 1
    v_ZNo: 1
    vOrigin_X: 0
    vOrigin_Y: 0
    vyaw: 0
    vResolution: 0.0161
    vImage_Height: 4722
    vImage_Width: 2547
    vMapId:str = '60d1d06b682c63fce6345bb7'
    vAisle_Total_Z1: 12
    vAisle_Total_Z2: 24
    vBaysPerAisle: 16
    vMaxSections1: 4
    vMaxSections2: 5
    vMaxShelves: 5
    vd_Aisle: 0
    vZoneYardLength: 5.1
    vFirstBaywidth: 0.6
    vAislesWdth: 1.2
    vBay_Length_1: 1.76
    vBay_Length_2: 2.17
    vBay_Width_z1: 1.2
    vBay_Width_z2: 0.8
    vdX: 0
    vdY: 0.2
    vJSON: 1

    def __init__(self):
        pass
        
    def salasGeotags(self,vAreaLevel:str = 'AL', vLevel:int = 1, v_ZNo:int = 1, vOrigin_X:float = 0, vOrigin_Y:float = 0, vyaw:float = 0, vResolution:float = 0.0161, vImage_Height:float = 4722, vImage_Width:float = 2547, 
                     vMapId:str = '60d1d06b682c63fce6345bb7', vAisle_Total_Z1:int = 12, vAisle_Total_Z2:int = 24, vBaysPerAisle:int = 16, vMaxSections1:int = 4, vMaxSections2:int = 5, 
                     vMaxShelves:int = 5, vd_Aisle:float = 0, vZoneYardLength:float = 5.1, vFirstBaywidth:float = 0.6, vAislesWdth:float = 1.2, vBay_Length_1:float = 1.76, vBay_Length_2:float = 2.17, 
                     vBay_Width_z1:float = 1.2, vBay_Width_z2:float = 0.8, vdX:float = 0, vdY:float = 0.2, vJSON:int = 1):
        sLabel = []
        sAreaLevel = vAreaLevel
        iLevel = vLevel
        #-------------
        MaxShelves = vMaxShelves
        d_Aisle = vd_Aisle
        MaxSections1 = vMaxSections1
        MaxSections2 = vMaxSections2
        #-------------
        iResolution = vResolution #1.61 (Cm/Pixel) /100 (Cm-to-Meter)
        Image_Height = vImage_Height #pixels
        Image_Width = vImage_Width #pixels
        Ros_Origin_y = Image_Height * iResolution
        #-------------
        i_ZNo = v_ZNo
        Origin_X = vOrigin_X
        Origin_Y = vOrigin_Y
        yaw = vyaw
        FirstBaywidth = vFirstBaywidth
        Bay_Length_1 = vBay_Length_1 #1.65+0.05
        Bay_Length_2 = vBay_Length_2 #2.05+0.1
        Bay_Width_z1 = vBay_Width_z1
        Bay_Width_z2 = vBay_Width_z2
        iAisle_Total_Z1 = vAisle_Total_Z1
        iAisle_Total_Z2 = vAisle_Total_Z2
        #------------------
        BaysPerAisle = vBaysPerAisle
        AislesWdth = vAislesWdth
        ZoneYardLength = vZoneYardLength
        dX = vdX
        dY = vdY
        iJSON = vJSON
        sMapId = vMapId
        #------------------
            
        #------------------ args ------------------
            
        Ros_Origin_y = Image_Height * iResolution
        if (i_ZNo == 1):
            AislesTotal = iAisle_Total_Z1
            BayWidth = Bay_Width_z1
            Bay_Width_Base = 0
            d_Aisle = 1
        else:   #Zone =2
            AislesTotal = iAisle_Total_Z2
            BayWidth = Bay_Width_z2
            Bay_Width_Base = (Bay_Width_z1*(iAisle_Total_Z1-1))+(AislesWdth*(iAisle_Total_Z1)) + dY
            d_Aisle = 0
            sAreaLevel = "BL"

        for i_Aisle in range(1,AislesTotal+1):
            for i_Bay in range(1,BaysPerAisle+1):
                if(((i_Aisle % 2) > 0 and i_Bay <= 2) or ((i_Aisle % 2) == 0 and i_Bay >= (BaysPerAisle-1))):
                    MaxSections = MaxSections1
                else:
                    MaxSections = MaxSections2
                for i_Shelve in range(1,MaxShelves+1):
                    for i_Section in range(1,MaxSections+1):
                        v_Bay = i_Bay
                        if((i_Aisle % 2) == 0):
                            v_Bay = BaysPerAisle + 1 -i_Bay
                        posX = Origin_X+ZoneYardLength+(Bay_Length_1*(i_Bay-1 if i_Bay<=2 else 2)) + ((Bay_Length_2*(i_Bay-3)) if i_Bay>2 else 0) +((Bay_Length_1 if i_Bay<=2 else Bay_Length_2)*(i_Section-0.5)/MaxSections) + dX
                        posY = Origin_Y+FirstBaywidth + Bay_Width_Base + BayWidth*(i_Aisle-d_Aisle) + (AislesWdth*(i_Aisle-1)) + dY
                        posY = Ros_Origin_y - posY
                        oLabel = {"name" : sAreaLevel + str(iLevel) +"-"+ "{:03d}".format(i_Aisle) +"-"+ "{:02d}".format(v_Bay) + "L" +"-"+ "{:02d}".format(i_Shelve) +"-"+ "{:02d}".format(i_Section), "pose": { "x": posX, "y": posY, "yaw": yaw}}
                        if(sMapId != ""):
                            oLabel['mapId']= sMapId
                        sLabel.append(oLabel)
                        if (i_ZNo != 1 or i_Aisle < iAisle_Total_Z1):
                            oLabel = {"name" : sAreaLevel + str(iLevel) +"-"+ "{:03d}".format(i_Aisle) +"-"+ "{:02d}".format(v_Bay) + "R" +"-"+ "{:02d}".format(i_Shelve) +"-"+ "{:02d}".format(i_Section), "pose": { "x": posX, "y": posY - (AislesWdth-dY), "yaw": yaw}}
                            if(sMapId != ""):
                                oLabel['mapId']= sMapId
                            sLabel.append(oLabel)
        ret ={"data":sLabel}
        if (iJSON != 1):
            ss="Label,X,Y,Theta" + '\n'
            for x in sLabel:
                ss += x["name"] + "," + str(x["pose"]["x"]) + "," + str(x["pose"]["y"]) + "," + str(x["pose"]["yaw"]) + '\n'    
            ret = ss
            
        return  ret    