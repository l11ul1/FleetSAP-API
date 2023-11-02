import requests
import csv
from datetime import datetime, timedelta
import time
import json
import base64
import pandas as pd
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from FleetsapServer import keep_alive
from datetime import date
from dateutil.relativedelta import relativedelta
import re


def createDataRange():
  return pd.period_range(end=pd.Period.now('M'),
                         start=(pd.Period.now('M') - 12),
                         freq="M")


def signIn():
  signInUrl = "https://apiv2ext.fleetsap.com/api/SignIn"
  requestBody = {
      "userName": "smun",
      "password": "g9vrCqfR54$",
      "companyName": "AFD",
      "appTypeDescription": "Web"
  }

  resp = requests.request('POST', signInUrl, json=requestBody).json()
  return resp["data"]["token"]


def getFuelDeliveryData(month, year):
  endPointURL = "https://apiv2ext.fleetsap.com/api/Workorders/V2/FuelDelivery/OrderManagement?"
  pattern = r"'(.*?)'"
  token = signIn()
  temp = ""

  for let in "Month:" + month + ",Year:" + year:
    temp = temp + let + "\0"

  encodedString = str(base64.b64encode(bytearray(temp.encode())))
  print(encodedString)
  clear_filter = re.search(pattern, encodedString).group().strip("''")
  params = {'CompanyId': 'NgA5ADMA', 'Filter': clear_filter}
  authorization = {'Authorization': 'Bearer ' + token}

  resp = requests.request('GET',
                          endPointURL,
                          headers=authorization,
                          params=params)

  try:
    parsedResponse = resp.json()
    return parsedResponse["data"]
  except json.decoder.JSONDecodeError:
    print("no data")
    return None

def getTanksDeliveryData():
  endPointURL = "https://apiv2ext.fleetsap.com/api/Tanks?"
  token = signIn()
  params = {'CompanyID':'NgA5ADMA', 'UserAccountId':'cwBtAHUAbgA=','DeviceType':'NAA='}
  authorization = {'Authorization': 'Bearer ' + token}
  resp = requests.request('GET',
    endPointURL,
    headers=authorization,
    params=params)
  
  try:
    parsedResponse = resp.json()
    return parsedResponse["data"]
  except json.decoder.JSONDecodeError:
    print("no data")
    return None

def pushFuelDeliveryDataToDB():
  print("triggered delivery data")
  uri = "mongodb+srv://l11ul1:Test1!@fleetsap.ghdu8ge.mongodb.net/?retryWrites=true&w=majority"
  client = MongoClient(uri)
  db = client["fleetsapp_db"]
  collection = db["fleetsapp_api_result_collection"]
  # x = collection.delete_many({})

  periods = createDataRange()
  print(periods)
  obsoletePeriods = periods[:11]
  refreshingPeriods = periods[11:]
  cursor = collection.find()

  results = list(cursor)

  if len(results) == 0:
    for period in obsoletePeriods:
      print(period)
      year = str(period).split("-")[0]
      month = str(period).split("-")[1]
      records = getFuelDeliveryData(month, year)

      if (records is not None):
        for record in records:
          data = {
              "id":
              record["id"],
              "tripnumber":
              record["tripnumber"],
              "tripStartDateTime":
              record["tripStartDateTime"],
              "tripEndDateTime":
              record["tripEndDateTime"],
              "tripstatus":
              record["tripstatus"],
              "tripLastStatusDate":
              record["tripLastStatusDate"],
              "tripVehicleName":
              record["tripVehicleName"],
              "tripTrailerName1":
              record["tripTrailerName1"],
              "tripTrailerName2":
              record["tripTrailerName2"],
              "tripDriverName":
              record["tripDriverName"],
              "ordernumber":
              record["ordernumber"],
              "ordertype":
              record["ordertype"],
              "customerdestination":
              record["customerdestination"],
              "locationdestination":
              record["locationdestination"],
              "orderStartDateTime":
              record["orderStartDateTime"],
              "orderEndDateTime":
              record["orderEndDateTime"],
              "orderstatus":
              record["orderstatus"],
              "lastOrderStatusDate":
              record["lastOrderStatusDate"],
              "fixedStatus":
              record["fixedStatus"],
              "orderSignature":
              record["orderSignature"],
              "orderBytripId":
              record["orderBytripId"],
              "typeOrigin":
              record["typeOrigin"],
              "originName":
              record["originName"],
              "billOfLading":
              record["billOfLading"],
              "typeDestination":
              record["typeDestination"],
              "originId":
              record["originId"],
              "destinationId":
              record["destinationId"],
              "originLocationId":
              record["originLocationId"],
              "destinationLocationId":
              record["destinationLocationId"],
              "locationNameDestinationLocationId":
              record["locationNameDestinationLocationId"],
              "locationNameOriginLocationId":
              record["locationNameOriginLocationId"],
              "customerNumber":
              record["customerNumber"],
              "fuelCode":
              record["fuelCode"],
              "locationCode":
              record["locationCode"],
              "estimatedVolume":
              record["estimatedVolume"],
              "deliveredNetVolume":
              record["deliveredNetVolume"],
              "deliveredGrossVolume":
              record["deliveredGrossVolume"],
              "batchNumber":
              record["batchNumber"],
              "orderdetailId":
              record["orderdetailId"]
          }
          collection.update_one(filter={
                                        'id': data['id'],
                                        'orderdetailId': data['orderdetailId']
                                       },
                                update={"$set": data},
                                upsert=True)
    print("obsolete data pushed")
  else:
    for period in refreshingPeriods:
      year = str(period).split("-")[0]
      month = str(period).split("-")[1]
      records = getFuelDeliveryData(month, year)
      if records is not None:
        for record in records:
          data = {
              "id":
              record["id"],
              "tripnumber":
              record["tripnumber"],
              "tripStartDateTime":
              record["tripStartDateTime"],
              "tripEndDateTime":
              record["tripEndDateTime"],
              "tripstatus":
              record["tripstatus"],
              "tripLastStatusDate":
              record["tripLastStatusDate"],
              "tripVehicleName":
              record["tripVehicleName"],
              "tripTrailerName1":
              record["tripTrailerName1"],
              "tripTrailerName2":
              record["tripTrailerName2"],
              "tripDriverName":
              record["tripDriverName"],
              "ordernumber":
              record["ordernumber"],
              "ordertype":
              record["ordertype"],
              "customerdestination":
              record["customerdestination"],
              "locationdestination":
              record["locationdestination"],
              "orderStartDateTime":
              record["orderStartDateTime"],
              "orderEndDateTime":
              record["orderEndDateTime"],
              "orderstatus":
              record["orderstatus"],
              "lastOrderStatusDate":
              record["lastOrderStatusDate"],
              "fixedStatus":
              record["fixedStatus"],
              "orderSignature":
              record["orderSignature"],
              "orderBytripId":
              record["orderBytripId"],
              "typeOrigin":
              record["typeOrigin"],
              "originName":
              record["originName"],
              "billOfLading":
              record["billOfLading"],
              "typeDestination":
              record["typeDestination"],
              "originId":
              record["originId"],
              "destinationId":
              record["destinationId"],
              "originLocationId":
              record["originLocationId"],
              "destinationLocationId":
              record["destinationLocationId"],
              "locationNameDestinationLocationId":
              record["locationNameDestinationLocationId"],
              "locationNameOriginLocationId":
              record["locationNameOriginLocationId"],
              "customerNumber":
              record["customerNumber"],
              "fuelCode":
              record["fuelCode"],
              "locationCode":
              record["locationCode"],
              "estimatedVolume":
              record["estimatedVolume"],
              "deliveredNetVolume":
              record["deliveredNetVolume"],
              "deliveredGrossVolume":
              record["deliveredGrossVolume"],
              "batchNumber":
              record["batchNumber"],
              "orderdetailId":
              record["orderdetailId"]
          }
          
          collection.update_one(filter={                                        
                                      'id': data['id'],
                                      'orderdetailId': data['orderdetailId']
                                    },
                                update={"$set": data},
                                upsert=True)
  
  print("Pushed/Updated Fuel Delivery")
  
def pushTanksDataToDB():
  print("triggered tanks data")
  uri = "mongodb+srv://l11ul1:Test1!@fleetsap.ghdu8ge.mongodb.net/?retryWrites=true&w=majority"
  client = MongoClient(uri)
  db = client["fleetsapp_db"]
  collection = db["fleetsap_tanks_api_collection"]

  records = getTanksDeliveryData()
  if records is not None:
    for record in records:
      data =  {
        "carName": record["carName"],
        "carNumber": record["carNumber"],
        "tankID": record["tankID"],
        "tankDescription": record["tankDescription"],
        "capacity": record["capacity"],
        "productlevel": record["productlevel"],
        "waterLevel": record["waterLevel"],
        "netProduct": record["netProduct"],
        "temperature": record["temperature"],
        "fuelType": record["fuelType"],
        "fuelTypeDescription": record["fuelTypeDescription"],
        "productIn": record["productIn"],
        "productOut": record["productOut"],
        "kioksId": record["productIn"],
        "kioskDescription": record["kioskDescription"],
        "tankOrientation": record["tankOrientation"],
        "tankTypeId": record["tankTypeId"],
        "latitud": record["latitud"],
        "longitud": record["longitud"],
        "currentLevelHeight": record["currentLevelHeight"],
        "currentWaterHeight": record["currentWaterHeight"],
        "eventType": record["eventType"],
        "eventCode": record["eventCode"],
        "category": record["category"],
        "eventId": record["eventId"],
        "ullage": record["ullage"],
        "burnRate": record["burnRate"],
        "daysBeforeEmpty": record["daysBeforeEmpty"],
        "inventoryCost": record["inventoryCost"],
        "currencySymbol": record["currencySymbol"]
      }
      
      collection.update_one(filter={
          'carNumber': data['carNumber'], 
          'eventId': data['eventId']
        },
        update={"$set": data},
        upsert=True
      )
      
  print("Pushed/Updated Tanks")
  

  
  
        


keep_alive()
while True:
  pushTanksDataToDB()
  pushFuelDeliveryDataToDB()
  time.sleep(3600)
