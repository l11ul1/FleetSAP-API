from flask import Flask
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import json
from threading import Thread

app = Flask(__name__)


@app.route("/result", methods=['GET'])
def result():
  uri = "mongodb+srv://l11ul1:Test1!@fleetsap.ghdu8ge.mongodb.net/?retryWrites=true&w=majority"
  client = MongoClient(uri)
  db = client["fleetsapp_db"]
  collection = db["fleetsapp_api_result_collection"]
  documents = collection.find()
  output = [{item: data[item]
             for item in data if item != '_id'} for data in documents]

  return output

@app.route("/resultTanks", methods=['GET'])
def resultTanks():
  uri = "mongodb+srv://l11ul1:Test1!@fleetsap.ghdu8ge.mongodb.net/?retryWrites=true&w=majority"
  client = MongoClient(uri)
  db = client["fleetsapp_db"]
  collection = db["fleetsap_tanks_api_collection"]
  documents = collection.find()
  output = [{item: data[item]
             for item in data if item != '_id'} for data in documents]

  return output


@app.route("/", methods=['GET'])
def main():
  return "fleetsap api"


def run():
  app.run(host='0.0.0.0', port=8080)


def keep_alive():
  t = Thread(target=run)
  t.start()
