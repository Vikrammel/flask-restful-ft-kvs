#-*- coding: utf-8 -*-
#app.py for project 3, CMPS128

from flask import Flask
from flask import request, abort, jsonify, json
from flask_restful import Resource, Api
import re, sys, os, requests
import datetime

app = Flask(__name__)
api = Api(app)
newline = "&#13;&#10;"

# Get expected environment variables.
K = os.environ.get('K')
IpPort = os.environ.get('IPPORT')
EnvView = os.environ.get('VIEW')

# Is this a replica or a proxy?
isReplica = False
# Dictionary acting as vector clock. IpPort -> local clock value.
vClock = {}
vClock[IpPort] = 0
# String to prepend onto URL.
http_str = 'http://'

# Dictionary where key->value pairs will be stored.
d = {}
# Arrays to store replicas and proxies.
view = []
replicas = []
proxies = []

# Initialize view array based on Environment Variable 'VIEW'
view = EnvView.split(",")
pos = view.index(IpPort)
# Initialize this node as a replica or a proxy.
if pos < K:
	isReplica = True
    replicas.append(IpPort)
else:
    proxies.append(IpPort)

class Handle(Resource):
    #returns if node is a replica
    def get_node_details():
        return {"result":"success", "replica": isReplica}, 200
    
    #returns list of replicas
    def get_all_replicas():
        return {"result":"success", "replicas": replicas}, 200

    if isReplica:
        #Handles GET request
        def get(self, key):
            #If key is not in dict, return error.
            if key not in d:
                return {'result': 'Error', 'msg': 'Key does not exist'}, 404
            #If key is in dict, return its corresponding value.
            return {'result': 'Success', 'value': d[key], 'node_id': IP, 'causal_payload': , 'timestamp': datetime.datetime.now().time()}, 200

        #Handles PUT request
        def put(self, key):
            try:
                value = request.form['val']
            except:
                value = ''
                pass
            #Makes sure a value was actually supplied in the PUT.
            if not value:
                return {'result': 'Error', 'msg': 'No value provided'}, 403
            #Restricts key length to 1<=key<=200 characters.
            if not 1 <= len(str(key)) <= 200:
                return {'result': 'Error', 'msg': 'Key not valid'}, 403
            #Restricts key to alphanumeric - both uppercase and lowercase, 0-9, and _
            if not re.match(r'^\w+$', key):
                return {'result': 'Error', 'msg': 'Key not valid'}, 403

            #Restricts value to a maximum of 1Mbyte.
            if sys.getsizeof(value) > 1000000:
                return {'result': 'Error', 'msg': 'Object too large. Size limit is 1MB'}, 403
            
			vClock[IpPort]++;
            #If key is not already in dict, create a new entry.
            if key not in d:
                d[key] = value
                return {'replaced': 'False', 'msg': 'New key created'}, 201
            #If key already exists, set replaced to true.
            d[key] = value
            return {'replaced': 'True', 'msg': 'Value of existing key replaced'}, 200
            
        #Handles DEL request
        def delete(self, key):
            #If key is not in dict, return error.
            if key not in d:
                return {'result': 'Error', 'msg': 'Key does not exist'}, 404

            #If key is in dict, delete key->value pair.
            del d[key]
            return {'result': 'Success'}, 200

    else:
        #handle requests from forwarding instance
        def get(self, key):
            #try requesting primary
            try:
                response = requests.get(http_str + mainAddr + '/kv-store/' + key)
            except requests.exceptions.RequestException as exc: #handle primary failure upon put request
                return {'result': 'Error','msg': 'Server unavailable'}, 500
            return response.json()

        def put(self,key):
            try:
                value = request.form['val']
            except:
                value = ''
                pass
            #Makes sure a value was actually supplied in the PUT.
            if not value:
                return {'result': 'Error', 'msg': 'No value provided'}, 403
            try:
                response = requests.put((http_str + mainAddr + '/kv-store/' + key), data = {'val': value})
            except requests.exceptions.RequestException as exc: #handle primary failure upon put request
                return {'result': 'Error','msg': 'Server unavailable'}, 500
            return response.json()

        def delete(self, key):
            #try requesting primary
            try:
                response = requests.delete(http_str + mainAddr + '/kv-store/' + key)
            except requests.exceptions.RequestException as exc: #handle primary failure upon delete request
                return {'result': 'Error','msg': 'Server unavailable'}, 500
            return response.json()
api.add_resource(Handle, '/kv-store/<key>')

if __name__ == "__main__":
	localAddress = EnvView.split(":")
    app.run(host=localAddress[0], port=localAddress[1])