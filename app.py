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

#function to compare 2 vector clocks.
#return value: -1 -> clock1 smaller, 0 -> concurrent, 1 -> clock2 smaller
def compareClocks(clock1, clock2):
    compareResult = 0
    if clock1.len() == clock2.len() :   # might need to change these to len(clock1) ect
        for i in range(0,clock1.len()):
            if clock1[i] < clock2[i] :
                if compareResult == 1:
                    return 0
                compareResult = -1
            if clock2[i] < clock1[i] :
                if compareResult == -1:
                    return 0
                compareResult = 1
    return compareResult

def removeReplica(ip):
    replicas.remove(ip)
    view.remove(ip)
    print("Replica: " + ip + " removed.")

def removeProxie(ip):
    proxies.remove(ip)
    view.remove(ip)
    print("Proxie: " + ip + " removed.")

def updateRatio():
    # If Replicas is less than K
    if len(replicas) < K:
        # Try to convert proxie to replica
        if len(proxies) > 0:
            tempNode = proxies[-1]
            replicas.append(tempNode)
            removeProxie(tempNode)
    # If more replicas then needed, convert to proxie        
    if len(replicas) > K:
            tempNode = replicas[-1]
            proxies.append(tempNode)
            removeReplica(tempNode)

class Handle(Resource):
    if isReplica:
        #Handles GET request
        def get(self, key):
            #Special command: Returns if node is a replica.
            if key == 'get_node_details':
                return {"result": "success", "replica": isReplica}, 200
            #Special command: Returns list of replicas.
            if key == 'get_all_replicas':
                return {"result": "success", "replicas": replicas}, 200
            
            #If key is not in dict, return error.
            if key not in d:
                return {'result': 'Error', 'msg': 'Key does not exist', 'node_id': IpPort, 'causal_payload': vClock, 'timestamp': datetime.datetime.now().time()}, 404
            #If key is in dict, return its corresponding value.
            return {'result': 'Success', 'value': d[key], 'node_id': IpPort, 'causal_payload': vClock, 'timestamp': datetime.datetime.now().time()}, 200
        
        #Handles PUT request
        def put(self, key):
            #Special command: Handles adding/deleting nodes.
            if key == 'update_view':
                # Checks to see if ip_port was given in the data payload
                try:
                    ip_payload = request.form['ip_port']
                except:
                    ip_payload = ''

                # Checks to see if request parameter 'type' was given, and what its value is set to
                try:
                    _type = request.args.get('type')
                except: 
                    _type = ''

                # If payload is empty
                if ip_payload == '':
                    return {'result': 'error', 'msg': 'Payload missing'}, 403
                
                # Check if IP is already in our view
                if ip_payload in view:
                    return {'result': 'error', 'msg': 'Ip is already in view'}, 403

                if _type == 'add':
                    print(K)
                    print(len(replicas))
                    sys.stdout.flush()
                    if (len(replicas) < K):
                        # Creates new replica
                        replicas.append(ip_payload)
                        view.append(ip_payload)
                        print("New replica created.")
                        sys.stdout.flush()
                        
                    else:
                        # Creates new 
                        proxies.append(ip_payload)
                        view.append(ip_payload)
                        print("New proxie created.")
                        sys.stdout.flush()
                    return {"msg": "success", "node_id": ip_payload, "number_of_nodes": len(view)}, 200

                if _type == 'remove':
                    # Check to see if IP is in our view
                    if ip_payload not in view:
                        return {'result': 'error', 'msg': 'Cannot remove, IP is not in view'}, 403
                    
                    # Check if replica
                    if replica.index(ip_payload) > 0:
                        removeReplica(ip_payload)
                    # Check if proxie
                    if proxies.index(ip_payload) > 0:
                        removeProxie(ip_payload)
                    # Update Replica/Proxie Ratio if needed
                    updateRatio()
                    return {"msg": "success", "number_of_nodes": len(view)}, 200
                return {'result': 'error', 'msg': 'Request type not valid'}, 403
            
            #Makes sure a value was actually supplied in the PUT.
            try:
                value = request.form['val']
            except:
                value = ''
                pass
            if not value:
                return {'result': 'Error', 'msg': 'No value provided', 'node_id': IpPort, 'causal_payload': vClock, 'timestamp': datetime.datetime.now().time()}, 403
            #Restricts key length to 1<=key<=200 characters.
            if not 1 <= len(str(key)) <= 200:
                return {'result': 'Error', 'msg': 'Key not valid', 'node_id': IpPort, 'causal_payload': vClock, 'timestamp': datetime.datetime.now().time()}, 403
            #Restricts key to alphanumeric - both uppercase and lowercase, 0-9, and _
            if not re.match(r'^\w+$', key):
                return {'result': 'Error', 'msg': 'Key not valid', 'node_id': IpPort, 'causal_payload': vClock, 'timestamp': datetime.datetime.now().time()}, 403

            #Restricts value to a maximum of 1Mbyte.
            if sys.getsizeof(value) > 1000000:
                return {'result': 'Error', 'msg': 'Object too large. Size limit is 1MB', 'node_id': IpPort, 'causal_payload': vClock, 'timestamp': datetime.datetime.now().time()}, 403
            
            vClock[IpPort] += 1
            #If key is not already in dict, create a new entry.
            if key not in d:
                d[key] = value
                return {'replaced': 'False', 'msg': 'New key created', 'node_id': IpPort, 'causal_payload': vClock, 'timestamp': datetime.datetime.now().time()}, 201
            #If key already exists, set replaced to true.
            d[key] = value
            return {'replaced': 'True', 'msg': 'Value of existing key replaced', 'node_id': IpPort, 'causal_payload': vClock, 'timestamp': datetime.datetime.now().time()}, 200
            
        #Handles DEL request
        def delete(self, key):
            #If key is not in dict, return error.
            if key not in d:
                return {'result': 'Error', 'msg': 'Key does not exist', 'node_id': IP, 'causal_payload': vClock, 'timestamp': datetime.datetime.now().time()}, 404

            #If key is in dict, delete key->value pair.
            del d[key]
            return {'result': 'Success', 'node_id': IP, 'causal_payload': vClock, 'timestamp': datetime.datetime.now().time()}, 200
        
    else:
        #Handle requests from forwarding instance.
        def get(self, key):
            #Try requesting primary.
            try:
                response = requests.get(http_str + mainAddr + '/kv-store/' + key)
            except requests.exceptions.RequestException as exc: #Handle primary failure upon get request.
                return {'result': 'Error','msg': 'Server unavailable', 'node_id': IP, 'causal_payload': vClock, 'timestamp': datetime.datetime.now().time()}, 500
            return response.json()
        
        def put(self,key):
            #Makes sure a value was actually supplied in the PUT.
            try:
                value = request.form['val']
            except:
                value = ''
                pass
            if not value:
                return {'result': 'Error', 'msg': 'No value provided', 'node_id': IP, 'causal_payload': vClock, 'timestamp': datetime.datetime.now().time()}, 403
        #Try requesting primary.
            try:
                response = requests.put((http_str + mainAddr + '/kv-store/' + key), data = {'val': value})
            except requests.exceptions.RequestException as exc: #Handle primary failure upon put request.
                return {'result': 'Error','msg': 'Server unavailable', 'node_id': IP, 'causal_payload': vClock, 'timestamp': datetime.datetime.now().time()}, 500
            return response.json()

        def delete(self, key):
            #Try requesting primary.
            try:
                response = requests.delete(http_str + mainAddr + '/kv-store/' + key)
            except requests.exceptions.RequestException as exc: #Handle primary failure upon delete request.
                return {'result': 'Error','msg': 'Server unavailable', 'node_id': IP, 'causal_payload': vClock, 'timestamp': datetime.datetime.now().time()}, 500
            return response.json()
api.add_resource(Handle, '/kv-store/<key>')

if __name__ == "__main__":
    localAddress = IpPort.split(":")
    app.run(host=localAddress[0], port=localAddress[1])