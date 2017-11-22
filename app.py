#-*- coding: utf-8 -*-
#app.py for project 3, CMPS128

from flask import Flask
from flask import request, abort, jsonify, json
from flask_restful import Resource, Api
import re, sys, os, requests, datetime, threading, random

app = Flask(__name__)
api = Api(app)
newline = "&#13;&#10;"
# Debug printing boolean.
debug = True

# Get expected environment variables.
K = int(os.environ.get('K'))
IpPort = os.environ.get('IPPORT')
EnvView = os.environ.get('VIEW')

# Is this a replica or a proxy?
isReplica = False
# Dictionaries acting as a vector clock and timestamps. key -> local clock value/timestamp.
vClock = {}
timestamps = {}
# String to prepend onto URL.
http_str = 'http://'
kv_str = '/kv-store/'

# Dictionary where key->value pairs will be stored.
d = {}
# Arrays to store replicas and proxies.
view = []
notInView = [] # Keep track of nodes not in view to see if they're back online.
replicas = []
proxies = []

# Initialize view array based on Environment Variable 'VIEW'
view = EnvView.split(",")
pos = view.index(IpPort)
# Initialize this node as a replica or a proxy.
for i in view:
    if view.index(i) < K:
        replicas.append(i)
    else:
        proxies.append(i)
if IpPort is in replicas:
    isReplica = True

#function to compare 2 vector clocks.
#return value: -1 -> clock1 smaller, 0 -> concurrent, 1 -> clock2 smaller
def compareClocks(clock1, clock2):
    compareResult = 0
    if clock1.len() == clock2.len():
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
    notInView.append(ip)
    if debug:
        print("Replica: " + ip + " removed.")

def removeProxie(ip):
    proxies.remove(ip)
    view.remove(ip)
    notInView.append(ip)
    if debug:
        print("Proxie: " + ip + " removed.")

def heartBeat(dead, heart):
    heart = threading.Timer(5.0, heartBeat)
    heart.daemon = True
    heart.start()
    if debug:
        print "Heartbeat"
        sys.stdout.flush()
    for ip in view:
        if ip != IpPort:
            try:
                response = (requests.get(http_str + ip + kv_str + "get_node_details")).json()
                if response['result'] == 'success':
                    if (response['replica'] == 'Yes') and (ip not in replicas) : #add ip to replica list if needed
                        replicas.append(ip)
                        if ip in proxies:
                            removeProxie(ip)
                    elif (response['replica'] == 'No') and (ip in replicas) : #remove from replicas list if needed
                        removeReplica(ip)
                        if ip not in proxies:
                            proxies.append(ip)
            except requests.exceptions.RequestException: #Handle no response from ip
                if ip in replicas:
                    removeReplica(ip)
                if ip in proxies:
                    removeProxie(ip)
    for ip in notInView: #check if any nodes not currently in view came back online
        try:
            response = (requests.get(http_str + ip + kv_str + "get_node_details")).json()
            if response['result'] == 'success':
                if response['replica'] == 'Yes' : #add to replicas if needed
                    replicas.append(ip)
                    view.append(ip)
                elif response['replica'] == 'No' : #add to proxies if needed
                    proxies.append(ip)
                    view.append(ip)
                notInView.remove(ip)
                #call functions to resolve partitions at this point because if response from ip in notInView is a success, then a "dead" node is back
                #gossip between replicas to sync different kvs
                for key in d:
                    requests.put((http_str + ip + kv_str + key), data = {'val': value, 'causal_payload': vClock[key], 'timestamp': timestamps[key]})
        except requests.exceptions.RequestException: #Handle no response from ip
            pass

def updateView(self, key):
    # Checks to see if ip_port was given in the data payload.
    try:
        ip_payload = request.form['ip_port']
    except:
        ip_payload = ''

    # Checks to see if request parameter 'type' was given, and what its value is set to.
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
        if ip_payload in view:
            return {'result': 'error', 'msg': 'Node already in view'}, 403
        if debug:
            print(K)
            print(len(replicas))
        
        # Creates new replica.
        if len(replicas) < K:
            replicas.append(ip_payload)
            view.append(ip_payload)
            if ip_payload in notInView:
                notInView.remove(ip_payload)
            requests.put(http_str + ip_payload + kv_str + '_update!', data = {"view": view, "notInView": notInView, "replicas": replicas, "proxies": proxies})
            if debug:
                print("New replica created.")
                sys.stdout.flush()
        # Creates new proxy.
        else:
            proxies.append(ip_payload)
            view.append(ip_payload)
            if ip_payload in notInView:
                notInView.remove(ip_payload)
            if debug:
                print("New proxie created.")
                sys.stdout.flush()
        return {"msg": "success", "node_id": ip_payload, "number_of_nodes": len(view)}, 200

    if _type == 'remove':
        # Check to see if IP is in our view.
        if ip_payload not in view:
            return {'result': 'error', 'msg': 'Node is not in view'}, 403
        
        # Check if replica.
        if replica.index(ip_payload) > 0:
            removeReplica(ip_payload)
        # Check if proxie.
        if proxies.index(ip_payload) > 0:
            removeProxie(ip_payload)
        # Update Replica/Proxie Ratio if needed.
        updateRatio()
        return {"msg": "success", "number_of_nodes": len(view)}, 200
    return {'result': 'error', 'msg': 'Request type not valid'}, 403

def updateRatio():
    # If Replicas is less than K...
    if len(replicas) < K:
        # Try to convert proxie to replica.
        if len(proxies) > 0:
            tempNode = proxies[-1]
            replicas.append(tempNode)
            removeProxie(tempNode)
            requests.put(http_str + tempNode + kv_str + '_update!', data = {"view": view, "notInView": notInView, "replicas": replicas, "proxies": proxies})
    # If more replicas then needed, convert to proxie.
    if len(replicas) > K:
        tempNode = replicas[-1]
        proxies.append(tempNode)
        removeReplica(tempNode)

#read-repair function
def readRepair(key):
    for ip in replicas:
        try:
            response = requests.get(http_str + ip + kv_str + key)
            if response[causal_payload] > vClock[key]:
                d[key] = response[value]
                vClock[key] = response[causal_payload]
                timestamps[key] = response[timestamp]
        except requests.exceptions.RequestException as exc: #Handle no response from ip
            removeReplica(ip)

def broadcastKey(key, value, payload, time):
    for address in replicas:
        if address is not IpPort:
            try:
                requests.put((http_str + address + kv_str + key), data = {'val': value, 'causal_payload': payload, 'timestamp': time})
            except request.exceptions.RequestException:
                removeReplica(address)

class Handle(Resource):
    if isReplica:
        #Handles GET request
        def get(self, key):
            #Special command: Returns if node is a replica.
            if key == 'get_node_details':
                answer = 'No'
                if isReplica:
                    answer = 'Yes'
                return {"result": "success", "replica": answer}, 200
            #Special command: Returns list of replicas.
            if key == 'get_all_replicas':
                return {"result": "success", "replicas": replicas}, 200
            
            #If key is not in dict, return error.
            if key not in d:
                return {'result': 'Error', 'msg': 'Key does not exist'}, 404
            
            clientRequest = False
            #Get attached timestamp, or set it if empty.
            try:
                timestamps[key] = request.form['timestamp']
            except:
                timestamps[key] = ''
            if timestamps[key] is '':
                timestamps[key] = int(datetime.datetime.now().time())
                clientRequest = True
            
            #Handle a new causality chain.
            try:
                causalPayload = request.form['causal_payload']
            except:
                causalPayload = ''
                pass
            if causalPayload is '':
                if vClock[key] is None:
                    vClock[key] = 0
            #Handle early get requests.
            if causal_payload > vClock[key]:
                readRepair(key)
                #if causal_payload > vClock[key]:
                #    return {'result': 'Error', 'msg': 'Server unavailable'}, 500
                vClock[key] = causal_payload
            
            #Increment vector clock when client get operation succeeds.
            if clientRequest:
                vClock[key] += 1
            #If key is in dict, return its corresponding value.
            return {'result': 'Success', 'value': d[key], 'node_id': IpPort, 'causal_payload': vClock[key], 'timestamp': timestamps[key]}, 200
        
        #Handles PUT request
        def put(self, key):
            #Special command: Handles adding/deleting nodes.
            if key == 'update_view':
                updateView(self, key)
            #Special command: Force read repair.
            if key == '_update!':
                try:
                    view = request.form['view']
                    notInView = request.form['notInView']
                    replicas = request.form['replicas']
                    proxies = request.form['proxies']
                except:
                    return {"result": "Error", 'msg': 'System command parameter error'}, 403
                for key in d:
                    readRepair(key)
                    return {"result": "success"}, 200
            
            #Makes sure a value was actually supplied in the PUT.
            try:
                value = request.form['val']
            except:
                value = ''
                pass
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
            
            clientRequest = False
            #Get attached timestamp, or set it if empty.
            try:
                timestamp = request.form['timestamp']
            except:
                timestamp = ''
            if timestamp is '':
                timestamp = int(datetime.datetime.now().time())
                clientRequest = True
            
            try:
                causalPayload = request.form['causal_payload']
            except:
                causalPayload = ''
            #If causal payload is none, and replica key is none initialize payload to 0 and set key value.
            if causalPayload is '':
                if vClock[key] is None:
                    vClock[key] = 0
            .
            if causal_payload >= vClock[key]:
                #Actually set the value
                if causal_payload == vClock[key]:
                    if timestamps[key] < timestamp:
                        d[key] = value
                    elif timestamps[key] == timestamp:
                        if d[key] < value
                            d[key] = value
                else:
                    d[key] = value
                    
                #Handle early put requests.
                if causal_payload > vClock[key]:
                    vClock[key] = causal_payload
                
                if clientRequest:
                    #Increment vector clock when client put operation succeeds.
                    vClock[key] += 1
                    timestamps[key] = timestamp
                    broadcastKey(key, value, vClock[key], timestamps[key])
            #If key is not already in dict, create a new entry.
            if key not in d:
                return {'replaced': 'False', 'msg': 'New key created', 'node_id': IpPort, 'causal_payload': vClock[key], 'timestamp': timestamps[key]}, 201
            #If key already exists, set replaced to true.
            return {'replaced': 'True', 'msg': 'Value of existing key replaced', 'node_id': IpPort, 'causal_payload': vClock[key], 'timestamp': timestamps[key]}, 200
            
        #Handles DEL request
        def delete(self, key):
            #If key is not in dict, return error.
            if key not in d:
                return {'result': 'Error', 'msg': 'Key does not exist'}, 404

            #If key is in dict, delete key->value pair.
            del d[key]
            return {'result': 'Success', 'node_id': IP, 'causal_payload': vClock[key], 'timestamp': timestamps[key]}, 200
        
    else:
        #Handle requests from forwarding instance.
        def get(self, key):
            #Try to retrieve timestamp and cp of read request.
            try:
                timestamp = request.form['timestamp']
            except:
                timestamp = ''
                pass
            try:
                causalPayload = request.form['causal_payload']
            except:
                causalPayload = ''
                pass
            #Try requesting random replicas
            noResp = True
            while noResp:
                if replicas.len < 1:
                    return {'result': 'Error', 'msg': 'Server unavailable'}, 500
                repIp = random.choice(replicas)
                try:
                    response = requests.get(http_str + repIp + '/kv-store/' + key, data={'causal_payload': causalPayload, 'timestamp': timestamp})
                except requests.exceptions.RequestException as exc: #Handle replica failure
                    removeReplica()
                    continue
                noResp = False
            return response.json()
        
        def put(self, key):
            #Makes sure a value was actually supplied in the PUT.
            try:
                value = request.form['val']
            except:
                value = ''
                pass
            if not value:
                return {'result': 'Error', 'msg': 'No value provided'}, 403
            #Try to retrieve timestamp and cp of read request.
            try:
                timestamp = request.form['timestamp']
            except:
                timestamp = ''
                pass
            try:
                causalPayload = request.form['causal_payload']
            except:
                causalPayload = ''
                pass
            #Try requesting random replicas
            noResp = True
            while noResp:
                if replicas.len < 1:
                    return {'result': 'Error', 'msg': 'Server unavailable'}, 500
                repIp = random.choice(replicas)
                try:
                    response = requests.put((http_str + repIp + kv_str + key), data = {'val': value, 'causal_payload': causalPayload, 'timestamp': timestamp})
                except requests.exceptions.RequestException as exc: #Handle replica failure
                    removeReplica()
                    continue
                noResp = False
            #Try requesting primary.
            return response.json()

        def delete(self, key):
            #Try requesting primary.
            try:
                response = requests.delete(http_str + mainAddr + kv_str + key)
            except requests.exceptions.RequestException as exc: #Handle primary failure upon delete request.
                return {'result': 'Error', 'msg': 'Server unavailable'}, 500
            return response.json()
api.add_resource(Handle, '/kv-store/<key>')

if __name__ == "__main__":
    localAddress = IpPort.split(":")
    heartBeat()
    app.run(host=localAddress[0], port=localAddress[1])