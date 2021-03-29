from flask import Flask, jsonify, request
import logging
import requests
import threading
import hashlib
import time
import sys
import glob
import socket
import math
import os.path
from os import path
import shutil

app = Flask(__name__)
db = dict()
db['id'] = 56
db['files'] = 'test.txt'
db['count'] = 0
fTable = dict()
ftable = dict()
hashTable = dict()
rtable = dict()
replicaTable = dict()
chord_id = -1
chord_url=''
ip = ''
leader_id = -1
leaderURL = ''
electionCache = dict()  # initiator_id : initiated_time
successorId = -1
successorURl = ''
deletionList = []
r = 2
rlist = list()


def removeRemoveElement(idG):
    global ftable
    global successorId
    global successorURl
    return 1
    if idG in ftable:
        ftable.pop(idG)
    else:
        return 1
    for el in sorted(ftable.keys()):
        if el > chord_id and el != -1:
            successorId = el
            successorURl = ftable[successorId]
            break
    if len(ftable.keys()) > 1:
        if chord_id >= max(ftable.keys()):
            successorId = min(ftable.keys())
            successorURl = ftable[successorId]

def buildRList():
    print(f'\n\n\n\n\nBuilding Rlist\n\n\n\n\n')
    global ftable
    global rtable
    global r
    global hashTable
    global rlist
    try:
        #time.sleep(5)
        # if num>5:
        # num=0
        # ftable=dict()

        # successorId=min(rtable.keys())
        global leader_id
        global successorId
        global successorURl
        global chord_id
        global chord_url
        tempId = chord_id
        templ = list()
        print(f'len of ftable:{len(list(ftable.keys()))}')
        if len(list(ftable.keys()))>1:
            fkeys = sorted(list(ftable.keys()))
            if chord_id in fkeys:
                fkeys.remove(chord_id)
            templ.append({'id': successorId, 'url': successorURl})
            tempURL = successorURl
            for i in range(r-1):
                print(f'inside for loop')
                #if i >= len(fkeys):
                #    break
                #nodeId = fkeys[i]
                
                #url = 'http://' + ftable[nodeId] + '/getNext'
                url = 'http://' + tempURL + '/getNext'
                print(url)
                data = ''
                
                resp = requests.get(url=url)
                data = resp.json()
                if(int(data['id']) == chord_id):
                    continue
                print(data)
                templ.append({'id': int(data['id']), 'url': data['url']})
                tempURL = data['url']
            
            prev_succ = [d['id'] for d in rlist]
            new_succ = [d['id'] for d in templ]

            print(f'\n\n\n prev succ: {prev_succ}\n\n new succ: {new_succ}')

            for old in prev_succ:
                if old not in new_succ and old != -1:
                    print(f'Sending delete replica to {old}')
                    ip = rlist[next((i for i, item in enumerate(rlist) if item['id'] == old), None)]['url']
                    url = 'http://' + ip + '/delete?id=' + str(int(chord_id))
                    try:
                        resp = requests.delete(url=url)
                    except Exception as e:
                        print(f'Error while resetting replica {e}')

            rlist = templ

    except Exception as e:
        print(f'Exception occured while building r list: {e}')
    
    return

def buildFTable():
    global ftable
    global rtable
    global r
    global hashTable
    global rlist
    time.sleep(5)
    num = 0
    while True:
        try:

            time.sleep(5)
            # if num>5:
            # num=0
            # ftable=dict()
            #if len(ftable.keys())<2:
                #continue
            # successorId=min(rtable.keys())

            global leader_id
            global leaderURL
            global successorId
            global successorURl
            global chord_id
            global chord_url
            global deletionList
            newsucc = -1
            print("in build f table")
            print("succ id is")
            print(successorId)
            print("old ftable")
            print(ftable)
            print("full table")
            print(fTable)
            print(deletionList)
            # tempdict=dict()
            if len(ftable.keys())<2:
                ftable[leader_id]=leaderURL
               


            for el in deletionList:
                # if el not in tempdict:
                # tempdict[el]=1
                # else:
                # continue
                #url = 'http://' + ftable[el] + '/getSuccessor?id=' + str(int(chord_id + 1))
                if el in ftable and el != chord_id:
                    try:

                        url = 'http://' + ftable[el] + '/getSuccessor?id=' + str(int(chord_id + 1))

                        resp = requests.get(url=url)
                        deletionList.remove(el)
                        continue
                    except:
                        print("failed to connect deleting")



                if el == chord_id:
                    deletionList.remove(el)
                    continue

                if el not in ftable:
                    deletionList.remove(el)
                else:
                    print('before')
                    print(ftable)
                    ftable.pop(el)
                    print('after')
                    print(ftable)
                    deletionList.remove(el)

                    #if el in rtable.keys():
                    #    url = 'http://' + rtable[el] + '/delete?id=' + str(int(chord_id))
                    #    try:
                    #        resp = requests.delete(url=url)
                    #    except Exception as e:
                    #        print(f'Error while resetting replica {e}')
                    #    
                    #    rtable.pop(el)
                        
                    for el2 in sorted(ftable.keys()):
                        if el2 > chord_id and el2 != -1 and el2 != el:
                            print('resetting successor')
                            successorId = el2
                            successorURl = ftable[successorId]
                            print(successorId)
                            break
            deletionList = []

            # if successorId ==-1 or successorId not in ftable:
            for el in sorted(ftable.keys()):
                if el > chord_id and el != -1:
                    successorId = el
                    successorURl = ftable[successorId]
                    break
            if len(ftable.keys()) == 2 and successorId == -1:
                successorId = min(ftable.keys())
                successorURl = ftable[successorId]
            if len(ftable.keys()) > 1:
                if chord_id >= max(ftable.keys()):
                    successorId = min(ftable.keys())
                    successorURl = ftable[successorId]

            # url = 'http://'+fTable[successorId]+'/getSuccessor?id='+str(chord_id)
            # resp = requests.get(url=url)
            # data = resp.json()
            # print("response data is")
            # print(data)
            # print(data['id'])
            # print("in ftable loop")
            # print(data['url'])
            newDict = dict()

            newDict[successorId]=ftable[successorId]
            #newDict[min(ftable.keys())]=ftable[min(ftable.keys())]
            #dontlist=[]

            for i in range(0, 8):
                succ = (chord_id + math.pow(2, i)) % math.pow(2, 8)
                # url = 'http://'+fTable[successorId]+'/getSuccessor?id='+str(int(succ))
                nid = successorId
                for gol in sorted(ftable.keys()):
                    #if gol >= succ and gol != chord_id and  gol not in deletionList:
                    if gol >succ:
                        break
                    #if gol <= succ and gol != chord_id and  gol not in deletionList:
                    if gol <= succ  and gol != chord_id and  gol not in deletionList:
                        nid = gol
                        #brea
                        

                # url = 'http://'+ftable[successorId]+'/getSuccessor?id='+str(int(succ))
                print("nid is")
                print(nid)
                if nid not in ftable:
                    nid= min(ftable.keys())
                if nid in deletionList:
                    for gol in sorted(ftable.keys()):
                        if gol not in deletionList:
                            nid=gol

                url = 'http://' + ftable[nid] + '/getSuccessor?id=' + str(int(succ))
                print(url)
                data = ''
                try:
                    resp = requests.get(url=url)
                    data = resp.json()
                    #if (int(data['id']) == chord_id):
                    #    continue
                    print(data)
                    url = 'http://' +data['url'] + '/getSuccessor?id=' + str(int(chord_id + 1))

                    resp = requests.get(url=url)
                    
                    ftable[int(data['id'])] = data['url']
                    newDict[int(data['id'])] = data['url']
                    if i == 0:
                        newsucc = int(data['id'])
                except Exception as e:
                    deletionList.append(nid)
                    print("error connecting")
                    print(e)
                    #removeRemoveElement(nid)
                    continue

                # data = resp.json()
                # print(data)
                # ftable[int(data['id'])]= data['url']
                # if i==0:
                # newsucc=int(data['id'])
                # successorId= int(data['id'])
            print("building r table")
            rtable = dict()
            newrtable = dict()
            tempId=-1
            if successorId in ftable:
                rtable[successorId] = ftable[successorId]
                #newrtable[int(data['id'])] = data['url']
                newrtable[successorId] = ftable[successorId]
                tempId = successorId
            else:
                newK= min(ftable.keys())
                rtable[newK]= ftable[newK]
                tempId=newK
            print(r)
            for i in range(r):
                url = 'http://' + rtable[tempId] + '/getSuccessor?id=' + str(int(tempId + 1))
                print(url)
                data = ''
                try:
                    resp = requests.get(url=url)
                    data = resp.json()
                    #if(int(data['id']) == chord_id):
                    #    continue
                    print(data)
                    rtable[int(data['id'])] = data['url']
                    newrtable[int(data['id'])] = data['url']
                    tempId = int(data['id'])
                    if leader_id<int(data['id']):
                        leader_id=int(data['id'])
                        leaderURL=data['url']
                except:
                    deletionList.append(tempId)
                    removeRemoveElement(tempId)
                    print("error connecting")
                    continue
            buildRList()
            for succ in rlist:
                #break

                for el in glob.glob("static/*.*"):
                    hash_object = hashlib.sha1(bytes(el, 'utf-8'))
                    pbHash = hash_object.hexdigest()
                    file_id = int(pbHash, 16) & 0xFF
                    hashTable[file_id] = el
                    #if rel>chord_id:
                    if (1):
                        #url = 'http://' + rtable[rel] + '/getSuccessor?id=' + str(int(file_id))
                        #print(url)
                        #data = ''
                        #try:
                            #resp = requests.get(url=url)
                        #except:
                            #deletionList.append(rel)
                            #removeRemoveElement(rel)
                            #print("error connecting")
                            #continue

                        #data = resp.json()
                        #newul = data['url']
                        #url = 'http://' + rtable[rel] + '/putFile'
                        url = 'http://' + succ['url'] + '/putReplica'
                        print(url)
                        print(el)
                        files = {'file': open(el, 'rb')}
                        values = {}
                        payload=dict()
                        #payload['noSend'] = rel
                        payload['r_value'] = chord_id
                        payload['id'] = file_id
                        try:
                            resp = requests.post(url, files=files, data=values,params=payload)
                        except Exception as e:
                            deletionList.append(rel)
                            removeRemoveElement(rel)
                            print(e)
                            print("error connecting")

            print("final f table is")
            print(ftable)
            print("final r table is")
            print(rtable)
            print("deletion list")
            print(deletionList)
            # successorId=newsucc
            print(f"chord is {chord_id}")
            print(f"successor: {successorId}")
            # successorId = min(ftable.keys())
            print("making important backup")
            for el in glob.glob("static/*.*"):
                hash_object = hashlib.sha1(bytes(el, 'utf-8'))
                pbHash = hash_object.hexdigest()
                file_id = int(pbHash, 16) & 0xFF
                hashTable[file_id] = el
                if (file_id > chord_id and successorId>chord_id) or (file_id<chord_id and successorId<chord_id) or (file_id>leader_id):

                    url = 'http://' + ftable[successorId] + '/getSuccessor?id=' + str(int(file_id))
                    print(url)
                    data = ''
                    try:
                        resp = requests.get(url=url)
                    except:
                        deletionList.append(successorId)
                        removeRemoveElement(successorId)
                        print("error connecting")
                        continue

                    data = resp.json()
                    newul = data['url']
                    url = 'http://' + newul + '/putFile'
                    print(url)
                    print(el)
                    files = {'file': open(el, 'rb')}
                    values = {}
                    payload=dict()
                    payload['noSend'] = 1
                    try:
                        resp = requests.post(url, files=files, data=values,params=payload)
                    except:
                        deletionList.append(int(data['id']))
                        removeRemoveElement(int(data['id']))
                        print("error connecting")
            if successorId == -1:
                for el in sorted(ftable.keys()):
                    if el > chord_id and el != -1:
                        successorId = el
                        successorURl = ftable[successorId]
                        break
                if len(ftable.keys()) == 2 and successorId == -1:
                    successorId = min(ftable.keys())
                    successorURl = ftable[successorId]
            if chord_id >= max(ftable.keys()):
                successorId = min(ftable.keys())
                successorURl = ftable[successorId]
            # global fTable
            num = num + 1
            if num > 5 or 1==1:
                num = 0
                succUrl = ftable[successorId]
                # payload = dict()
                for el in newDict.keys():
                    if el in deletionList and el in newDict.keys():
                        newDict.pop(el)
                print("new dict")
                print(newDict)
                print("before")
                print(ftable)
                        
                ftable = dict(newDict)
                ftable[successorId] = succUrl
                print("after")
                print(ftable)

                rtable=dict(newrtable)
                # payload['id']=idGotten
                # payload['url']= urlGotten
                payload = dict()
                payload['id'] = chord_id
                payload['url'] = chord_url
                print("payload is")
                print(payload)
                # if leader != chord_id:
                #try:
                #url = 'http://' + ftable[max(ftable.keys())] + '/addEntry'
                url = 'http://' + leaderURL + '/addEntry'
                print(url)
                try:
                    resp = requests.post(url=url, json=payload)
                except:
                    print("error connecting to leader")
                    url = 'http://' + successorURl + '/election'
                    print(f'POST {url} json=id: {chord_id}')
                    requests.post(url=url, json={'id_arr': [chord_id]})

            elif 0 == 1:
                for el in ftable.keys():
                    payload = dict()
                    payload['id'] = chord_id
                    payload['url'] = chord_url
                    print("payload is")
                    print(payload)
                    url = 'http://' + ftable[el] + '/addEntry'

                    print('search url is' + url)
                    try:
                        resp = requests.post(url=url, json=payload)
                    except:
                        print("error making request")
                        removeRemoveElement(el)
                        deletionList.append(el)

                # ftable=dict()
                # ftable[successorId]=succUrl
        except Exception as e:
            print(e)
            # ftable=dict()
            # ftable[successorId]=succUrl



def election_thread():  # initiate election
    while True:
        time.sleep(15)
        print("election_thread()")
        global chord_id
        global leader_id
        global electionCache
        if successorId != -1:
            try:  # regular election to ensure the correct leader
                url = 'http://' + successorURl + '/election'
                if "election_time" in electionCache:
                    now = time.time()
                    diff = int(now - electionCache["election_time"])
                    if diff > 5 or chord_id > electionCache["initiator_id"]:
                        electionCache["initiator_id"] = chord_id
                        electionCache["election_time"] = time.time()
                        print(f'POST {url} json=id: {chord_id}')
                        requests.post(url=url, json={'id_arr': [chord_id]})
                    else:
                        print("discard initiating election: Last election was < 5s & last initiator_id has larger ID.")
                else:
                    electionCache["initiator_id"] = chord_id
                    electionCache["election_time"] = time.time()
                    print(f'POST {url} json=id: {chord_id}')
                    requests.post(url=url, json={'id_arr': [chord_id]})
            except Exception as e:
                print(f'ERROR: {e}')
        else:
            leader_id = chord_id
            print(f"No successor, leader set to itself: {leader_id}")


def announce_leader(id_, id_arr):
    global successorId
    global chord_id
    global chord_url
    global leader_id
    print(f'announce_leader({id_}, {id_arr}): successorId: {successorId}  leader: {leader_id}')

    if successorId != -1:
        url = 'http://' + ftable[successorId] + '/leader'
        print(f'POST {url} json= id_: {id_}, id_arr: {id_arr}')
        try:
            resp = requests.post(url=url, json={'id_': id_, 'id_arr': id_arr})
            # print(resp)
        except Exception as e:
            removeRemoveElement(successorId)
            print(f"announce_leader({id_}, {id_arr}): error connecting")
            print(f'ERROR: {e}')
    else:
        print("WARNING: Cannot announce leader, no successor.")

    responsedict = dict()
    return jsonify(responsedict)


def elect(id_arr):
    global successorId
    print(f'elect({id_arr}): successorId: {successorId}  current leader: {leader_id}')

    if successorId != -1:
        url = 'http://' + ftable[successorId] + '/election'
        print(f'POST {url} json=id_arr: {id_arr}')
        try:
            resp = requests.post(url=url, json={'id_arr': id_arr})
            # print(resp)
        except Exception as e:
            removeRemoveElement(successorId)
            print(f"elect({id_arr}): error connecting")
            print(f'ERROR: {e}')
    else:
        print("WARNING: Cannot elect, no successor.")

    responsedict = dict()
    return jsonify(responsedict)


def receive_election_message(id_arr):
    global chord_id
    print(f'receive_election_message({id_arr}), chord_id: {chord_id}')
    if chord_id in id_arr:
        print(f'Election message went around a circle, election terminated, announce leader: {max(id_arr)}')
        global leader_id
        leader_id = max(id_arr)
        print(f"leader set to: {leader_id}")
        announce_leader(leader_id, [chord_id])
    else:
        id_arr.append(chord_id)
        elect(id_arr)


def receive_leader_message(id_, id_arr):
    global chord_id
    print(f"receive_leader_message({id_}, {id_arr}), this node's chord_id: {chord_id}")
    if chord_id in id_arr:
        print('Leader announcement message went around a circle')
        if id_ in id_arr:
            print(f'Leader announcement terminated, all nodes set their leaders to: {id_}')
        else:
            print(f"leader is not in the network, re-initiate election")
            elect([chord_id])
    else:  # continue to announce leader
        global leader_id
        leader_id = id_
        print(f"leader set to: {leader_id}")
        id_arr.append(chord_id)
        announce_leader(id_, id_arr)


@app.route('/')
def index():
    global chord_id
    global chord_url
    global leader_id
    global leaderURL
    global successorId
    global successorURl
    global ftable
    global rtable
    global hashTable
    global rlist
    html = f'''
        <!doctype html>
        <html>
            <head>
                <title>chord_id: {chord_id}</title>
            </head>
            <body>
                <h1>Chord ID: {chord_id}</h1>
                <p>Chord URL: {chord_url}</p>
                <p>Leader ID: {leader_id}</p>
                <p>Leader URL: {leaderURL}</p>
                <p>Successor ID : {successorId}</p>
                <p>Successor URL : {successorURl}</p>
                <p>Finger Table: {str(ftable)}</p>
                <p>Hash Table: {str(hashTable)}</p>
                <p>Replica Hash Tables: {str(replicaTable)}</p>
                <p>Successor List: {str(rlist)}<p>
            </body>
        </html>
    '''
    return html

'''
@app.route('/getDb')
def dbTest():
    print("self finger table is")
    print(fTable)
    idReq = request.args.get('id')
    print("is id " + str(idReq))
    # db['count']=db['count']+1
    responsedict = dict()
    responsedict['url'] = fTable[int(idReq)]
    responsedict['id'] = idReq
    # responsedict['ip']=
    return jsonify(responsedict)
'''

@app.route('/getFingerEntry')
def dbTest1():
    print("self finger table is")
    print(fTable)
    idReq = request.args.get('id')
    print("is id " + str(idReq))
    # db['count']=db['count']+1
    responsedict = dict()
    #responsedict['url'] = fTable[int(idReq)]
    responsedict['url'] = ftable[int(idReq)]
    responsedict['id'] = idReq
    # responsedict['ip']=
    return jsonify(responsedict)


@app.route('/election', methods=['POST'])
def election():
    print(f"Received POST /election post_data: {request.get_json()['id_arr']}")
    global chord_id
    global leader_id
    global electionCache
    if request.method == 'POST':
        if "initiator_id" in electionCache and request.get_json()['id_arr'][0] < electionCache["initiator_id"]:
            now = time.time()
            diff = int(now - electionCache["election_time"])
            if diff > 5:
                electionCache["initiator_id"] = chord_id
                electionCache["election_time"] = time.time()
                receive_election_message(request.get_json()['id_arr'])
                return 'electing...'
            else:
                print("discard election: Last election was within 5 seconds")
                return 'discard election.'
        else:
            electionCache["initiator_id"] = request.get_json()['id_arr'][0]
            electionCache["election_time"] = time.time()
            receive_election_message(request.get_json()['id_arr'])
            return 'electing...'

@app.route('/delete', methods=['DELETE'])
def delete_replica():
    idGotten = int(request.args.get('id'))
    print(f"Received replica deletion from predecessor {id}")
    global replicaTable
    dir_path = 'static/bkp/'+str(int(idGotten))
    if idGotten in replicaTable.keys():
        replicaTable.pop(idGotten)
        if (os.path.exists(dir_path)):
            try:
                shutil.rmtree(dir_path)
                return '1'
            except OSError as e:
                print("Error: %s : %s" % (dir_path, e.strerror))
                return '0'

    else:
        print(replicaTable)
        print(f'predecessor {idGotten} not found in replicaTable')
        return '0'
    


@app.route('/leader', methods=['GET', 'POST'])
def leader():
    if request.method == 'POST':
        print(f"received POST /leader   post_data: {request.get_json()['id_']}, {request.get_json()['id_arr']}")
        receive_leader_message(request.get_json()['id_'], request.get_json()['id_arr'])
        return 'announcing leader...'
    else:
        global leader_id
        global leaderURL
        print(f"received GET /leader    Return: leader_id: {leader_id}, leaderURL: {leaderURL}")
        responsedict = dict()
        responsedict['id'] = leader_id
        responsedict['url'] = leaderURL
        return jsonify(responsedict)


@app.route('/addEntry', methods=['POST'])
def getAddEntry():
    print('in add entry')
    global leader_id
    global successorId
    global successorURl
    global chord_id
    global chord_url

    print(leader_id)
    print("self finger table is")
    print(request.get_json()['id'])
    print(request.get_json()['url'])
    idGotten = int(request.get_json()['id'])
    urlGotten = request.get_json()['url']
    # if idGotten in fTable or idGotten in ftable:
    # return "error change port no"

    print('id gotten ')
    print(idGotten)
    print('url gotten')
    print(urlGotten)
    fTable[int(idGotten)] = urlGotten
    ftable[int(idGotten)] = urlGotten
    if len(ftable.keys()) == 2 and chord_id > idGotten:
        successorId = idGotten
        successorURl = ftable[successorId]
    # for el in sorted(fTable.keys()):
    # if

    # if idGotten < max(fTable.keys()) :leader=max(fTable.keys())
    # if leader != chord_id:
    # if idGotten> chord_id:
    '''
    if leader < max(fTable.keys()) or successorId == -1 :
        #time.sleep(5)
        leader=max(fTable.keys())
        #url = 'http://'+sys.argv[2]+':'+sys.argv[3]+'/getDb?id='+str(leader)
        #url = 'http://'+sys.argv[2]+':'+sys.argv[3]+'/getLeader'
        #url = 'http://'+fTable[leader]+'/getSuccessor?id='+str(chord_id+1)
        url = 'http://'+urlGotten+'/getSuccessor?id='+str(chord_id+1)
        print('search url is'+url)
        resp = requests.get(url=url)
        data = resp.json()
        print("response data is")
        print(data)
        print(data['id'])
        print(data['url'])
        #global chord_id
        print("chord id is")
        print(chord_id)
        #if data['id'] != chord_id
        successorId=data['id']
        successorURl=data['url']
        #print(data['id'])
        #fTable[data['id']]=data['ip']+':'+data['port']
        open("db.json",'wb').write(resp.content)
        print('new finger table is')
        print(fTable)
        fTable[int(data['id'])]=data['url']
        print(fTable)
        leader=max(fTable.keys())
    '''
    if successorId != -1 and successorId != chord_id and len(ftable) > 2 and successorId > chord_id:
        payload = dict()
        payload['id'] = idGotten
        payload['url'] = urlGotten
        print("payload is")
        print(payload)
        url = 'http://' + ftable[successorId] + '/addEntry'

        print('search url is' + url)
        try:
            resp = requests.post(url=url, json=payload)
        except:
            print("error making request in add entry")
            deletionList.append(successorId)
            removeRemoveElement(successorId)
    for el in sorted(ftable.keys()):
        if el > chord_id:
            successorId = el
            successorURl = ftable[successorId]
            break
    print(fTable)
    # buildFTable()

    return jsonify(fTable)


@app.route('/getfTable')
def getFTable():
    print("self finger table is")
    # print(fTable)
    # idReq = request.args.get('id')
    # print("is id "+str(idReq))
    ##db['count']=db['count']+1
    # responsedict=dict()
    # responsedict['url']= fTable[int(idReq)]
    # responsedict['id']= idReq
    #return jsonify(fTable)
    return jsonify(ftable)

@app.route('/getNext')
def getNext():
    global leader_id
    global chord_id
    global chord_url
    global successorId
    global ftable
    

    if successorId in ftable:
        retdict = dict()
        retdict['id'] =  successorId
        retdict['url'] = ftable[successorId]
        return jsonify(retdict)
    retdict = dict()
    retdict['id'] = chord_id
    retdict['url'] = chord_url
    return jsonify(retdict)

@app.route('/getSuccessor')
def getSuccessor():
    global leader_id
    global chord_id
    global chord_url
    global successorId
    global ftable
    idGotten = int(request.args.get('id'))
    print("getting successor for "+str(idGotten))
    #idGotten = int(request.args.get('id'))
    if idGotten == chord_id:
        retdict = dict()
        retdict['id'] = chord_id
        retdict['url'] = chord_url
        return jsonify(retdict)
    if (idGotten > chord_id and idGotten<successorId or idGotten<chord_id and successorId<chord_id and successorId> idGotten ) and successorId in ftable:# if there is a bug commpent this ot
        retdict = dict()
        retdict['id'] =  successorId
        retdict['url'] = ftable[successorId]
        return jsonify(retdict)
    
    usingEl=-1
    for el in sorted(ftable.keys()):
        print("looking ")
        print(el)
        if el>=usingEl and el<=idGotten:
            print("chosee ")
            print(el)
            usingEl=el
    #if usingEl != -1 and usingEl != chord_id:        
    if usingEl != -1:        
        try:
            print("thefatcow")
            #print(url)
            print(usingEl)
            url = 'http://' + ftable[usingEl] + '/getSuccessor?id=' + str(int(idGotten))
            if max(ftable.keys())>idGotten or usingEl>=max(ftable.keys()) or usingEl>=leader_id:
                url = 'http://' + ftable[usingEl] + '/getNext'
            print(url)

            resp = requests.get(url=url)
            print(resp.content)
            print(resp.headers)
            #return str(resp.text)
            return (resp.content,resp.status_code,resp.headers.items())
        except:   
            print("error retrieving file")
    #if usingEl == -1 and idGotten>chord_id:        
    if usingEl == -1 and idGotten>chord_id:        
        try:
            print("thefatpool")
            #print(url)
            url = 'http://' + ftable[successorId] + '/getNext'
            print(url)

            resp = requests.get(url=url)
            print(resp.headers)
            #return str(resp.text)
            return (resp.content,resp.status_code,resp.headers.items())
        except:   
            print("error retrieving file")
            

    if usingEl == -1 and idGotten<min(ftable.keys()):        
        try:
            print("thefatpool")
            #print(url)
            url = 'http://' + ftable[max(ftable.keys())] + '/getNext'
            print(url)

            resp = requests.get(url=url)
            print(resp.headers)
            #return str(resp.text)
            return (resp.content,resp.status_code,resp.headers.items())
        except:   
            print("error retrieving file")
        


    retdict = dict()
    retdict['id'] = chord_id
    retdict['url'] = chord_url
    return jsonify(retdict)

@app.route('/joinNetwork')
def joinNetwork():
    print("in get successor")
    idGotten = int(request.args.get('id'))
    urlGotten = request.args.get('url')
    print(idGotten)
    print(ftable)
    if idGotten > int(pow(2, 8)):
        idGotten = idGotten % int(pow(2, 8))
    print(idGotten)
    if len(ftable.keys()) == 0:
        print("error")
        return '<p>error</p>'

    if idGotten > max(ftable.keys()):
        retdict = dict()
        retdict['id'] = min(ftable.keys())
        retdict['url'] = ftable[min(ftable.keys())]
        return jsonify(retdict)
    # if idGotten > max(ftable.keys()):  TODO CHECK
    # idGotten=idGotten%max(fTable.keys())
    # retdict=dict()
    # retdict['id']= min(fTable.keys())
    # retdict['url']= fTable[min(fTable.keys())]
    # return jsonify(retdict)

    prevEl= max(ftable.keys())

    for el in sorted(ftable.keys()):
        print(el)
        if el >= idGotten:
            retdict = dict()
            retdict['id'] = el
            retdict['url'] = ftable[el]

            payload=dict()
            payload['id'] = idGotten
            payload['url'] = urlGotten
            url = 'http://' + ftable[prevEl] + '/addEntry'
            try:
                resp = requests.post(url=url, json=payload)
            except:
                print("connection error with joining node")



            return jsonify(retdict)
        prevEl=el

    retdict = dict()
    global chord_id
    global chord_url
    retdict['id'] = chord_id
    retdict['url'] = chord_url
    return jsonify(retdict)

@app.route('/getFile')
def root():
    global r
    global rtable
    global hashTable
    global successorId
    global leader_id
    print("in get file")
    idGotten = int(request.args.get('id'))
    print(f'\n\n\n\n Requested file id : {idGotten}\n\n\n\nHashtable:\n{hashTable}\n\n\n\n')
    if int(idGotten)  in hashTable and (idGotten>leader_id or idGotten<chord_id):
        #return '<p>not found</p>'
        print("self seving")

        fname = hashTable[int(idGotten)]
        fname = fname[fname.find("static/") + 7:]
        return app.send_static_file(fname)

    usingEl=-1
    for el in sorted(ftable.keys()):
        if (idGotten <= el and chord_id>idGotten and chord_id>el) or (idGotten<=el and chord_id<idGotten and chord_id<el):
            usingEl=el
    if usingEl == -1 and idGotten>chord_id:
        usingEl=successorId
    #else:   
    if usingEl != -1:
        try:
            print("thefatcat")
            #print(url)
            print(usingEl)
            url = 'http://' + ftable[usingEl] + '/getFile?id=' + str(int(idGotten))
            print(url)

            resp = requests.get(url=url)
            print(resp.headers)
            #return str(resp.text)
            return (resp.content,resp.status_code,resp.headers.items())
        except:   
            print("error retrieving file")
        #for el in sorted(ftable.keys()):
            #if el > chord_id and idGotten < el and idGotten > chord_id:
                #try:
                    #print("making important request")
                    #url = 'http://' + ftable[el] + '/getFile?id=' + str(int(idGotten))

                    #resp = requests.get(url=url)
                    #print(resp.headers)
                    #return str(resp.text)
                    #return (resp.content,resp.status_code,resp.headers.items())
                #except:   
                    #print("error retrieving file")
                #print(el)
                #retdict = dict()
                #retdict['id'] = el
                #retdict['url'] = ftable[el]
                #return jsonify(retdict)
    if int(idGotten) not in hashTable:
        return '<p>not found</p>'

    fname = hashTable[int(idGotten)]
    fname = fname[fname.find("static/") + 7:]

    if (not path.exists("static/"+fname)):
        for successor in rlist:

            print(f'File not found  {fname}     Getting replica from {successor}')
            payload = dict()
            payload['file_id'] = idGotten
            payload['chord_id'] = chord_id
            payload['url'] = chord_url
            url = "http://" + successor['url'] + "/getReplica"

            try:
                found = requests.get(url=url, params=payload)
            except Exception as e:
                print("Error connecting to successor")
                print(str(e))

            if (int(found.text)):
                print(f'File found  {fname}     in {successor}')
                if(path.exists("static/"+fname)):
                    break
                else:
                    print("Error receiving bkp file from successor")
    print(fname)
    return app.send_static_file(fname)


@app.route('/getReplica', methods=['GET'])
def getReplica():
    global replicaTable
    global rtable
    idGotten = int(request.args.get('file_id'))
    pred_id = int(request.args.get('chord_id'))
    url = request.args.get('url')
    print(replicaTable)
    print(f'replica table of {pred_id} :\n {replicaTable[pred_id]}\nFinding {idGotten} file in the above table....')
    for fileid in sorted(replicaTable[pred_id].keys()):
        if idGotten == fileid:
            fpath = replicaTable[pred_id][fileid]
            print(f'Sending     {fpath}     file to     {pred_id}')
            url = 'http://' + url + '/putFile'
            files = {'file': open(fpath, 'rb')}
            payload = dict()
            payload['noSend'] = 1
            resp = requests.post(url=url, files=files, params=payload)
            return '1'
    print('File not found')
    responsedict = dict()
    responsedict['found'] = 0
    return '0'


# @app.route('/putFile', methods=['POST'])
# def upload_file():
#    print("in uppload file")
#    print(request)
#    print(request.files)
#    print(request.files.keys())
#    print(request.files['filedata'])
#    print(request.files['name'])
#    uploaded_file = request.files['file']
#    print(uploaded_file.filename)
#    print("pew loop")

#    if uploaded_file.filename != '':
#        print("in if")
#        print(uploaded_file.filename)
#        uploaded_file.save(uploaded_file.filename)
#    return 'uploaded file'

@app.route("/upload", methods=['GET'])
def uploadPage():
    res='''
    <html>
   <body>
      <form action = "/putFile" method = "POST"
         enctype = "multipart/form-data">
         <input type = "file" name = "file" />
         <input type = "submit"/>
      </form>
   </body>
    </html>
        '''
    return res    
        
@app.route("/putFile", methods=['POST', 'PUT'])
def print_filename():
    global chord_id
    global chord_url
    global hashTable
    fileN = request.files['file']
    # filename=secure_filename(fileN.filename)
    filename = fileN.filename

    noS=request.args.get("noSend")
    if noS is None:
        noS=0
    print(filename)
    if filename != '':
        print("in if")
        print(filename)
        fileN.save('static/' + filename)
    for el in glob.glob("static/*.*"):
        hash_object = hashlib.sha1(bytes(el, 'utf-8'))
        pbHash = hash_object.hexdigest()
        file_id = int(pbHash, 16) & 0xFF
        hashTable[file_id] = el
        print("backuking up file")
        print(file_id)
        for el2 in sorted(ftable.keys()):
            if el2>file_id and noS==0:
                print("backing up to")
                print(el2)
                if el2==chord_id:
                    continue
                if el2 != successorId:
                    url = 'http://' + ftable[el2] + '/getSuccessor?id=' + str(int(file_id))
                    print(url)
                    data = ''
                    try:
                        resp = requests.get(url=url)
                    except:
                        deletionList.append(successorId)
                        removeRemoveElement(successorId)
                        print("error connecting")
                        continue

                    data = resp.json()
                    newul = data['url']
                    url = 'http://' + newul + '/putFile'
                    print(url)
                    print(el)
                    files = {'file': open(el, 'rb')}
                    values = {}
                    payload=dict()
                    payload['noSend'] = 1
                    try:
                        resp = requests.post(url, files=files, data=values,params=payload)
                    except:
                        deletionList.append(int(data['id']))
                        removeRemoveElement(int(data['id']))
                        print("error connecting")
                    break

                else:
                    url = 'http://' + ftable[el2] + '/putFile'
                    print(url)
                    print(el)
                    files = {'file': open(el, 'rb')}
                    values = {}
                    payload=dict()
                    payload['noSend'] = 1
                    try:
                        resp = requests.post(url, files=files, data=values,params=payload)
                    except:
                        deletionList.append(int(data['id']))
                        removeRemoveElement(int(data['id']))
                        print("error connecting")
                    break
        '''        
        if file_id > chord_id and successorId>chord_id and noS==0:

            url = 'http://' + ftable[successorId] + '/getSuccessor?id=' + str(int(file_id))
            print(url)
            data = ''
            try:
                resp = requests.get(url=url)
            except:
                deletionList.append(successorId)
                removeRemoveElement(successorId)
                print("error connecting")
                continue

            data = resp.json()
            newul = data['url']
            url = 'http://' + newul + '/putFile'
            print(url)
            print(el)
            files = {'file': open(el, 'rb')}
            values = {}
            payload=dict()
            payload['noSend'] = 1
            try:
                resp = requests.post(url, files=files, data=values,params=payload)
            except:
                deletionList.append(int(data['id']))
                removeRemoveElement(int(data['id']))
                print("error connecting")

            print(data)
            '''
    print(hashTable)
    return filename

@app.route("/putReplica", methods=['POST', 'PUT'])
def putReplica():
    global chord_id
    global chord_url
    global replicaTable
    fileN = request.files['file']

    # filename=secure_filename(fileN.filename)
    filename = fileN.filename

    pred_id = int(request.args.get("r_value"))
    file_id = int(request.args.get('id'))
    print(filename)

    rpath = 'static/bkp/' + str(pred_id)

    if not path.exists(rpath):
        os.makedirs(rpath)

    if filename != '':
        print("in if")
        print(filename)
        fileN.save(rpath + '/' + filename)
    #temp = dict()

    #for el in glob.glob(rpath + '/*'):
    #    original_path = re.sub(r"\/bkp\/[0-9]+", "", el)
    #    print(f'\n\n\n\n\n original file path : {original_path}\n\n\n\n\n')
    #    hash_object = hashlib.sha1(bytes(el, 'utf-8'))
    #    pbHash = hash_object.hexdigest()
    #    file_id = int(pbHash, 16) & 0xFF
    #    temp[file_id] = el
    try:
        replicaTable[pred_id][file_id] = rpath + '/' + filename
    except Exception as e:
        print(f'Key not found: {e}')
        try:
            replicaTable[pred_id][file_id] = rpath + '/' + filename
        except Exception as e:
            print(e)
            replicaTable[pred_id] = {file_id: rpath + '/' + filename}

    #replicaTable[pred_id] = temp
    print(f'Replicating {pred_id} predecessor files...\n{replicaTable[pred_id]}')
    return filename


if __name__ == '__main__':
    for i in range(r):
        rlist.append({'id': -1, 'url': ''})
    z = threading.Thread(target=buildFTable, args=())
    electionThread = threading.Thread(target=election_thread)
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 80))
    print(s.getsockname()[0])
    ip = s.getsockname()[0]
    hash_object = hashlib.sha1(bytes(ip + sys.argv[1], 'utf-8'))
    pbHash = hash_object.hexdigest()
    chord_id = int(pbHash, 16) & 0xFF
    # db['id'] = chord_id
    # db['port'] = sys.argv[1]
    # db['ip'] = ip
    fTable[chord_id] = ip + ':' + sys.argv[1]
    ftable[chord_id] = ip + ':' + sys.argv[1]
    chord_url=ip + ':' + sys.argv[1]
    if len(sys.argv) == 2:
        leader_id = chord_id
        leaderURL = chord_url
    print(fTable)
    print(f'leader is {leader_id}')
    print(f'id is {chord_id}')
    # ftable[int(data['id'])]=data['url']

    for el in glob.glob("static/*.*"):
        hash_object = hashlib.sha1(bytes(el, 'utf-8'))
        pbHash = hash_object.hexdigest()
        file_id = int(pbHash, 16) & 0xFF
        hashTable[file_id] = el
    print(hashTable)

    # digest = sha1.hexdigest()
    # digest_int = int(digest,16)

    logging.info("Main    : before running thread")

    # hash_object = hashlib.sha1(bytes(sys.argv[2]+sys.argv[3], 'utf-8'))
    # pbHash = hash_object.hexdigest()
    # searchId = int(pbHash,16)&0xFF
    if len(sys.argv) > 2:
        hash_object = hashlib.sha1(bytes(sys.argv[2] + sys.argv[3], 'utf-8'))
        pbHash = hash_object.hexdigest()
        searchId = int(pbHash, 16) & 0xFF
        # url = 'http://' + sys.argv[2] + ':' + sys.argv[3] + '/getLeader'
        url = 'http://' + sys.argv[2] + ':' + sys.argv[3] + '/leader'
        print('search url is' + url)
        resp = requests.get(url=url)
        print(resp)
        data = resp.json()
        print(data)
        # print(data['id'])
        # fTable[data['id']]=data['ip']+':'+data['port']
        open("db.json", 'wb').write(resp.content)
        print('new finger table is')
        # print(fTable)
        # fTable[int(data['id'])] = data['url']
        # ftable[int(data['id'])] = data['url']
        leader_id = data['id']
        leaderURL = data['url']
        print(fTable)
        if leader_id != chord_id:
            # url = 'http://'+sys.argv[2]+':'+sys.argv[3]+'/getDb?id='+str(leader)
            # url = 'http://'+sys.argv[2]+':'+sys.argv[3]+'/getLeader'
            #url = 'http://' + fTable[leader] + '/getSuccessor?id=' + str(chord_id)
            #print('search url is' + url)
            #resp = requests.get(url=url)
            payload=dict()
            payload['id'] = chord_id
            payload['url'] = chord_url
            print("payload is")
            print(payload)
            url = 'http://' + leaderURL + '/joinNetwork'
            resp = requests.get(url=url, params=payload)
            print(resp)
            data = resp.json()
            print("response data is")
            print(data)
            print(data['id'])
            print(data['url'])
            successorId = data['id']
            successorURl = data['url']
            # print(data['id'])
            # fTable[data['id']]=data['ip']+':'+data['port']
            open("db.json", 'wb').write(resp.content)
            print('new finger table is')
            print(fTable)
            fTable[int(data['id'])] = data['url']
            ftable[int(data['id'])] = data['url']
            print(fTable)
            payload = dict()
            payload['id'] = chord_id
            payload['url'] = chord_url
            print("payload is")
            print(payload)
            # if leader != chord_id:
            #url = 'http://' + fTable[leader] + '/addEntry'
            url = 'http://' + leaderURL + '/addEntry'
            resp = requests.post(url=url, json=payload)

        payload = dict()
        payload['id'] = chord_id
        payload['url'] = chord_url
        print("payload is")
        print(payload)
        url = 'http://' + sys.argv[2] + ':' + sys.argv[3] + '/addEntry'

        print('search url is' + url)
        resp = requests.post(url=url, json=payload)
    print(successorId)
    if successorId != -1 and successorId != chord_id:
        payload = dict()
        payload['id'] = chord_id
        payload['url'] = chord_url
        print("payload is")
        print(payload)
        url = 'http://' + successorURl + '/addEntry'

        print('search url is' + url)
        resp = requests.post(url=url, json=payload)
    z.start()
    electionThread.start()
    app.run(host='0.0.0.0', port=sys.argv[1])
