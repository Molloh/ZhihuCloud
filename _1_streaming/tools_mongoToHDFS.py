import hdfs
import pymongo
import json
import os
import time

client = hdfs.Client('http://172.19.240.199:50070', root='/')

myClient = pymongo.MongoClient(host='172.19.240.199', port=20000)
mydb = myClient['CloudComputing']
mycol = mydb['UserInfo']

Mongo_json_OK = []
with open('Mongo_json_OK.txt', 'r', encoding='utf-8') as f:
    mongoId = f.readline().strip()
    while mongoId:
        Mongo_json_OK.append(id)
        mongoId = f.readline().strip()

count = len(Mongo_json_OK)
for item in mycol.find():
    item['_id'] = str(item['_id'])
    if item['_id'] not in Mongo_json_OK:
        filePath = './json/'+item['_id']+'.json'
        with open(filePath, 'w', encoding='utf-8') as f:
            json.dump(item, f, ensure_ascii=False)
        client.upload('/streaminput/', filePath, overwrite=True)
        os.remove(filePath)

        Mongo_json_OK.append(item['_id'])
        with open('Mongo_json_OK.txt', 'a', encoding='utf-8') as f:
            f.write(item['_id']+'\n')

        count += 1
        print('%d : %s' % (count,  item['_id']))
        time.sleep(1)