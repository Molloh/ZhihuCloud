import pymongo
import re
import time
import os

myClient = pymongo.MongoClient(host='127.0.0.1', port=27017)
mydb = myClient['CloudComputing']

mycol1 = mydb['UserFollow']
mycol2 = mydb['UserAct_Answer']
mycol3 = mydb['UserQuestionRank']
# mycol4 = mydb['UserAct_Info']
mycol5 = mydb['UserId']

rate_follow, rate_approve, rate_collect, rate_publish = 5, 1, 1.5, 2.5

count = 0
userQRate = {}

for line in mycol1.find():
    if 'urlToken' in line and 'question' in line:
        urlToken = line['urlToken']
        QAndRate = {}
        for question in line['question']:
            QId = question['id']
            QAndRate[QId] = [5]
        userQRate[urlToken] = QAndRate

for line in mycol2.find():
    if 'userUrlToken' in line and 'questionId' in line and 'actType' in line:
        urlToken = line['userUrlToken']
        QId = line['questionId']
        rate = 0
        if line['actType'] == 'answer_approve':
            rate = rate_approve
        elif line['actType'] == 'answer_collect':
            rate = rate_collect
        else:
            rate = rate_publish
        
        if urlToken in userQRate:
            if QId in userQRate[urlToken]:
                if rate not in userQRate[urlToken][QId]:
                    userQRate[urlToken][QId].append(rate)
            else:
                userQRate[urlToken][QId] = [rate]
        else:
            userQRate[urlToken] = {QId: [rate]}

for urlToken in userQRate:
    res = mycol5.find_one({'urlToken': urlToken})
    if res and 'UId' in res:
        id = res['UId']
        for QId in userQRate[urlToken]:
            try:
                count += 1
                info = {
                    'UId': int(id),
                    'QId': int(QId),
                }
                rate = sum(userQRate[urlToken][QId])
                res = len(list(mycol3.find(info)))
                if res == 0:
                    info['rate'] = rate
                    mycol3.insert_one(info)
                    print(str(count) + ' : ' + str(info))
                elif res > 0:
                    mycol3.update_one(info, {'$set': {'rate': rate}})
            except Exception as e:
                print(e)

myClient.close()
