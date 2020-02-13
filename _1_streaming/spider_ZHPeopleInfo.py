import scrapy
import json
import random
import re
import time
import os
import pymongo
from urllib import parse

AGENTS = [
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.75 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0.2 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.120 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.102 Safari/537.36 Edge/18.18362'
]

HEADERS = {
    'method': 'GET',
    'scheme': 'https',
    'Referer': 'https://www.zhihu.com/',
    'Origin': 'https://www.zhihu.com/',
    'cache-control': 'no-cache',
    'Connection': 'close',
}


class ZHPeopleInfoSpider(scrapy.Spider):
    name = 'zhPI'
    custom_settings = {
        "DOWNLOAD_DELAY": 5,
    }

    def start_requests(self):
        userNO = self.findUserFollowNotSpider()
        HEADERS['User-Agent'] = random.choice(AGENTS)
        if len(userNO) > 0:
            print('\n爬取用户 （%s） 信息\n' % userNO)
            url = 'https://www.zhihu.com/people/' + userNO + '/following'
            yield scrapy.Request(url=url, headers=HEADERS, callback=self.parse)
        else:
            print('所有用户都已经爬取完毕')
            return None

    def parse(self, response):
        regex_1 = r'\/people\/.*\/following$'
        r_htmlEle = r'(<[\d\w\s\;\:\'\"\,\.\/\?\!\@\#\$\%\^\&\*\(\)\-\_\=\+]+\/*>)'
        print('')
        if re.search(regex_1, response.url) is not None:
            print('正在分析 %s ...' % response.url)
            print('')
            UId = response.url.split('www.zhihu.com/people/')[-1].split('/following')[0]
            userJSON = json.loads(response.css('script#js-initialData::text').get())
            userJSON = userJSON['initialState']['entities']['users'][UId]
            try:
                userInfo = {
                    'id': userJSON['id'],
                    'urlToken': userJSON['urlToken'],
                    'name': userJSON['name'],
                    'isOrg': userJSON['isOrg'],
                    'type': userJSON['type'],
                    'url': userJSON['url'],
                    'headline': str(userJSON['headline']).replace('\n', ''),
                    'isActive': userJSON['isActive'],
                    'description': re.sub(r_htmlEle, '', userJSON['description']),
                    'gender': userJSON['gender'],
                    'badge': userJSON['badge'],
                    'followerCount': userJSON['followerCount'],
                    'followingCount': userJSON['followingCount'],
                    'answerCount': userJSON['answerCount'],
                    'questionCount': userJSON['questionCount'],
                    'commercialQuestionCount': userJSON['commercialQuestionCount'],
                    'articlesCount': userJSON['articlesCount'],
                    'favoritedCount': userJSON['favoritedCount'],
                    'pinsCount': userJSON['pinsCount'],
                    'logsCount': userJSON['logsCount'],
                    'voteupCount': userJSON['voteupCount'],
                    'thankedCount': userJSON['thankedCount'],
                    'hostedLiveCount': userJSON['hostedLiveCount'],
                    'followingColumnsCount': userJSON['followingColumnsCount'],
                    'followingTopicCount': userJSON['followingTopicCount'],
                    'followingQuestionCount': userJSON['followingQuestionCount'],
                    'followingFavlistsCount': userJSON['followingFavlistsCount'],
                    'voteToCount': userJSON['voteToCount'],
                    'voteFromCount': userJSON['voteFromCount'],
                    'thankToCount': userJSON['thankToCount'],
                    'thankFromCount': userJSON['thankFromCount'],
                    'business': userJSON['business']['name'],
                    'locations': [item['name'] for item in userJSON['locations'] if 'name' in item],
                    'educations': [{
                        'school': item['school']['name'],
                        'major': item['major']['name']
                    } for item in userJSON['educations'] if 'school' in item and 'major' in item],
                }
                # self.writeInfo(UId, userInfo)
                self.writeInfoToMongo(UId, userInfo)
            except Exception as e:
                print('KeyError : ', end=' ')
                print(e)
                with open('./User_Info_OK.txt', 'a', encoding='utf-8') as f:
                    f.write(UId+':::ERROR'+'\n')
                
            userNO = self.findUserFollowNotSpider()
            if len(userNO) > 0:
                print('\n爬取用户 （%s） 信息\n' % userNO)
                url = 'https://www.zhihu.com/people/' + userNO + '/following'
                yield scrapy.Request(url=url, headers=HEADERS, callback=self.parse)
            else:
                print('所有用户都已经爬取完毕')
                return None

    @staticmethod
    def writeInfo(UId, userInfo):
        print('info ======')
        print(str(userInfo))
        with open('./User_Infos_2.txt', 'a', encoding='utf-8') as f:
            str_w = ''
            for key in userInfo:
                str_w += str(key)+':::'+str(userInfo[key])+'---'
            f.write(str_w[:-3]+'\n')
        with open('./User_Info_OK.txt', 'a', encoding='utf-8') as f:
            f.write(UId+'\n')

    @staticmethod
    def writeInfoToMongo(UId, userInfo):
        print('info ======')
        myClient1 = pymongo.MongoClient(host='127.0.0.1', port=27017)
        mydb1 = myClient1['CloudComputing']
        mycol1 = mydb1['UserInfo']
        res = mycol1.insert_one(userInfo)

        myClient2 = pymongo.MongoClient(host='172.19.240.199', port=20000)
        mydb2 = myClient2['CloudComputing']
        mycol2 = mydb2['UserInfo']
        db_admin = myClient2['admin']
        mycol2.insert_one(userInfo)
        # CloudComputing.UserInfo 启动分片
        # db_admin.command('enablesharding', 'CloudComputing')
        # db_admin.command('shardcollection', 'CloudComputing.UserInfo', key={'_id': 1})
        myClient1.close()
        myClient2.close()
        print(res)
        with open('./User_Info_OK.txt', 'a', encoding='utf-8') as f:
            f.write(UId + '\n')

    @staticmethod
    def findUserFollowNotSpider():
        filePath_UNO = './Users.txt'
        filePath_UOK = './User_Info_OK.txt'
        userOKs = []
        if os.path.isfile(filePath_UOK):
            with open(filePath_UOK, 'r', encoding='utf-8') as f:
                userOK = f.readline().strip()
                while userOK:
                    userOKs.append(userOK.split(':::')[0])
                    userOK = f.readline().strip()
        if os.path.isfile(filePath_UNO):
            with open(filePath_UNO, 'r', encoding='utf-8') as f:
                user = f.readline().strip()
                while user:
                    if user not in userOKs:
                        return user
                        # print(user)
                        # user = f.readline().strip()
                    else:
                        user = f.readline().strip()
        return ''
