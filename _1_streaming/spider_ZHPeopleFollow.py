import scrapy
import json
import random
import re
import time
import os
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


class ZHPeopleFollowSpider(scrapy.Spider):
    name = 'zhPF'
    custom_settings = {
        "DOWNLOAD_DELAY": 5,
    }

    def start_requests(self):
        userNO = self.findUserFollowNotSpider()
        HEADERS['User-Agent'] = random.choice(AGENTS)
        if len(userNO) > 0:
            print('\n爬取用户 （%s） 关注\n' % userNO)
            url = 'https://www.zhihu.com/people/' + userNO + '/following'
            yield scrapy.Request(url=url, headers=HEADERS, callback=self.parse)
        else:
            print('所有用户都已经爬取完毕')
            return None

    def parse(self, response):
        regex_1 = r'\/people\/.*\/following$'
        regex_2 = r'\/members\/.*\/followees?'
        r_htmlEle = r'(<[\d\w\s\;\:\'\"\,\.\/\?\!\@\#\$\%\^\&\*\(\)\-\_\=\+]+\/*>)'
        r_onlyNum = r'[^\d]'
        followLimit = 100
        userLimit = 25000
        UOKs = []
        UserCount = self.getUserCount()
        if not UserCount or UserCount == 0:
            UserCount = 0
            with open('./Users.txt', 'r', encoding='utf-8') as f:
                UId = f.readline().strip()
                while UId:
                    if len(UId) > 0:
                        UserCount += 1
                    UId = f.readline().strip()
            with open('./User_Count.txt', 'w', encoding='utf-8') as f:
                f.write(str(UserCount)+'\n')
        if UserCount and UserCount >= userLimit:
            print('已经爬取 %d 个用户 ID' % userLimit)
            return None
        if os.path.isfile('./User_Follow_OK.txt'):
            with open('./User_Follow_OK.txt', 'r', encoding='utf-8') as f:
                UId = f.readline().strip().split(':::')[0]
                while UId:
                    UOKs.append(UId)
                    UId = f.readline().strip()
        
        follow = []

        print('')
        if re.search(regex_1, response.url) is not None:
            print('正在分析 %s ...' % response.url)
            print('')
            UId = response.url.split('www.zhihu.com/people/')[-1].split('/following')[0]
            if UId in UOKs:
                print('已经爬取过用户 %s' % UId)
                return None
            for item in response.css('div.List-item'):
                try:
                    divT = item.css('div.ContentItem-head h2.ContentItem-title div.Popover div')
                    user = {
                        'id': str(divT.css('a::attr(href)').get()).split('/people/')[-1],
                        'url': response.urljoin(str(divT.css('a::attr(href)').get())),
                        'name': str(divT.css('a::text').get()),
                        'gender': -1
                    }
                    follow.append(user)
                except Exception as e:
                    print(e)
            print('分析完毕，开始写入文件')
            count = self.getUserFollowCount(UId)
            userCount = self.getUserCount()
            count = self.writeFollow(UId, follow, count, userCount)
            print('文件写入完毕，用户 %s 已爬取 %d 个关注' % (UId, count))
            # time.sleep(3)
            if count < followLimit:
                nextUrl = 'https://www.zhihu.com/api/v4/members/' + UId + '/followees?' \
                          + 'include=data[*].answer_count,articles_count,gender,follower_count,is_followed,is_following,badge[?(type=best_answerer)].topics' \
                          + '&offset=' + str(count) + '&limit=20'
                yield scrapy.Request(nextUrl, headers=HEADERS, callback=self.parse)
        elif re.search(regex_2, response.url) is not None:
            print('正在分析 %s ...' % response.url)
            print('')
            res = json.loads(response.body_as_unicode())
            UId = response.url.split('/members/')[-1].split('/followees?')[0]
            offset = response.url.split('&offset=')[-1].split('&')[0]
            limit = response.url.split('&limit=')[-1].split('&')[0]
            if UId in UOKs:
                print('已经爬取过用户 %s' % UId)
                return None
            is_end = True
            try:
                is_end = res['paging']['is_end']
            except Exception as e:
                print('KeyError : ', end=' ')
                print(e)
                print('用户 %s 爬取结束' % UId)
                count = self.getUserFollowCount(UId)
                with open('./User_Follow_OK.txt', 'a', encoding='utf-8') as f:
                    f.write(UId + ':::' + str(count) + '\n')
                # time.sleep(5)
                userNO = self.findUserFollowNotSpider()
                if len(userNO) > 0:
                    print('\n爬取用户 （%s） 关注\n' % userNO)
                    url = 'https://www.zhihu.com/people/' + userNO + '/following'
                    yield scrapy.Request(url=url, headers=HEADERS, callback=self.parse)
                return None
            for data in res['data']:
                try:
                    user = {
                        'id': data['url_token'],
                        'url': data['url'],
                        'name': data['name'],
                        'gender': data['gender']
                    }
                    follow.append(user)
                except Exception as e:
                    print(str(data))
                    print(e)
            print('分析完毕，开始写入文件')
            count = self.getUserFollowCount(UId)
            userCount = self.getUserCount()
            count = self.writeFollow(UId, follow, count, userCount)
            print('文件写入完毕，用户 %s 已爬取 %d 个关注' % (UId, count))
            # time.sleep(3)
            if not is_end and count < followLimit:
                offset += limit
                nextUrl = 'https://www.zhihu.com/api/v4/members/' + UId + '/followees?' \
                          + 'include=data[*].answer_count,articles_count,gender,follower_count,is_followed,is_following,badge[?(type=best_answerer)].topics' \
                          + '&offset=' + str(offset) + '&limit=' + str(limit)
                yield scrapy.Request(nextUrl, headers=HEADERS, callback=self.parse)
            else:
                with open('./User_Follow_OK.txt', 'a', encoding='utf-8') as f:
                    f.write(UId + ':::' + str(count) + '\n')
                # time.sleep(5)
                userNO = self.findUserFollowNotSpider()
                if len(userNO) > 0:
                    print('\n爬取用户 （%s） 关注\n' % userNO)
                    url = 'https://www.zhihu.com/people/' + userNO + '/following'
                    yield scrapy.Request(url=url, headers=HEADERS, callback=self.parse)
                else:
                    print('所有用户都已经爬取完毕')
                    return None

    @staticmethod
    def writeFollow(UId, follow, count, userCount):
        filePathPre = './UserFollow/' + UId + '/' + UId
        filePath = './UserFollow/' + UId + '/'

        if not os.path.exists(filePath):
            os.makedirs(filePath)
        
        if len(follow) > 0:
            print('follow ======')
            filePath = filePathPre + '_follow.txt'
            with open(filePath, 'a', encoding='utf-8') as f:
                actCount = 0
                for user in follow:
                    if user['id'].find('www.zhihu.com/org/') > -1:
                        user['id'] = user['id'].split('www.zhihu.com/org/')[-1]
                    str_w = ''
                    for key in user:
                        str_w += str(key) + ':::' + str(user[key]) + '---'
                    f.write(str_w[:-3] + '\n')
                    with open('./Users.txt', 'a', encoding='utf-8') as f2:
                        f2.write(user['id']+'\n')
                    print(str(actCount) + ' : ' + str_w[:-3])
                    actCount += 1
                    count += 1

            filePath = filePathPre + '_follow_info.txt'
            with open(filePath, 'w', encoding='utf-8') as f:
                f.write('follow_total_count:::%d' % count)
            userCount += actCount
            with open('./User_Count.txt', 'w', encoding='utf-8') as f:
                f.write(str(userCount)+"\n")
        return count

    @staticmethod
    def getUserCount():
        count = 0
        filePath = './User_Count.txt'
        if os.path.isfile(filePath):
            with open(filePath, 'r', encoding='utf-8') as f:
                try:
                    count = int(f.readline().strip())
                except Exception:
                    count = 0
            if not count:
                count = 0
        return count

    @staticmethod
    def getUserFollowCount(UId):
        count = 0
        filePath = './UserFollow/' + UId + '/' + UId + '_follow_info.txt'
        if os.path.isfile(filePath):
            with open(filePath, 'r', encoding='utf-8') as f:
                line = f.readline().strip()
                if line:
                    try:
                        count = int(str(line).split(':::')[-1])
                    except Exception as e:
                        count = 0
        return count

    @staticmethod
    def findUserFollowNotSpider():
        filePath_UNO = './Users.txt'
        filePath_UOK = './User_Follow_OK.txt'
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
