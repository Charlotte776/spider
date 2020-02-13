# conpany_name company_code company_http time question answer question_http question_state question_people question_plat

import requests,json,pymysql,time,telnetlib,re

def get_proxy():
    return requests.get('http://tunnel-api.apeyun.com/q?id=2120012700159601253&secret=nQcg4KdZ1clkOaPI&limit=1&format=json&auth_mode=auto').json()
    # return requests.get("http://47.104.228.235:5010/get/").json()

def delete_proxy(proxy):
    requests.get("http://47.104.228.235:5010/delete/?proxy={}".format(proxy))

# your spider code

def proxy():
    # ....

    # 代理服务器
    proxyHost = "forward.apeyun.com"
    proxyPort = "9082"

    # 代理隧道验证信息
    proxyUser = "2120012800166061732"
    proxyPass = "nQcg4KdZ1clkOaPI"

    proxyMeta = "http://%(user)s:%(pass)s@%(host)s:%(port)s" % {
        "host": proxyHost,
        "port": proxyPort,
        "user": proxyUser,
        "pass": proxyPass,
    }

    proxies = {
        "http": proxyMeta,
        "https": proxyMeta,
    }
    return proxies





def insert_data(data_list):
    # 连接database
    conn = pymysql.connect(
        host="127.0.0.1",
        user='root',
        password=password,
        database='auth',
        charset='utf8')

    # 得到一个可以执行SQL语句的光标对象
    cursor = conn.cursor()  # 执行完毕返回的结果集默认以元组显示
    for data in data_list:
        # 定义要执行的SQL语句
        try:
            sql = """
            INSERT INTO `crawl3` (`conpany_name`,`company_code`, `company_http`, `time`, `question`, `answer`, `question_http`,`question_people`,`question_plat`)
             VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')
            """%(data['conpany_name'],data['company_code'],data['company_http'],data['time'],
                data['question'],data['answer'],data['question_http'],data['question_people'],data['question_plat'])

            # 执行SQL语句
            cursor.execute(sql)
        except Exception as e:
            try:
                sql = """
                            INSERT INTO `crawl2` (`conpany_name`,`company_code`, `company_http`, `time`, `question`, `answer`, `question_http`,`question_people`,`question_plat`)
                             VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')
                            """ % (data['conpany_name'], data['company_code'], data['company_http'], data['time'],
                                   pymysql.escape_string(data['question']), pymysql.escape_string(data['answer']), data['question_http'], data['question_people'],
                                   data['question_plat'])

                # 执行SQL语句
                cursor.execute(sql)
            except:
                pass
    # 关闭光标对象
    cursor.close()

    # 关闭数据库连接
    conn.close()

def get_company(stockcode,orgId):
    api = 'http://irm.cninfo.com.cn/ircs/company/company'
    data ={
        'stockcode': stockcode,
        'orgId': orgId
    }
    try:
        r = requests.post(api,data=data,proxies=proxy())
    except Exception as e:
        time.sleep(3)
        r = requests.post(api,data=data)
    try:
        js = json.loads(r.text)
        if js['statusCode'] == 200:
            data = js['data']
            attentionCount = data['attentionCount']
            questionCount = data['questionCount']
            replyCount = data['replyCount']
            return [attentionCount, questionCount, replyCount]

    except Exception as e:
        print(r.text)
    # print(js)

def question(stockcode="002925",orgId="9900033915"):
    api = "http://irm.cninfo.com.cn/ircs/company/question"
    data = {
        "stockcode": stockcode,
        "orgId": orgId,
        "pageSize": "100",
        "pageNum": "1"
    }
    try:
        r = requests.post(api,data=data,proxies=proxy())
    except Exception as e:
        try:
            r = requests.post(api, data=data, proxies=proxy())
        except Exception as e:
            r = requests.post(api,data=data)

    js = json.loads(r.text)
    totalPage = js['totalPage']
    rows = js['rows']
    #
    # print(data)
    # print(totalPage,type(totalPage))
    if totalPage == 1:
        data_list = []
        for row in rows:
            one = {}
            one['conpany_name'] = row['companyShortName']
            one['company_code'] = row['stockCode']
            one['company_http'] = 'http://irm.cninfo.com.cn/ircs/company/companyDetail?stockcode={}&orgId={}'.format(
                stockcode, orgId)
            one['pubDate'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(row['pubDate'] / 1000))
            one['time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(row['updateDate'] / 1000))
            one['question'] = row['mainContent']
            try:
                one['answer'] = row['attachedContent']
            except Exception as e:
                one['answer'] = '无答案'

            one['question_http'] = 'http://irm.cninfo.com.cn/ircs/question/questionDetail?questionId={}'.format(
                row['indexId'])

            one['question_people'] = row['authorName']
            one['question_plat'] = '来源 website'
            data_list.append(one)
        insert_data(data_list)
    else:
        num = 1
        while num < totalPage:
            data = {
                "stockcode": stockcode,
                "orgId": orgId,
                "pageSize": "100",
                "pageNum": str(num)
            }
            print(data)
            try:
                r = requests.post(api, data=data, proxies=proxy())
            except Exception as e:
                try:
                    r = requests.post(api, data=data, proxies=proxy())
                except Exception as e:
                    try:
                        r = requests.post(api, data=data, proxies=proxy())
                    except:
                        r = requests.post(api, data=data)
            js = json.loads(r.text)
            totalPage = js['totalPage']
            rows = js['rows']
            data_list = []
            for row in rows:
                one = {}
                one['conpany_name'] = row['companyShortName']
                one['company_code'] = row['stockCode']
                one['company_http'] = 'http://irm.cninfo.com.cn/ircs/company/companyDetail?stockcode={}&orgId={}'.format(
                    stockcode, orgId)
                one['pubDate'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(row['pubDate'] / 1000))
                one['time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(row['updateDate'] / 1000))
                one['question'] = row['mainContent']
                try:
                    one['answer'] = row['attachedContent']
                except Exception as e:
                    one['answer'] = ''

                one['question_http'] = 'http://irm.cninfo.com.cn/ircs/question/questionDetail?questionId={}'.format(
                    row['indexId'])

                one['question_people'] = row['authorName']
                one['question_plat'] = '来源 website'
                data_list.append(one)

            num += 1

            insert_data(data_list)




def get_info(stockcode,orgId):
    conn = pymysql.connect(
        host="127.0.0.1",
        user='root',
        password=password,
        database='auth',
        charset='utf8')

    # 得到一个可以执行SQL语句的光标对象
    cursor = conn.cursor()  # 执行完毕返回的结果集默认以元组显示
    api = "http://irm.cninfo.com.cn/ircs/company/executiveInfo"
    data ={
        'stockcode': stockcode,
        'orgId': orgId
    }
    try:
        r = requests.post(api,data=data,proxies=proxy(),timeout=2)
    except Exception as e:
        try:
            r = requests.post(api, data=data, proxies=proxy(),timeout=2)
        except Exception as e:
                r = requests.post(api,data=data)


    js = json.loads(r.text)

    if js['statusCode'] ==200:
        data = js['data']
        insert_data = {}
        insert_data['company_code'] = stockcode
        insert_data['name'] = ''
        insert_data['record'] = ''
        insert_data['CEO'] = ''
        insert_data['dongshizhang'] = ''
        insert_data['caiwu'] = ''
        insert_data['else'] =''
        for each in data:
            if'董秘' in each['position']:
                insert_data['name'] = each['name']
                insert_data['record'] = each['record']
            elif '总经理' in each['position']:
                insert_data['CEO'] = each['name']

            elif '财务总监' in each['position']:
                insert_data['caiwu'] = each['name']

            elif '董事长' in each['position']:
                insert_data['dongshizhang'] = each['name']
            else:
                insert_data['else']+= each['name'] + '  ' + each['position']+ '  '

        attentionCount,questionCount,replyCount = get_company(stockcode,orgId)
        insert_data['attentionCount'] = attentionCount
        insert_data['questionCount'] = questionCount
        insert_data['replyCount'] = replyCount

        sql = """
        INSERT INTO `detail` (`company_code`, `attentionCount`, `questionCount`, `replyCount`, `name`, `record`,`dongshizhang`,`caiwu`,`CEO`, `else`)
         VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s','%s', '%s', '%s')
        """%(insert_data['company_code'],insert_data['attentionCount'],insert_data['questionCount'],insert_data['replyCount'],insert_data['name'],insert_data['record'],
             insert_data['dongshizhang'],insert_data['caiwu'],insert_data['CEO'],insert_data['else'])

        # 执行SQL语句
        cursor.execute(sql)



def main():
    company_set = set()
    n = 0
    while n<=100:

        # url = 'http://irm.cninfo.com.cn/'
        api = 'http://irm.cninfo.com.cn/ircs/index/search'
        data ={
        'pageNo': str(n),
        'pageSize': '100',
        'searchTypes': '1,11,',
        'keyWord': '',
        'market': '',
        'industry': '',
        'stockCode': '',
        }
        print('页数：{}'.format(n))
        try:
            r = requests.post(api,data=data,proxies=proxy())
        except Exception as e:
            try:
                r = requests.post(api, data=data, proxies=proxy())
            except :
                r = requests.post(api, data=data )
        js = json.loads(r.text)
        results = js['results']
        data_list = []
        for each in results:
            try:
                if each['stockCode'] not in company_set:
                    one ={}
                    one['conpany_name'] = each['companyShortName']
                    one['company_code'] = each['stockCode']
                    one['company_http'] = 'http://irm.cninfo.com.cn/ircs/company/companyDetail?stockcode={}&orgId={}'.format(each['stockCode'],each['secid'])
                    one['time'] = each['packageDate']
                    one['question'] = each['mainContent']
                    try:
                        one['answer'] = each['attachedContent']
                    except Exception as e:
                        one['answer'] = ''

                    one['question_http'] = 'http://irm.cninfo.com.cn/ircs/question/questionDetail?questionId={}'.format(each['indexId'])

                    one['question_people'] = each['authorName']
                    one['question_plat'] = '来源 website'
                    data_list.append(one)

                    # try:
                    #     get_info(each['stockCode'],each['secid'])
                    # except Exception as e:
                    #     print(e,each['stockCode'],each['secid'])

                    company_set.add(each['stockCode'])
            except Exception as e:
                pass
        insert_data(data_list)
        time.sleep(3)
        n+=1


def run():
    # 连接database
    conn = pymysql.connect(
        host="127.0.0.1",
        user='root',
        password=password,
        database='auth',
        charset='utf8')

    # 得到一个可以执行SQL语句的光标对象
    cursor = conn.cursor()  # 执行完毕返回的结果集默认以元组显示

    cursor.execute("SELECT `company_http` FROM `crawl` ")
    data = cursor.fetchall()
    # 关闭光标对象
    cursor.close()

    # 关闭数据库连接
    conn.close()
    total = 0
    for each in data:

        matchObj  = re.match(r'.*?/?stockcode=(\d+)&orgId=(.+)', each[0], re.M | re.I)
        print(matchObj.group())
        stockcode = matchObj.group(1)
        orgId = matchObj.group(2)
        try:
            question(stockcode,orgId)
        except:
            print(stockcode)




if __name__ == '__main__':
    password='123456'
    run()
