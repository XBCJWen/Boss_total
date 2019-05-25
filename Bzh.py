import csv
import json
import os
import random
import re
import time

import pandas
import pymongo
import pymysql
import requests
from fake_useragent import UserAgent
from pyquery import PyQuery as pq
from requests import RequestException

class Bzi(object):

    def __init__(self):
        ua = UserAgent().random
        self.headers={
            'User-Agent':ua
        }
        self.code=self.position_code()

    def response_headler(self,url):  #构造响应
        proxy = ['58.218.201.103:4112', '58.218.201.103:3283', '58.218.201.126:7557', '58.218.201.126:7637',
                 '58.218.201.103:2645', '58.218.205.47:5239', '58.218.201.108:5294', '58.218.201.126:4083',
                 '58.218.205.49:3990', '58.218.205.49:5725', '58.218.205.48:2338', '58.218.205.48:7183',
                 '58.218.201.126:3903', '58.218.205.47:2989', '58.218.201.103:7108', '58.218.201.108:8319',
                 '58.218.201.108:5119', '58.218.205.48:9458', '58.218.201.126:8840', '58.218.205.48:3473']
        proxies = {

            'https:': 'https://' + random.choice(proxy),
            'http:': 'https://' + random.choice(proxy)
        }
        print('构造中...')
        try:
            response=requests.get(url=url,headers=self.headers,proxies=proxies)
            if response==200:
                print('构造完成')
                return  response
            return  None
        except  RequestException:
            return None


    def parse_page(self,response):  #解析详细页面url
        html=pq(response.text)
        print('开始解析详细页面url')
        try:
            result=html.find('#main > div > div.job-list > ul li').items()
            items=[]
            for res in result:
                item='https://www.zhipin.com'+res('a').attr('href')
                items.append(item)
        except:
            print('pass')
        else:
            print('解析详细页面url成功')
            return items

    def crawl_info(self,info_url,position):  #爬取页面详细信息
        for url in info_url:
            print('sadfasdf')
            response=self.response_headler(url)
            print('asdfasdf',response.text)
            data=self.parse_info_parse(response)
            print('解析页面详细信息完成')
            print('爬取页面信息成功,开始保存')
            try:
                self.sava_data(data,position)
                print('totai_sava_succeed')
            except:
                print('保存失败,跳过此职位')
                time.sleep(random.randint(3,10))
            else:
                time.sleep(random.randint(3,10))

    def parse_info_parse(self,response): #解析页面详细信息
        html=pq(response.text)
        print('开始解析页面详细信息')
        return {
            "company_name":html('#main > div.job-box > div > div.job-sider > div.sider-company > div > a:nth-child(1)').attr('title').strip(),
            'city': html('#main > div.job-banner > div > div > div.info-primary > p').text()[:2],
            'demand':html('#main > div.job-banner > div > div > div.info-primary > p').text()[2:],
            'work_name':html('#main > div.job-banner > div > div > div.info-primary > div.name > h1').text(),
            'money':html('#main > div.job-banner > div > div > div.info-primary > div.name > span').text(),
            'position_details':html('#main > div.job-box > div > div.job-detail > div.detail-content > div:nth-child(1) > div').text().split('\n'),
        }

    def sava_data(self,data,position):  #保存数据
        print('sava_data...',data)
        self.db = pymysql.connect(host='localhost', user='root', password='123456', db='spiders')
        self.cursor = self.db.cursor()
        headers=['company_name','city','demand','work_name','money','position_details']
        with open('{position}.txt'.format(position=position),'a',encoding='utf-8') as f:
            f.write(json.dumps(data,ensure_ascii=False)+'\n')
            print('sava text_file succeed')

        with open('{position}.csv'.format(position=position),'a',encoding='utf_8_sig',newline='') as fp:
            self.f_csv=csv.DictWriter(fp,headers)
            self.f_csv.writerow(data)
            print('sava csv_file succeed')

        sql="insert into test(company_name, city, demand, work_name,money, position_details) VALUES ('{company_name}','{city}','{demand}','{work_name}','{money}','{position_details}')"
        SQL=sql.format(company_name=data.get('company_name'), city=data.get('city'), demand=data.get('demand'),
                   work_name=data.get('work_name'), money=data.get('money'),
                   position_details=','.join(data.get('position_details')))
        try:
            print('开始保存mysql')
            self.cursor.execute(SQL)
            self.db.commit()
        except:
            print('start_rollback')
            self.db.rollback()
        else:
            self.db.close()
        print('sava_mysql_scceed')
        print('sava_over')

    def next_page(self,response,position):  #获取下一页链接
        html=pq(response.text)
        try:
            next='https://www.zhipin.com'+html('#main > div > div.job-list > div.page > a.next').attr('href')
            print(next)
            if html('#main > div > div.job-list > div.page > a.next'):
                print('开始爬取翻页信息')
                self.main(next,position)
        except TypeError as e :
            print(e)
        else:
            print('获取下一页成功')

    def position_code(self): #crawl positioninfo code
        print('爬取职位信息')
        url='https://www.zhipin.com/wapi/zpCommon/data/position.json'
        response=self.response_headler(url)
        datas=json.loads(response.text, encoding=False)
        item=[]
        for data in datas.get('zpData'):
            for category in data.get('subLevelModelList'):
                for sub in category.get('subLevelModelList'):
                    items={}
                    items['position']=category.get('name')
                    items['category_position']=sub.get('name')
                    items['category_code']=sub.get('code')
                    item.append(items)
        print('爬取职位信息成功')
        return item

    def main(self,url,position):  #翻页爬取
        print('翻页成功...','开始爬取')
        response=self.response_headler(url)
        try:
            info_url = self.parse_page(response)
            self.crawl_info(info_url,position)
        except:
            print('解析失败，跳过本页面')
        else:
            self.next_page(response,position)
            print('页面爬取成功，开始新一页爬取')

    def crawl_main(self,url,position):  #first爬取
        response = self.response_headler(url)
        try:
            info_url = self.parse_page(response)
            self.crawl_info(info_url,position)
        except:
            print('失败，跳过本页面')
        else:
            print('爬取成功')
            print('准备翻页爬取...')
            self.next_page(response,position)

    def crawl_total(self): #主程序
        url = 'https://www.zhipin.com/c101010100-p{code}/?page=1&ka=page-next'
        print('start_crawl...')
        for datas in range(1,len(self.code)+1): #datas is int
            data=random.choice(self.code)
            main_code=data.get('category_code')
            main_name=data.get('position')
            print('爬取的职位分类信息',data)
            self.crawl_main(url.format(code=main_code),main_name)
        print('crawl...over')

if __name__ == '__main__':
    c=Bzi()
    c.crawl_total()
