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

class Bzi(object):

    def __init__(self):
        ua = UserAgent().random
        self.headers={
            'User-Agent':ua
        }
        self.code=self.position_code()

        self.db = pymysql.connect(host='localhost', user='root', password='123456', db='spiders')
        self.cursor = self.db.cursor()

    def response_headler(self,url):  #构造响应
        try:
            response=requests.get(url=url,headers=self.headers)
        except:
            self.response_headler(url)
        else:
            return response

    def parse_page(self,response):  #解析详细页面url
        html=pq(response.text)
        result=html.find('#main > div > div.job-list > ul li').items()
        items=[]
        for res in result:
            item='https://www.zhipin.com'+res('a').attr('href')
            items.append(item)
        return items

    def crawl_info(self,info_url,position):  #爬取页面详细信息
        for url in info_url:

            response=self.response_headler(url)
            data=self.parse_info_parse(response)
            print('爬取页面信息成功,开始保存')
            try:
                self.sava_data(data,position)
            except:
                print('保存失败,重新保存')
                time.sleep(5)
                self.crawl_info(data,position)
            else:
                time.sleep(5)

    def parse_info_parse(self,response): #解析页面详细信息
        html=pq(response.text)
        print('解析完成')
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
        print('准备保存')
        try:
            print('开始保存')
            self.cursor.execute(SQL)
            self.db.commit()
        except:
            print('start_rollback')
            self.db.rollback()


        print('sava_mysql_scceed')

        print('sava_over')

    def next_page(self,response,position):  #获取下一页链接
        html=pq(response.text)
        try:
            next='https://www.zhipin.com'+html('#main > div > div.job-list > div.page > a.next').attr('href')
            if html('#main > div > div.job-list > div.page > a.next'):
                self.main(next,position)
        except:
            self.next_page(response,position)
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
            print('解析失败，重新解析本页面')
            self.main(url,position)
        else:
            self.next_page(response,position)
            print('页面爬取成功，开始新一页爬取')

    def crawl_main(self,url,position):  #first爬取
        print('first_crawl')
        response = self.response_headler(url)
        try:
            info_url = self.parse_page(response)
            self.crawl_info(info_url,position)
        except:
            print('失败，重新爬取本页面')
            self.crawl_main(url,position)

        else:
            print('爬取成功')
            print('准备翻页爬取...')
            self.next_page(response,position)

    def crawl_total(self): #主程序
        url = 'https://www.zhipin.com/c101010100-p{code}/?page=1&ka=page-next'
        print('start_crawl...')
        for datas in range(1,len(self.code)+1):
            data=random.choice(self.code)
            print(data)
            main_code=data.get('category_code')
            main_name=data.get('position')
            self.crawl_main(url.format(code=main_code),main_name)
        self.db.close()
        print('crawl...over')

if __name__ == '__main__':
    c=Bzi()
    c.crawl_total()
