import csv
import json

import os
import random
import time
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

    def response_headler(self,url):  #构造响应
        proxy = ['58.218.205.52:8586','182.101.202.124:4213']
        ra=random.choice(proxy)
        proxies = {
            'https:': 'https://' + ra,
            'http:': 'http://' + ra
        }
        print('构造中...',proxies)
        response=requests.get(url=url,headers=self.headers,proxies=proxies)
        print('构造完成')
        return  response



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
            response=self.response_headler(url)
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
        print(self.file_path+r'\{position}.txt'.format(position=position))
        print(self.file_path+r'\{position}.csv'.format(position=position))
        with open(self.file_path+r'\{position}.txt'.format(position=position),'a',encoding='utf-8') as f:
            f.write(json.dumps(data,ensure_ascii=False)+'\n')
            f.close()
            print('sava text_file succeed')

        with open(self.file_path+r'\{position}.csv'.format(position=position),'a',encoding='utf_8_sig',newline='') as fp:
            self.f_csv=csv.DictWriter(fp,headers)
            self.f_csv.writerow(data)
            fp.close()
            print('sava csv_file succeed')

        sql="insert into test(company_name, city, demand, work_name,money, position_details) VALUES ('{company_name}','{city}','{demand}','{work_name}','{money}','{position_details}')"
        SQL=sql.format(company_name=data.get('company_name'), city=data.get('city'), demand=data.get('demand'),
                   work_name=data.get('work_name'), money=data.get('money'),
                   position_details=','.join(data.get('position_details')))
        print(SQL)
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
            if next!='https://www.zhipin.comjavascript:;':
                print('开始爬取翻页信息')
                self.main(next,position)
            else:
                print('已到尾页')
        except TypeError as e :
            print(e)
        else:
            print('获取下一页url成功')



    def main(self,url,position):  #翻页详细信息爬取
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
            print('爬取页面成功')
            print('开始准备翻页爬取...')
            self.next_page(response,position)

    def crawl_total(self): #主程序
        url = 'https://www.zhipin.com/c101010100-p{code}/?page=1&ka=page-next'
        print('start_crawl...')
        for data in self.code: #datas is int
            # data=random.choice(self.code)
            main_code = data.get('category_code')
            print(data)
            file_name=data.get('category_position')
            catalogue_name=data.get('position_name')
            totel_catalogue_name=data.get('position_totel')
            file_dir = os.getcwd()
            self.file_path = os.path.join(file_dir,totel_catalogue_name,catalogue_name,file_name)
            print(self.file_path)
            print(os.path.exists(os.path.join(self.file_path)))
            if os.path.exists(os.path.join(self.file_path)):
                print('文件已存在，不实行创建')
            else:
                os.makedirs(self.file_path)
            # print('爬取的职位信息',data)
            # self.crawl_main(url.format(code=main_code),file_name)
        print('crawl...over')

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
                    items['position_totel']=category['name']
                    items['position_name']=data.get('name')
                    items['category_position']=sub.get('name')
                    items['category_code']=sub.get('code')
                    item.append(items)
        print('爬取职位信息成功')
        return item
if __name__ == '__main__':
    c=Bzi()
    c.crawl_total()
