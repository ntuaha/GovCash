# -*- coding: utf-8 -*- 

import re

#處理掉unicode 和 str 在ascii上的問題
import sys 
import os
import psycopg2
import datetime
import calendar
import csv
import math
import json
import urllib
#from string import maketrans 
from time import mktime as mktime

reload(sys) 
sys.setdefaultencoding('utf8') 


class Page:
	database=""
	user=""
	password=""
	host=""
	port=""
	conn = None
	filepath = '/home/aha/Project/GovCash/data/page.json'
	table = "page"
	def __init__(self,filepath):
		f = open(filepath,'r')
		self.database = f.readline()[:-1]
		self.user = f.readline()[:-1]
		self.password = f.readline()[:-1]
		self.host = f.readline()[:-1]
		self.port =f.readline()[:-1]
		f.close()

	def initial_load(self,file):
		print '執行重建Table'
		os.system('psql -d %s -f %s'%(self.database,file))
		print '下載更新檔案'
		#os.system('wget http://campaign-finance.g0v.ronny.tw/api/gettables -O  $s'%self.filepath)
		
	def work(self):
		#讀取資料
		
		#f = urllib.urlopen("http://campaign-finance.g0v.ronny.tw/api/gettables")
		f = open(self.filepath,'r')
		rows = json.loads(f.read())["data"]
		f.close()
		#開啓Database
		
		self.conn = psycopg2.connect(database=self.database, user=self.user, password=self.password, host=self.host, port=self.port)
		cur = self.conn.cursor()	
		for row in rows:
			#Debug
			#print "line:%s"%row['file']
			lists = row['file'].split("-")
			f = lists[0]
			if len(lists)>1:
				txn_code = lists[1].split(".")[0]
			else:
				txn_code ="NULL"

			if len(lists)>2:
				from_txndt = lists[2]
				to_txndt = lists[3]
				try:
					(year,month,day) = from_txndt.split("/")
					a = datetime.datetime(year=(int(year)+1911),month=int(month),day=int(day))
					from_txndt = "'%d-%s-%s'"%(int(year)+1911,month,day)
				except:
					from_txndt = "NULL"

				try:
					(year,month,day) = to_txndt.split("/")
					b = datetime.datetime(year=(int(year)+1911),month=int(month),day=int(day))
					to_txndt = "'%d-%s-%s'"%(int(year)+1911,month,day)
				except:
					to_txndt = "NULL"

				if from_txndt !="NULL" and to_txndt !="NULL" and a>b:
					temp = from_txndt
					from_txndt = to_txndt
					to_txndt = temp
			else:
				form_txndt="NULL"
				to_txndt="NULL"

			
			page = int(row['page'])
			pic_url = row['pic_url']
			pic_width = int(row['pic_width'])
			pic_height = int(row['pic_height'])
			if row.has_key("reverse") and row['reverse']:
				reverse = 1
			else:
				reverse = 0
			id = int(row['id'])
			tables_api_url = row['tables_api_url']
			
			
			sql = "INSERT INTO %s (file,page,pic_url,pic_width,pic_height,reverse,tables_api_url,from_txndt,to_txndt,txn_code,id) VALUES ('%s',%d,'%s',%d,%d,%d,'%s',%s,%s,'%s',%d)" %(self.table,f,page,pic_url,pic_width,pic_height,reverse,tables_api_url,from_txndt,to_txndt,txn_code,id)
			#Debug
			#print sql
			cur.execute(sql)
		#print '輸出檔案'
		#cur.execute("\copy (select * from %s) To '/home/aha/Project/GovCash/data/%s.csv' With CSV HEADER"%(self.table,self.table))
		self.conn.commit()
		self.conn.close()
		print "FINISH"




if __name__ == '__main__':
	worker = Page('/home/aha/Project/GovCash/link.info')
	worker.initial_load(sys.argv[1])
	worker.work()

