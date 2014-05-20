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

	def initial_load(self,file,canditate):
		print '執行重建Table'
		os.system('psql -d %s -f %s'%(self.database,file))
		os.system('psql -d %s -f %s'%(self.database,canditate))
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
			
			f = lists[0].replace('七','7').replace('八','8').replace('十','10').replace('、',',')
			f = re.sub(r'第(九)[屆|任]', '9',f, flags=re.IGNORECASE)
			m = re.match(u'第(\d+)[屆|任](.+)擬參選人(.+)政治獻金專戶',f,flags=re.IGNORECASE|re.UNICODE|re.X)
			term = int(m.group(1))
			position = m.group(2).split(',')
			p1 = position[0]
			if len(position) ==2:
				p2 = "'"+position[1]+"'"
			else:
				p2 = 'Null'
			alist = m.group(3).split(',')
			a1= alist[0]
			if len(alist) == 2:
				a2 = "'"+alist[1]+"'"
			else:
				a2 = 'Null'
			#Find out Candidate_no
			if p2 == "Null" or a2 =="Null":
				sql = "SELECT candidate_no from candidate where account1 = '%s' and term=%d and position1='%s' "%(a1,term,p1)	
			else:
				sql = "SELECT candidate_no from candidate where account1 = '%s' and account2=%s and term=%d and position1='%s' and position2=%s "%(a1,a2,term,p1,p2)	
			cur.execute(sql)
			rows =  cur.fetchall()
			run = True
			candidate_no = -1
			for subrow in rows:
				run  = False
				candidate_no = subrow[0]
			if run:
				sql = "INSERT INTO candidate (term,position1,position2,account1,account2) VALUES (%d,'%s',%s,'%s',%s);" %(term,p1,p2,a1,a2)	
				
				cur.execute(sql)
				self.conn.commit()
				if p2 == "Null" or a2 =="Null":
					sql = "SELECT candidate_no from candidate where account1 = '%s' and term=%d and position1='%s' "%(a1,term,p1)	
				else:
					sql = "SELECT candidate_no from candidate where account1 = '%s' and account2=%s and term=%d and position1='%s' and position2=%s "%(a1,a2,term,p1,p2)		
				
				cur.execute(sql)
				candidate_no =  cur.fetchall()[0][0]





			
			'''
			p = re.compile('第(\d+)[屆|任](.+)擬參選人(.+)政治獻金專戶',re.IGNORECASE|re.UNICODE|re.X)
			m = p.match(f)
			print f
			term = int(m.groups(1))
			position = m.groups(2)
			account = m.groups(3)
			print "%d, %s, %s"%(term,position,account)
'''


			if len(lists)>1:
				txn_code = "'"+lists[1].split(".")[0]+"'"
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
			#塞入資料		
			sql = "INSERT INTO %s (file,page,pic_url,pic_width,pic_height,reverse,tables_api_url,from_txndt,to_txndt,txn_code,id,candidate_no) VALUES ('%s',%d,'%s',%d,%d,%d,'%s',%s,%s,%s,%d,%d)" %(self.table,f,page,pic_url,pic_width,pic_height,reverse,tables_api_url,from_txndt,to_txndt,txn_code,id,candidate_no)
			cur.execute(sql)
		self.conn.commit()
		self.conn.close()
		print "FINISH"




if __name__ == '__main__':
	worker = Page('/home/aha/Project/GovCash/link.info')
	worker.initial_load(sys.argv[1],sys.argv[2])
	worker.work()

