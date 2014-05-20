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



class PAYANDREC:
	database=""
	user=""
	password=""
	host=""
	port=""
	conn = None
	filepath = '/home/aha/Project/GovCash/data/pay_and_rec.json'
	table = "page"
	def __init__(self,filepath):
		f = open(filepath,'r')
		self.database = f.readline()[:-1]
		self.user = f.readline()[:-1]
		self.password = f.readline()[:-1]
		self.host = f.readline()[:-1]
		self.port =f.readline()[:-1]
		f.close()


	def work(self):
		#讀取資料

		f = open(self.filepath,'w+')
		

		#開啓Database
		self.conn = psycopg2.connect(database=self.database, user=self.user, password=self.password, host=self.host, port=self.port)
		cur = self.conn.cursor()	
		sql = "select A.candidate_no,sum(receive_money) as r , sum(pay_money) as p,B.account1,B.position1,term from govcash as A left join candidate as B on (A.candidate_no = B.candidate_no) group by A.candidate_no,B.account1,B.position1,term;"	
		cur.execute(sql)
		rows =  cur.fetchall()
		f.write("[\n");
		data=[]
		for row in rows:
			[g0,g1,g2,g3,g4,g5] = row
			print row
			if row[2] == None:
				g2 = -1
			if row[1] == None:
				g1 = -1
			if row[3] == None:
				g3 = "無名"
			if row[4] == None:
				g4 = "無名"
			if row[5] == None:
				g5 = 0
			d = "{\"chart_title\": \"第%d屆%s-%s\",\"unit\": \"百萬\",\"支出\": %0.2f,\"收入\":%0.2f}\n"%(int(g5),g4,g3,int(g2)/1e6,int(g1)/1e6)
			data.append(d)
		string = ",".join(data)
		f.write("%s]"%string)





		self.conn.close()
		f.close()
		
		



if __name__ == '__main__':
	worker = PAYANDREC('/home/aha/Project/GovCash/link.info')
	worker.work()
