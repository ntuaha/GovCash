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
#from string import maketrans 
from time import mktime as mktime

reload(sys) 
sys.setdefaultencoding('utf8') 


class Transpose:
	database=""
	user=""
	password=""
	host=""
	port=""
	conn = None

	table = "GovCash_Txn"
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
		os.system('psql -d data -f %s'%file)
		
	def work(self):
		
		self.conn = psycopg2.connect(database=self.database, user=self.user, password=self.password, host=self.host, port=self.port)
		cur = self.conn.cursor()	
		cur.execute("SELECT count(*) from tablecolumn where page<>0;")
		count = cur.fetchone()[0]
		page=None
		r=None
		txn_dt=None
		txn_code=None
		user_nm=None
		id=None
		receive_money=None
		pay_money=None
		cash_ind=""
		area=None
		rate=None
		rate1=None
		start_index = 0
		jump_size = 1000000
		rate = None
		rate1 =None
		current_page = 0
		current_row = 0
		f = open("TEST.log","w+")
		while 1:
			print "Processing...       [%d,%d,%0.2f%%]"%(start_index,count,(float(start_index)/float(count)))
			cur.execute("SELECT * from tablecolumn where page<>0 and row <>1 order by page,row,col limit %d offset %d"%(jump_size,start_index))
			rows = cur.fetchall()
			for row in rows:
				try:
					if row[2]==2:
						if row[5] is not None:
							dd=row[3].split(" ")[0].split('/')
							year=dd[0]
							month=dd[1]
							day=dd[2]
							#print "%s %s|%s|"%(row[0],row[1],row[3])
							year = int(year)+1911
							try:
								datetime.datetime(year=year,month=int(month),day=int(day))
								txn_dt  = "%d-%02d-%02d"%(year,int(month),int(day))
								rate = math.log(float(row[5]))
							except:																						
								txn_dt='1970-01-01'
								rate=0
						else:
							txn_dt='1970-01-01'
							rate=0
					elif row[2]==3:
						txn_code = row[3]
						if row[5]!="" and row[5] is not None  and rate!=0:
							rate = math.log(float(row[5]))+rate
						else:
							rate=0
					elif row[2]==4: #姓名
						user_nm = row[3]
						if row[5]!="" and row[5] is not None  and rate!=0:
							rate = math.log(float(row[5]))+rate
							rate1= math.log(float(row[5]))
						else:
							rate = 0
							rate1 =0
					elif row[2]==5:
						id = row[3]
						if row[5]!="" and row[5] is not None  and rate!=0:
							rate = math.log(float(row[5]))+rate
							rate1= math.log(float(row[5]))+rate1
						else:
							rate = 0
							rate1 =0
					elif row[2]==6:
						if row[3]=="" or row[3] is None:
							receive_money = "NULL"
						else:
							receive_money = row[3]
						if row[5]!="" and row[5] is not None  and rate!=0:
							rate = math.log(float(row[5]))+rate
							rate1= math.log(float(row[5]))+rate1
						else:
							rate = 0
							rate1 =0
					elif row[2]==7:
						if row[3]=="" or row[3] is None:
							pay_money="NULL"
						else:
							pay_money = row[3]
						if row[5]!="" and row[5] is not None and rate!=0:
							rate = math.log(float(row[5]))+rate #winner_ratio
							rate1= math.log(float(row[5]))+rate1
						else:
							rate = 0
							rate1 =0
					elif row[2]==8:
						if row[3] is None:
							cash_ind = ""
						else:
							cash_ind = row[3]
					elif row[2]==9:
						page = row[0] #page
						r = row[1]  #row
						ans = row[3]  #ans
						if row[5]!="" and row[5] is not None  and rate!=0:
							rate = math.exp(math.log(float(row[5]))+rate) #還原
						else:
							rate=0
						rate1 = math.exp(rate1)
						sql = "INSERT INTO govcash (page,row,txn_dt,txn_code,user_nm,id,receive_money,pay_money,cash_ind,area,correct_rate,correct_rate_imp) VALUES (%d,%d,'%s','%s','%s','%s',%s,%s,'%s','%s',%f,%f);"%(page,r,txn_dt,txn_code,user_nm,id,receive_money,pay_money,cash_ind,ans,rate,rate1)
						f.write("%d,%d,%s,%s,%s,%s,%s,%s,%s,%s,%f,%f\n"%(page,r,txn_dt,txn_code,user_nm,id,receive_money,pay_money,cash_ind,ans,rate,rate1))
						#print sql
						cur.execute(sql)
						self.conn.commit()
				except Exception as inst:
					print "%s %s %s %s %s"%(row[0],row[1],row[2],row[3],row[5])
					print inst
					

			#大於總列數的時候離開
			start_index = start_index+jump_size
			if start_index > count:
				break

		f.close()
		self.conn.close()
		print "FINISH"



if __name__ == '__main__':
	worker = Transpose('/home/aha/Project/GovCash/link.info')
	worker.initial_load('/home/aha/Project/GovCash/sql/CashGov.sql')
	worker.work()

