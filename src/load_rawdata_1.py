# -*- coding: utf-8 -*- 

import re

#處理掉unicode 和 str 在ascii上的問題
import sys 
import os
import psycopg2
import datetime
import calendar
import csv

reload(sys) 
sys.setdefaultencoding('utf8') 


class Load_RawData_1:
	database=""
	user=""
	password=""
	host=""
	port=""
	conn = None
	datasource = ""
	table = "GovCash_Txn"
	def __init__(self,filepath,datasource):
		f = open(filepath,'r')
		self.database = f.readline()[:-1]
		self.user = f.readline()[:-1]
		self.password = f.readline()[:-1]
		self.host = f.readline()[:-1]
		self.port =f.readline()[:-1]
		self.datasource = datasource
		f.close()
		
	def work(self):
		self.conn = psycopg2.connect(database=self.database, user=self.user, password=self.password, host=self.host, port=self.port)
		cur = self.conn.cursor()	
		try:
			cur.execute("DELETE FROM %s"%self.table);
		except:
			print "%s doesn't exist! "%self.table

		f = open(self.datasource,'r')
		rows = csv.reader(f, delimiter=',', quotechar='"')
		error = open('./error.log','w+')

		f.readline()  #ignore the 1st line in the file
		for row in rows:
			try:
				
				[page,row,col,ans,user_id,time] = row
				ans = ans.strip()
				ans = re.sub("\'","",ans)

				if col == 9:
					ans = ans.replace('台','臺')
				elif col == 7:
					ans = ans.replace(',','')
					ans = ans.replace(' ','')

				
				#print int(time)
				current_time = datetime.datetime.fromtimestamp(int(time)).strftime('%Y-%m-%d %H:%M:%S')
				#print current_time 
				cur.execute("INSERT INTO %s (page,row,col,ans,user_id,time) VALUES (%s,%s,%s,'%s',%s,'%s');"%(self.table,page,row,col,ans,user_id,current_time))
				#print "INSERT INTO GovCash_OCR_TXN (page,row,col,ans,user_id,time) VALUES (%s,%s,%s,'%s',%s,'%s');"%(page,row,col,ans,user_id,current_time)
			except ValueError as e:
				print "%s : %s"%(e,line)
				error.write("%s"%(line))
			
			

		error.close()	
		f.close()
		self.conn.commit()
		self.conn.close()
		print "FINISH"



if __name__ == '__main__':
	worker = Load_RawData_1('/home/aha/Project/GovCash/link.info','/home/aha/Data/GovCash/ans.csv')
	worker.work()