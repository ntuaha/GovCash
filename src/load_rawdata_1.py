# -*- coding: utf-8 -*- 

import re

#處理掉unicode 和 str 在ascii上的問題
import sys 
import os
import psycopg2
import datetime
import calendar
import csv
#from string import maketrans 

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

	def initial_load(self,sql):
		print '執行重建Table'
		os.system('psql -d %s -f %s'%(self.database,sql))
		
	def work(self):
		self.conn = psycopg2.connect(database=self.database, user=self.user, password=self.password, host=self.host, port=self.port)
		cur = self.conn.cursor()	

		f = open(self.datasource,'r')
		rows = csv.reader(f, delimiter=',', quotechar='"')
		error = open('./error.log','w+')

		f.readline()  #ignore the 1st line in the file

		line = 0
		for row in rows:
			line = line+1
			try:
				
				[page,row,col,ans,user_id,time] = row
				ans = ans.strip()
				ans = re.sub("\'","",ans)			
				original_ans = ans
				ans = ans.replace('０','0')
				ans = ans.replace('１','1')
				ans = ans.replace('２','2')
				ans = ans.replace('３','3')
				ans = ans.replace('４','4')
				ans = ans.replace('５','5')
				ans = ans.replace('６','6')
				ans = ans.replace('７','7')
				ans = ans.replace('８','8')
				ans = ans.replace('９','9')
				if col == "9":
					ans = ans.replace('台','臺')
				elif col =="4":
					if ans == "":
						ans = "FFFFFFFFFFFFF"
					else:
						ans = ans.replace('－','-')
						ans = ans.replace('—','-')
						ans = ans.replace(' - ','-')
						ans = ans.replace('　','-')
						ans = ans.replace('  ','-')
						ans = ans.replace('   ','-')
						ans = ans.replace('（','(')
						ans = ans.replace('）',')')
					
				elif col =="5":
				#身分證處理
					ans = ans.upper()
					#64/2/5
					if ans=="" or '政黨' in ans:
						pass
					elif len(ans)<3:
						ans = "FFFFFFFFFFFFF"
					elif ans[0].isalpha() and ans[1:3].isdigit():
						ans = ans[0:3]+"*******"
					elif ans[0].isalpha() and ans[2].isdigit():
						ans = ans[0:2]+"********"
					elif ans.isdigit():
						#統編要等於8碼
						#if len(ans) != 8:
						#	ans = "FFFFFFFFFFFFF"
						#有時候是原始資料有誤
						pass
					else:
						ans = "FFFFFFFFFFFFF"
				
				elif col == "7" or col == "6":
					ans = ans.replace(',','')
					ans = ans.replace('，','')
					ans = ans.replace(' ','')
					if ans=="":
						pass
					elif ans.isdigit() ==False:
						ans = "FFFFFFFFFFFFF"

				elif col == "2":
					ans = ans.split(" ")[0]
					ans = ans.replace('／','/')
					ans = ans.replace('╱','/')
					result = re.match( r'(\d+/\d+/\d+)', ans, re.M|re.I)
					#格式檢定
					if result:
						#日期檢定
						dd=ans.split('/')
						year=dd[0]
						month=dd[1]
						day=dd[2]
						year = int(year)+1911
						try:
							datetime.datetime(year=year,month=int(month),day=int(day))
						except:																						
							ans = "FFFFFFFFFFFFF"						
					else:
						ans = "FFFFFFFFFFFFF"
					


				elif col== "8":
					if ans.find("是") != -1:
						ans = "是"
					elif ans.find("否") != -1:
						ans = "否"
					elif ans=="":
						pass
					else:
						ans = "FFFFFFFFFFFFF"


				
				#print int(time)
				#因為有些t值不見了
				if time != "":
					current_time = datetime.datetime.fromtimestamp(int(time)).strftime('%Y-%m-%d %H:%M:%S')
				else:
					current_time = datetime.datetime.fromtimestamp(0).strftime('%Y-%m-%d %H:%M:%S')
				#print current_time 
				cur.execute("INSERT INTO %s (page,row,col,ans,original_ans,user_id,time) VALUES (%s,%s,%s,'%s','%s',%s,'%s');"%(self.table,page,row,col,ans,original_ans,user_id,current_time))
				#print "INSERT INTO %s (page,row,col,ans,user_id,time) VALUES (%s,%s,%s,'%s',%s,'%s');"%(self.table,page,row,col,ans,user_id,current_time)
				if line%100000==0:
					self.conn.commit()
			except ValueError as e:
				print "%s : %s"%(e,line)
				error.write("%s"%(line))
			
			

		error.close()	
		f.close()
		self.conn.commit()
		self.conn.close()
		print "FINISH"



if __name__ == '__main__':
	worker = Load_RawData_1('/home/aha/Project/GovCash/link.info',sys.argv[1])
	worker.initial_load(sys.argv[2])
	worker.work()

