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
					if len(ans)<3:
						ans = "FFFFFFFFFFFFF"
					elif ans[0].isalpha() and ans[1:2].isdigit():
						ans = ans[0:2]+"*******"
					elif ans[0].isalpha() and ans[1].isdigit():
						ans = ans[0:1]+"********"
					elif ans.isdigit():
						#統編要等於8碼
						if len(ans) != 8:
							ans = "FFFFFFFFFFFFF"
					else:
						ans = "FFFFFFFFFFFFF"
				
				elif col == "7" or col == "6":
					ans = ans.replace(',','')
					ans = ans.replace('，','')
					ans = ans.replace(' ','')
					if ans.isdigit() ==False:
						ans = "FFFFFFFFFFFFF"

				elif col == "2":
					ans = ans.replace('／','/')
					ans = ans.replace('╱','/')
					result = re.match( r'(\d+/\d+/\d+)', ans, re.M|re.I)
					if result:
						pass
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
				current_time = datetime.datetime.fromtimestamp(int(time)).strftime('%Y-%m-%d %H:%M:%S')
				#print current_time 
				cur.execute("INSERT INTO %s (page,row,col,ans,original_ans,user_id,time) VALUES (%s,%s,%s,'%s','%s',%s,'%s');"%(self.table,page,row,col,ans,original_ans,user_id,current_time))
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
