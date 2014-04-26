# -*- coding: utf-8 -*- 

import re

#處理掉unicode 和 str 在ascii上的問題
import sys 
import os
import psycopg2

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
	def __init__(self,filepath,datasource):
		f = open(filepath,'r')
		self.database = f.readline()
		self.user = f.readline()
		self.password = f.readline()
		self.host = f.readline()
		self.port =f.readline()
		f.close()
		
	def work(self):
		self.conn = psycopg2.connect(database=self.database, user=self.user, password=self.password, host=self.host, port=self.port)
		cur = conn.cursor()	
		f = open(self.datasource,'r')
		a = f.readline()
		print a
		f.close()



if __name__ == '__main__':
	worker = Load_RawData_1('/home/aha/Project/GovCash/link.info','/home/aha/Data/GovCash/ans.csv')