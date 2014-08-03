# -*- coding: utf-8 -*- 

import sys 
reload(sys) 
sys.setdefaultencoding('utf8') 

from PAYANDREC import *

if __name__ == '__main__':
	worker = PAYANDREC('/home/aha/Project/GovCash/link.info')
	worker.work()
