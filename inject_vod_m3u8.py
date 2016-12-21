#!/usr/bin/python
# coding: UTF-8
from __future__ import division
from decimal import *
import StringIO
import threading
import sys
import os
import datetime
import logging
import subprocess
import fcntl
import time
import random
import string
import hashlib
import urlparse, re

#Last modify: 2016-10-19
#使用方法 
#python inject_miss_url.py 183.207.130.60 miss.txt
#其中183.207.130.60 为HCS的实际地址，miss.txt 为需要注入的URL列表，一个URL一行
def DOWNLOAD_TS(host_info,url):
	thread_public_status.append(1)
	domain =urlparse.urlsplit(url)[1]
	try:
		port=domain.split(':')[1]
	except:
		port=80
	rel_url=urlparse.urlsplit(url)[2]+'?'+urlparse.urlsplit(url)[3]
	cmd="curl -s --connect-timeout 30 --max-time 36000 --limit-rate 1M -L -H 'Host: %s' 'http://%s:%s%s' -o /dev/null -w %%{http_code}:%%{time_connect}:%%{time_starttransfer}:%%{time_total}:%%{size_download}:%%{speed_download}" %(domain,host_info,port,rel_url)
	logging.info('exceute system command: %s'%cmd)
	p = subprocess.Popen(cmd, stdin = subprocess.PIPE,stdout = subprocess.PIPE, stderr = subprocess.PIPE, shell = True) 
	curlstat=p.stdout.readlines()
	logging.info('url %s download task quit with result: %s'%(url,curlstat[0]))
	#print curlstat
	sem.release()
	thread_public_status.pop()

def Get_M3U8_STAT(host_info,m3u8_url):
	domain =urlparse.urlsplit(m3u8_url)[1]
	try:
		port=domain.split(':')[1]
	except:
		port=80
	rel_url=urlparse.urlsplit(m3u8_url)[2]+'?'+urlparse.urlsplit(m3u8_url)[3]
	store_file='inject.m3u8'
	cmd="curl -s --connect-timeout 1 --max-time 3 -L -H 'Host: %s' 'http://%s:%s%s' -o %s -w %%{http_code}:%%{time_connect}:%%{time_starttransfer}:%%{time_total}:%%{size_download}:%%{speed_download}" %(domain,host_info,port,rel_url,store_file)
	logging.info('exceute system command: %s'%cmd)
	p = subprocess.Popen(cmd, stdin = subprocess.PIPE,stdout = subprocess.PIPE, stderr = subprocess.PIPE, shell = True) 
	curlstat=p.stdout.readlines()
	#print curlstat
	(http_code,time_connect,time_total,time_pretransfer,size_download,speed_download)=curlstat[0].split(':')
	fd = None
	try:
		fd = open(store_file, "r")
	except Exception, e:
		#print "Failed to open file %s. Reason: %s." % (IP_CFG, str(e))
		logging.info("Failed to open file %s. Reason: %s." % (store_file, str(e)))
		return None
	TSList = [ x.strip() for x in fd.readlines() if x and not x.strip().startswith("#")]
	os.remove(store_file)
	msg="%s %s %s %s %s %s %s\n"%(m3u8_url,http_code,time_connect,time_total,time_pretransfer,size_download,speed_download)
	return TSList
	
if __name__ == '__main__':
	#创建文件锁，确保只有一个脚本在运行
	try:
		fp = open('cdn_check.lck','w')  
		fcntl.flock(fp, fcntl.LOCK_EX|fcntl.LOCK_NB)
	except:
		print 'Script already running by other process!'
		sys.exit(1)
	#配置并发
	concurrence_t=100
	
	logging.basicConfig(level=logging.INFO,
					format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
					datefmt='%a, %d %b %Y %H:%M:%S',
					filename='download_ts.log',
					filemode='a')
	lockfile=threading.Lock()
	q=[] 
	thread_public_status=[]
	threads = [concurrence_t]
	sem=threading.Semaphore(concurrence_t)
	if len(sys.argv)!=2:
		print len(sys.argv)
		print("usage: python inject_miss_url.py miss.txt")
		sys.exit()
	else:
		if os.path.exists(sys.argv[1]):
			d_file=sys.argv[1]
			try:
				fd=file(d_file,'r')
				all_lines=fd.readlines()
				fd.close()
			except Exception, e:
				#print "Failed to open file %s. Reason: %s." % (IP_CFG, str(e))
				logging.info("Failed to open file %s. Reason: %s." % (d_file, str(e)))
		else:
			logging.error("%s file not exist!"%(d_file))
			sys.exit()
	#启动下载
	for line in all_lines:
		line=line.strip('\n')
		(m3u8_url,hcs_ip,live_domain)=line.strip('\n').split('|')
		TS_LIST=Get_M3U8_STAT(hcs_ip,m3u8_url)
		rel_url=urlparse.urlsplit(m3u8_url)[2]
		prefix_path="/".join(rel_url.split('/')[0:-1])
		
		for TS_URL in TS_LIST:
			full_url='http://'+live_domain+prefix_path+'/'+TS_URL
			print full_url
			sem.acquire()
			a=threading.Thread(target=DOWNLOAD_TS,args=(hcs_ip,full_url))
			a.start()
			q.append(a)
	for th in q:
		th.join()
	#用于等待子线程运行结束的最长时间。
	timer=36000
	#如果时间未用完继续等待
	while timer>0:
		if len(thread_public_status)==0:
			logging.info("All the thread finished")
			break
		else:
			time.sleep(5)
			timer=timer-5
		logging.info("all ts download finshed for %s" % (d_file))
		#ftp_up(filename)
	logging.info('*****************Script END************************')
	fcntl.flock(fp, fcntl.LOCK_UN)
	fp.close()