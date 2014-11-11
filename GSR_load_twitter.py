#!/usr/bin/env python
# -*- coding:utf-8 -*-
import json
import argparse
from unidecode import unidecode
from datetime import date, datetime,timedelta
import sqlite3 as lite
import os
import sys
# format: http://dev.datasift.com/docs/getting-started/data/twitter
# only consider the original tweets

def twitTimeToDBTime1(t): 
    TIME_FORMAT = "%Y-%m-%dT%H:%M:%S"
    createdAt = datetime.strptime(t,TIME_FORMAT)
    return createdAt.strftime("%Y-%m-%d")


def encode(s, coding): 
	try:
		return s.decode(coding)
	except:
		return s


def	read_twitter(filepath, database):	
	with open(filepath) as f:
		con = lite.connect(database)
		for line in f:
			try:
				tweet  = json.loads(line)
									
				eventDate = ''
				mitreId = ''
				country = ''
				state = ''
				city = ''
				population = ''
				gsslink = ''
				firstlink = ''
				
				eventDate = twitTimeToDBTime1(tweet['eventDate'])
				mitreId = tweet['mitreId']
				country = tweet['location'][0]
				state =  tweet['location'][1]
				city = tweet['location'][2]
				population = tweet['population']
				gsslink = tweet['derivedFrom']['gssLink']
				firstlink = tweet['derivedFrom']['firstReportedLink']
				GRSData = [country, state, city, eventDate, mitreId, population, gsslink, firstlink]

				with con:			
				        cur=con.cursor()
					if mitreId:		
				        	cur.execute("INSERT INTO t_gsr(country, state, city, eventDate, mitreId, population, gsslink, firstlink) VALUES(?,?,?,?,?,?,?,?)", GRSData)
	      
			except:
				print "first ", sys.exc_info()
				continue


def execute():
	for _file in os.listdir("/home/jf/Documents/EMBERS/GPS_tag/GSR/2012Nov-2014Mar"):
		try:		
			read_twitter("/home/jf/Documents/EMBERS/GPS_tag/GSR/2012Nov-2014Mar/"+_file,"/home/jf/Documents/EMBERS/GPS_tag/GSR_all.db")
			print "file is done: ", _file
		except:
			print sys.exc_info()
			print _file, "  file read error"
			continue


	
if __name__ == "__main__":
	ap = argparse.ArgumentParser("")
	ap.add_argument('--db', metavar='DATABASE', type=str, default="/home/jf/Documents/EMBERS/GPS_tag/GSR_all.db", help='GSR_all.db')	
	ap.add_argument('--sd',dest="start_day",metavar="the start day",type=str, default="2012-11-01", help="%Y-%m-%d")
	ap.add_argument('--ed',dest="end_day",metavar="the end day",type=str, default="2014-01-31", help="%Y-%m-%d")
	args = ap.parse_args()	
	database = args.db
	start_day = args.start_day
	end_day = args.end_day
	execute()
	





def	read_twitter_Sao(filepath, database):	
	with open(filepath) as f:
		con = lite.connect(database)
		for line in f:
			try:
				tweet  = json.loads(line)
									
				eventDate = ''				
				mitreId = ''
				flag = False

				if encode('Brazil', "utf-8")  in tweet['location'] and encode('São Paulo', "utf-8") in tweet['location']:
					flag = True
					eventDate = twitTimeToDBTime1(tweet['eventDate'])
					mitreId = tweet['mitreId']

				GRSData = ['Brazil', encode('São Paulo', "utf-8"), encode('São Paulo', "utf-8"), eventDate, mitreId ]

				with con:			
				        cur=con.cursor()
					if mitreId:		
				        	cur.execute("INSERT INTO t_grs(country, state, city, eventDate, mitreId) VALUES(?,?,?,?,?)", GRSData)
	      
			except:
				print "first ", sys.exc_info()
				continue



def	insert_count(start_day, end_day, database):
	start_day = datetime.strptime(start_day,"%Y-%m-%d")  # strptime, change string into datetime
	end_day = datetime.strptime(end_day,"%Y-%m-%d")
	conn = lite.connect(database)
	dd = [start_day + timedelta(days=x) for x in range((end_day-start_day).days + 1)]
	value1 = ''
	value2 = 0
	for day in dd:
		day = datetime.strftime(day, "%Y-%m-%d")
		try:					
			sql = "select eventDate, count(1) from t_grs where eventDate='{}' ".format( day )
			with conn:
				cur = conn.cursor()
				cur.execute(sql)
				row = cur.fetchone()
				sql2 = "insert into t_count(eventDate, today) values(?,?) "		
				if row[0] is not None:							
					cur.execute(sql2, [row[0], row[1] ] )
				else:
					cur.execute(sql2, [day, 0] )
				
		except:
			print "why ", sys.exc_info()
			continue
	
