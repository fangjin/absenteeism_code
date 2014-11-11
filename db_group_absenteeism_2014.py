#!/usr/bin/env python
# -*- coding:utf-8 -*-
import sys
import argparse
from datetime import datetime,timedelta
import time
import datetime
import os
import string
from unidecode import unidecode
from Levenshtein import ratio
import sqlite3 as lite
import matplotlib.pyplot as plt
import pylab
import numpy as np
from scipy import stats
					
"this function is to calculate given an end day, the fix_time's zscore"
def get_zscore(database, cal_day, fix_time, num):
	#start = datetime.strptime(cal_day, '%Y-%m-%d') + timedelta(days = -int(num))
	start = cal_day + timedelta(days = -int(num))
	#print "start ", start
	one_month = []
	con = lite.connect(database)	
	sql = "select count from t_city where created_at>='{}' and created_at<='{}' and created_at like '%{}' order by created_at".format(start, cal_day, fix_time)
	with con:
		cur = con.cursor()
		cur.execute(sql)
		rows = cur.fetchall()
		for row in rows:
			one_month.append(row[0])
	ar = np.array(one_month)
	end_fix = stats.zscore(ar)[-1] # the zscore of the end_day at fix_time

	fix_day = cal_day.strftime('%Y-%m-%d')
	created_at = " ".join([fix_day, fix_time])
	sql2 =  "update t_city set zscore30='{}' where created_at='{}' ".format(end_fix, created_at)
	with con:
		cur = con.cursor()
		cur.execute(sql2)




# firstly, distinguish weekday or weekend in the db, then calculate zscore depending on weekday or weekend. 
"given an end day and a fix_time, calculate their zscore depends on weekday and weekend, w_zscore28, always take one month ahead"
def get_zscore_weekday(database, cal_day, fix_time, num):
	one_month = []
	con = lite.connect(database)
	try:
		if cal_day.weekday() < 5:	
			sql = "select count from t_city where created_at<='{}' and created_at like '%{}' and daynum < 5 order by created_at desc limit '{}' ".format(cal_day, fix_time, 21) 
		elif cal_day.weekday() > 4:
			sql = "select count from t_city where created_at<='{}' and created_at like '%{}' and daynum >4 order by created_at desc limit 16 ".format(cal_day, fix_time, 9)  

		with con:
			cur = con.cursor()
			cur.execute(sql)
			rows = cur.fetchall()
			for row in rows:
				one_month.append(row[0])
		ar = np.array(one_month)
		end_fix = stats.zscore(ar)[0] # the zscore of the end_day at fix_time

		fix_day = cal_day.strftime('%Y-%m-%d')
		created_at = " ".join([fix_day, fix_time])
		sql2 =  "update t_city set w_zscore28='{}' where created_at='{}' ".format(end_fix, created_at)
		with con:
			cur = con.cursor()
			cur.execute(sql2)
	except:
		print sys.exc_info()



def get_zscore_city_weekday(database, cal_day, fix_time):
	city_list = ["Rio de Janeiro", "Salvador", "Brasília",  "Belo Horizonte", "Manaus", "Curitiba", "Recife", "Porto Alegre", "Belém", "Goiânia", "Guarulhos","Campinas", "Natal","João Pessoa"]	
	con = lite.connect(database)
	try:
		for city in city_list:
			one_month = []		
			if cal_day.weekday() < 5:	
				sql = "select count from t_city where city='{}' and created_at<='{}' and created_at like '%{}' and daynum < 5 order by created_at desc limit '{}' ".format(city, cal_day, fix_time, 21) 
			elif cal_day.weekday() > 4:
				sql = "select count from t_city where city='{}' and created_at<='{}' and created_at like '%{}' and daynum >4 order by created_at desc limit 16 ".format(city, cal_day, fix_time, 9)  

			with con:
				cur = con.cursor()
				cur.execute(sql)
				rows = cur.fetchall()
				for row in rows:
					one_month.append(row[0])
			ar = np.array(one_month)
			end_fix = stats.zscore(ar)[0] # the zscore of the end_day at fix_time

			fix_day = cal_day.strftime('%Y-%m-%d')
			created_at = " ".join([fix_day, fix_time])
			sql2 =  "update t_city set w_zscore28='{}' where city='{}' and created_at='{}' ".format(end_fix, city, created_at)
			with con:
				cur = con.cursor()
				cur.execute(sql2)
	except:
		print sys.exc_info()




def insert_zscore(database, begin_day, end_day, num):
	day_list = []
	time_list = ['00:00:00', '00:30:00', '01:00:00', '01:30:00', '02:00:00', '02:30:00', '03:00:00', '03:30:00','04:00:00', '04:30:00', '05:00:00', '05:30:00', '06:00:00', '06:30:00', '07:00:00', '07:30:00','08:00:00', '08:30:00', '09:00:00', '09:30:00', '10:00:00', '10:30:00', '11:00:00', '11:30:00','12:00:00', '12:30:00', '13:00:00', '13:30:00', '14:00:00', '14:30:00', '15:00:00', '15:30:00','16:00:00', '16:30:00', '17:00:00', '17:30:00','18:00:00', '18:30:00', '19:00:00', '19:30:00', '20:00:00', '20:30:00', '21:00:00', '21:30:00','22:00:00', '22:30:00', '23:00:00', '23:30:00']
	end_day = datetime.datetime.strptime(end_day,"%Y-%m-%d")
	begin_day = datetime.datetime.strptime(begin_day,"%Y-%m-%d")
	delta =  end_day - begin_day
	for i in range(delta.days + 1):
		day_list.append(begin_day + timedelta(days=i))
	
	for day in day_list:
		for time in time_list:			
			get_zscore_city_weekday(database, day, time) # for multiple cities




"translate the unique time to readable date"
def twitTimeToDBTime(t):
	return (datetime.datetime.fromtimestamp(int(t)).strftime('%Y-%m-%d %H:%M:%S'))


def encode(s, coding): 
	try:
		return s.decode(coding)
	except:
		return s


import geocode
"given a city, state, country, return its latitude and longitude"
def get_lat_log(city, state, country):	
	geo = geocode.Geo()
	LT = geo.resolve(city, state, country, geocode.RESOLUTION_LEVEL.city)
	return LT[6], LT[7]
	
	

def get_multiple_city_lat_log(database):
	lat_log_dic = {}
	con = lite.connect(database)
	for f in os.listdir("/home/jf/Documents/EMBERS/GPS_tag/time_series/2014test"):
		with open("/home/jf/Documents/EMBERS/GPS_tag/time_series/2014test/"+f) as ff:
			for line in ff:
				try:
					latitude = 0.0
					longitude = 0.0
					count = line.rsplit(" ", 1)[1]
					left = line.rsplit(" ", 1)[0]
					time1, city, state, country = left.split(',')
					time1 =  twitTimeToDBTime(time1)
					daynum = datetime.datetime.strptime(time1, '%Y-%m-%d %H:%M:%S').weekday()

					# to avoid calculate every city's lat and log every time, we store the loc_str and its latitude, logitude into dictionary
					loc_str = ",".join([city, state, country])					
					if loc_str not in lat_log_dic:
						lat_log_dic[loc_str] = {}
						latitude, longitude = get_lat_log(city, state, country)
						lat_log_dic[loc_str]["lat"] = latitude
						lat_log_dic[loc_str]["log"] = longitude
					else:
						latitude = lat_log_dic[loc_str]["lat"]
						longitude = lat_log_dic[loc_str]["log"]					
									
					cityData = [time1, encode(city, "utf-8"), encode(state, "utf-8"), country, int(count), daynum, latitude, longitude]
				
					with con:			
						cur=con.cursor()
						cur.execute("INSERT INTO t_city(created_at, city, state, country, count, daynum, latitude, longitude) VALUES(?,?,?,?,?,?,?,?)", cityData)

				except:
					print sys.exc_info()
					continue



def get_multiple_city_result(database):
	con = lite.connect(database)
	for f in os.listdir("/home/jf/Documents/EMBERS/GPS_tag/time_series/2014"):
		with open("/home/jf/Documents/EMBERS/GPS_tag/time_series/2014/"+f) as ff:
			for line in ff:
				try:
					latitude = 0.0
					longitude = 0.0
					count = line.rsplit(" ", 1)[1]
					left = line.rsplit(" ", 1)[0]
					time1, city, state, country = left.split(',')
					time1 =  twitTimeToDBTime(time1)
					daynum = datetime.datetime.strptime(time1, '%Y-%m-%d %H:%M:%S').weekday()
					latitude, longitude = get_lat_log(city, state, country)
									
					cityData = [time1, encode(city, "utf-8"), encode(state, "utf-8"), country, int(count), daynum, latitude, longitude]
				
					with con:			
						cur=con.cursor()
						cur.execute("INSERT INTO t_city(created_at, city, state, country, count, daynum, latitude, longitude) VALUES(?,?,?,?,?,?,?,?)", cityData)

				except:
					print sys.exc_info()
					continue

	
if __name__ == "__main__":
	ap = argparse.ArgumentParser("")
	ap.add_argument('--db', metavar='DATABASE', type=str, default="/home/jf/Documents/EMBERS/GPS_tag/timeSeries.db", help='timeSeries.db')	
	#ap.add_argument('--sd',dest="start_day",metavar="the start day",type=str, default="2012-08-01", help="%Y-%m-%d")
	#ap.add_argument('--ed',dest="end_day",metavar="the end day",type=str, default="2014-01-31", help="%Y-%m-%d")
	#ap.add_argument('--n',dest="num",metavar="loop day for zscore",type=str, default="28")
	args = ap.parse_args()
	database = args.db
	#start_day = args.start_day
	#end_day = args.end_day
	#num =  args.num
	get_multiple_city_result(database)
	


