#!/usr/bin/env python
# -*- coding:utf-8 -*-
import sys
import json
import datetime
import sqlite3 as lite
from unidecode import unidecode
import numpy as np
from scipy import stats

def certain_time_absent_score_two():
	con = lite.connect("/home/jf/Documents/EMBERS/GPS_tag/algorithm/score.db")
	fix_day = "2014-05-01"
	fix_time = "13:00:00"
	created_at = "2014-05-01 13:00:00"	

	result = {}
	city_dic = json.load( open("./country_gsr_lat_lon.json") ) # sort by country

	for loc in city_dic["Argentina"]:
		loc = loc.encode("utf-8")				
		try:
			one_month = []
			end_fix = 0
			sql = "select count from t_city where location='{}' and created_at<='{}' and created_at like '%{}' and daynum < 5 order by created_at desc limit '{}' ".format(loc, created_at, fix_time, 20)			

			with con:
				cur = con.cursor()
				cur.execute(sql)
				rows = cur.fetchall()

				for row in rows:
					one_month.append(row[0])
			ar = np.array(one_month)

			if np.std(ar):		
				end_fix = stats.zscore(ar)[0] # the zscore of the end_day at fix_time
	
			if created_at not in result:
				result[created_at] = {}
				result[created_at][loc] = end_fix
			else:
				if loc not in result[created_at]:
					result[created_at][loc] = end_fix
				
		except:
			print sys.exc_info()			
			continue
	with open("./event_absent_two/absent_two_Argentina_20140501:1300.json", "w") as out:
		out.write(json.dumps(result))


print datetime.datetime.now()
certain_time_absent_score_two()
print datetime.datetime.now()





def read():	
	country_list = ["Colombia", "Chile", "Mexico", "Venezuela", "Ecuador", "Uruguay", "Uruguay", "El Salvador", "Paraguay", "Brazil", "Argentina", "Peru", "Guyana", "Bolivia", "Suriname"]
	result = {}
	for c in country_list:
		result[c] = {}

	city_dic = json.load( open("./gsr_city_lat_log.json") )
	for loc in city_dic:
		#loc = loc.encode("utf-8")

		country = loc.strip().split(",")[-1]
		if country not in country_list:
			result[country] = {}
		result[country][loc] = city_dic[loc]

	with open("./country_gsr_lat_lon.json", "w") as out:
		out.write(json.dumps(result))



def certain_time_absent_score_two_list():
	con = lite.connect("/home/jf/Documents/EMBERS/GPS_tag/algorithm/score.db")
	#day_list = ["2014-05-01"]
	#time_list = ['18:00:00'] #, '13:30:00', '14:00:00', '14:30:00', '15:00:00', '15:30:00'
	fix_day = "2014-05-01"
	created_at = "2014-05-01 18:00:00"

	result = {}
	city_dic = json.load( open("./country_gsr_lat_lon.json") ) # sort by country

	for loc in city_dic["Brazil"]:
		loc = loc.encode("utf-8")
		for cal_day in day_list:						
			for fix_time in time_list:			
				try:
					one_month = []
					end_fix = 0
				
					fix_day = datetime.datetime.strptime(cal_day, '%Y-%m-%d')
					
					created_at = " ".join([cal_day, fix_time])
					
					
		
					if fix_day.weekday() < 5:	
						sql = "select count from t_city where location='{}' and created_at<='{}' and created_at like '%{}' and daynum < 5 order by created_at desc limit '{}' ".format(loc, created_at, fix_time, 20) 
					elif fix_day.weekday() > 4:
						sql = "select count from t_city where location='{}' and created_at<='{}' and created_at like '%{}' and daynum >4 order by created_at desc limit '{}' ".format(loc, created_at, fix_time, 8)

					with con:
						cur = con.cursor()
						cur.execute(sql)
						rows = cur.fetchall()
	
						for row in rows:
							one_month.append(row[0])
					ar = np.array(one_month)

					if np.std(ar):		
						end_fix = stats.zscore(ar)[0] # the zscore of the end_day at fix_time
					#record = "|".join([str(created_at), loc, str(end_fix)]) 
					#print record

					if created_at not in result:
						result[created_at] = {}
						result[created_at][loc] = end_fix
					else:
						if loc not in result[created_at]:
							result[created_at][loc] = end_fix
						
				except:
					#print sys.exc_info()			
					continue
	with open("./absent_two_Mexico_20130911_15:00.json", "w") as out:
		out.write(json.dumps(result))



