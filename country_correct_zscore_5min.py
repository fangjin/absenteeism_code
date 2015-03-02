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
import sqlite3 as lite
import numpy as np
from scipy import stats
import json


					
"translate the unique time to readable date"
def twitTimeToDBTime(t):
	return (datetime.datetime.fromtimestamp(int(t)).strftime('%Y-%m-%d %H:%M:%S'))


def encode(s, coding): 
	try:
		return s.decode(coding)
	except:
		return s



"given a city, state, country, return its latitude and longitude"
def get_lat_log(city, state, country):	
	geo = geocode.Geo()
	LT = geo.resolve(city, state, country, geocode.RESOLUTION_LEVEL.city)
	return LT[6], LT[7]
	


"get all the gsr cities and their latitude, logitude"
# pay attention to code format, using utf-8
def get_gsr_city_lat_log(gsr_db):
	lat_log_dic = {}
	con = lite.connect(gsr_db)
	sql = "select distinct(city), state, country from t_gsr"
	with con:
		cur=con.cursor()		
		cur.execute(sql)
		rows = cur.fetchall()
		for row in rows:			
			city = row[0]			
			state = row[1]
			country = row[2]
			loc_str = ",".join([city, state, country])
			try:					
				if loc_str not in lat_log_dic:
					lat_log_dic[loc_str] = {}
					latitude, longitude = get_lat_log(city, state, country)
					lat_log_dic[loc_str]["lat"] = latitude
					lat_log_dic[loc_str]["log"] = longitude				
			except:
					print sys.exc_info()
					continue

	with open("./gsr_lat_log.json","w") as output_file:
		dump = json.dumps(lat_log_dic)
		if type(dump) == unicode:
			dump = dump.encode('utf-8')
		output_file.write("%s\n" % dump)



def pure_dic():
	location_dic = json.load( open("./gsr_lat_log.json") )
	delete_list = []
	for key in location_dic:
		if location_dic[key] == {}:
			delete_list.append(key)
	for k in delete_list:
		del location_dic[k]
	with open("./new_gsr_lat_log.json","w") as output:
		json.dump(location_dic, output)



## make sure they are unicode during the processing, when write into files, use utf-8; #city = unicode(city, errors='ignore')
def insert_city_time_series():
	location_dic = json.load( open("./gsr_city_lat_log.json") )
	for f in os.listdir("/home/jf/Documents/EMBERS/GPS_tag/time_series/daily_time"):
		with open("/home/jf/Documents/EMBERS/GPS_tag/time_series/daily_time/"+f) as ff:
			#print f
			for line in ff:
				try:	
					line = line.strip().replace("\n", "")					
					left = line.rsplit(" ", 1)[0]
					time1, city, state, country = left.strip().replace("\n", "").split(',')
					time1 = twitTimeToDBTime(time1)					
					location = ",".join([city, state, country])
					location = location.decode("utf-8")
 					
					if location in location_dic:				
						record = {}
						record["count"] = line.rsplit(" ", 1)[1]
						record["created_at"] =  time1
						record["location"] = encode(location, "utf-8")
						record["daynum"] = datetime.datetime.strptime(time1, '%Y-%m-%d %H:%M:%S').weekday()
						location_file = "./daily_city_time_series_45/%s" % location		
						with open(location_file, "a") as out:
							out.write(json.dumps(record) + "\n")
				except:
					print sys.exc_info()
					continue

#insert_city_time_series()



# step 1, convert all the count from score.db into files
def get_time_interval(begin_day, end_day):
	time_interval = []
	day_list = []
	time_list = ['00:00:00', '01:00:00', '02:00:00', '03:00:00', '04:00:00', '05:00:00', '06:00:00', '07:00:00', '08:00:00', '09:00:00', '10:00:00', '11:00:00', '12:00:00', '13:00:00', '14:00:00', '15:00:00', '16:00:00', '17:00:00', '18:00:00', '19:00:00', '20:00:00', '21:00:00', '22:00:00', '23:00:00', '00:30:00', '01:30:00', '02:30:00', '03:30:00', '04:30:00', '05:30:00', '06:30:00', '07:30:00', '08:30:00', '09:30:00', '10:30:00', '11:30:00', '12:30:00', '13:30:00', '14:30:00', '15:30:00', '16:30:00', '17:30:00', '18:30:00', '19:30:00', '20:30:00', '21:30:00', '22:30:00', '23:30:00']

	end_day = datetime.datetime.strptime(end_day,"%Y-%m-%d") 
	begin_day = datetime.datetime.strptime(begin_day,"%Y-%m-%d")
	delta =  end_day - begin_day
	for i in range(delta.days + 1):
		day_list.append( (begin_day + timedelta(days=i)).strftime("%Y-%m-%d") )
	
	for day in day_list:
		for time in time_list:
			time_interval.append(" ".join([str(day), time]) )
	time_interval = sorted(time_interval)
	return time_interval



# step1
def insert_city_time_series():
	location_dic = json.load( open("./new_gsr_city_lat_lon.json") )
			    
	for f in os.listdir("/home/jf/Documents/EMBERS/GPS_tag/time_series/5min-Venezuela/2013"):
		with open("/home/jf/Documents/EMBERS/GPS_tag/time_series/5min-Venezuela/2013/" + f) as ff:	
			for line in ff:
				try:	
					line = line.strip().replace("\n", "")					
					left = line.rsplit(" ", 1)[0]
					time1, city, state, country = left.strip().replace("\n", "").split(',')
					time1 = twitTimeToDBTime(time1)					
					location = ",".join([city, state, country])
					location = location.decode("utf-8")
				
					if location in location_dic:
					#if country == 'Venezuela':				
						record = {}
						record["count"] = line.rsplit(" ", 1)[1]
						record["created_at"] =  time1
						record["location"] = encode(location, "utf-8")
						record["daynum"] = datetime.datetime.strptime(time1, '%Y-%m-%d %H:%M:%S').weekday()
						location_file = "./Dec2-2013_count/%s" % location		
						with open(location_file, "a") as out:
							out.write(json.dumps(record) + "\n")
				except:
					print sys.exc_info()
					continue

#insert_city_time_series()



# step 2
#{"count": "8", "daynum": 6, "created_at": "2014-04-06 15:30:00"}
def convert_object_city():
	for loc in os.listdir("./Dec2-2013_count"):
		with open("./Dec2-2013_count/" + loc) as ff:
			result = {}
			for line in ff:
				line = line.strip()
				record = json.loads(line)
				created_at = record["created_at"]				
				if created_at not in result:
					result[created_at] = {}
					result[created_at]["count"] = int(record["count"]) + 1
					result[created_at]["daynum"] = record["daynum"]
			location_file = "./Dec2-2013_object/%s" % loc	
			with open(location_file, "w") as out:
				out.write(json.dumps(result))

			
#convert_object_city()




# step 3
def calculate_Venezuela_Zscore_5min():	
	time_list = ["2013-12-02 19:30:00", "2013-12-02 19:35:00", "2013-12-02 19:40:00", "2013-12-02 19:45:00", "2013-12-02 19:50:00", "2013-12-02 19:55:00", "2013-12-02 20:00:00", "2013-12-02 20:05:00", "2013-12-02 20:10:00", "2013-12-02 20:15:00", "2013-12-02 20:20:00", "2013-12-02 20:25:00",  "2013-12-02 20:30:00",  "2013-12-02 20:35:00",  "2013-12-02 20:40:00",  "2013-12-02 20:45:00"]
	
	for filename in os.listdir("./Venezuela_object"):
		try:
			one = json.load(open("./Venezuela_object/" + filename ))
			score = {}

			for key in time_list:							
				timing = datetime.datetime.strptime(key, '%Y-%m-%d %H:%M:%S') # "2014-05-29 02:00:00"
				timing_list = []
				count_list = []
				#daynum = timing.weekday()					

				for i in range(28):
					delta = (timing - timedelta(days=(28-i-1)) )
					#if daynum > 4:
					#	if delta.strftime('%A') in ('Sunday', 'Saturday'):						
					#		timing_list.append( delta.strftime('%Y-%m-%d %H:%M:%S') ) #Code for weekend
					#elif daynum <5:
					if delta.strftime('%A') not in ('Sunday', 'Saturday'):					
						timing_list.append( delta.strftime('%Y-%m-%d %H:%M:%S') ) #code for weekday
		
				
				for t in timing_list:
					if t in one:
						count_list.append(one[t]["count"])
					else:
						count_list.append(1)					
	
				end_fix = 0.0
				ar = np.array(count_list)
				if np.std(ar):		
					end_fix = stats.zscore(ar)[-1] # the last value		
			
				if key not in score:
					score[key] = end_fix

			location_file = "./Venezuela_zscore/%s" % filename	
			with open(location_file, "w") as out:
				out.write(json.dumps(score))	
		except:
			print sys.exc_info()
			continue

#calculate_Venezuela_Zscore_5min()



from pygeocoder import Geocoder
def get_wavelet_score():
	gsr =  json.load(open("/home/jf/Documents/EMBERS/GPS_tag/wavelet/correct_gsr_city_lat_lon.json"))	

	time_list = ["2013-12-02 19:30:00", "2013-12-02 19:35:00", "2013-12-02 19:40:00", "2013-12-02 19:45:00", "2013-12-02 19:50:00", "2013-12-02 19:55:00", "2013-12-02 20:00:00", "2013-12-02 20:05:00", "2013-12-02 20:10:00", "2013-12-02 20:15:00", "2013-12-02 20:20:00", "2013-12-02 20:25:00",  "2013-12-02 20:30:00",  "2013-12-02 20:35:00",  "2013-12-02 20:40:00",  "2013-12-02 20:45:00"]

	for loc in os.listdir("./Venezuela_zscore"):					
		one = json.load(open("./Venezuela_zscore/" + loc ))

		loc1 = encode(loc, "utf-8")
		if loc1 in gsr:
			lat = gsr[loc1]["lat"]
			lon = gsr[loc1]['log']		
	
		for time in time_list:
			if time in one:
				later = time.strip().split()[1]
				score = one[time]				
				record = str(lat) + "\t" + str(lon) + "\t" + str(score)

				location_file = "./Venezuela-map-%s" % later
				with open(location_file, 'a') as out:				
					out.write( record + '\n' )
		
		
					
get_wavelet_score()	



import time

def finish_gsr_lat_lon():
	gsr =  json.load(open("/home/jf/Documents/EMBERS/GPS_tag/wavelet/new_gsr_city_lat_lon.json"))	

	for loc in os.listdir("./Dec2-2013_object"):					
		one = json.load(open("./Dec2-2013_object/" + loc ))
		loc1 = encode(loc, "utf-8")

		if loc1 not in gsr:
			
			gsr[loc1] = {}
			results = Geocoder.geocode(loc)
			lat = results[0].coordinates[0]
			lon =  results[0].coordinates[1]
						
			gsr[loc1]["lat"] = results[0].coordinates[0]
			gsr[loc1]["log"] = results[0].coordinates[1]
			print loc, lat, lon
			time.sleep(1)			
			
	with open("./correct_gsr_city_lat_lon.json", "w") as out:
		out.write(json.dumps(gsr))

#finish_gsr_lat_lon()
