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
#import geocode

					
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
def insert_city_time_series0():
	location_dic = json.load( open("./gsr_city_lat_log.json") )
	for f in os.listdir("/home/jf/Documents/EMBERS/GPS_tag/time_series/April_2014"):
		with open("/home/jf/Documents/EMBERS/GPS_tag/time_series/April_2014/"+f) as ff:
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
						location_file = "./city_time_series_2013/%s" % location		
						with open(location_file, "a") as out:
							out.write(json.dumps(record) + "\n")
				except:
					print sys.exc_info()
					continue


def insert_city_time_series():
	result = {}
	time_list = {"2014-04-01", "2014-04-02", "2014-04-03"}
	for f in os.listdir("/home/jf/Documents/EMBERS/GPS_tag/time_series/April_2014"):
		with open("/home/jf/Documents/EMBERS/GPS_tag/time_series/April_2014/"+f) as ff:
			#print f
			for line in ff:
				try:	
					line = line.strip().replace("\n", "")					
					left = line.rsplit(" ", 1)[0]
					time1, city, state, country = left.strip().replace("\n", "").split(',')
					time1 = twitTimeToDBTime(time1)					
					location = ",".join([city, state, country])
					location = location.decode("utf-8")

					short = time1.strip().split()[0]
					if short in time_list:
						if location == encode('Iquique,Tarapacá,Chile','utf-8'):				
							count = line.rsplit(" ", 1)[1]						
							record = "|".join([time1, count])
							result[time1]=count					
							with open("./Chile_earthquake.json", "a") as out:
								out.write(json.dumps(result) )
				except:
					print sys.exc_info()
					continue

insert_city_time_series()



########################### the following program is to calculate absenteeism one ##############
"test period: Dec 2013, 31 days"
"using Theodoros Lappas method to calculate absenteeism score"
def get_absent_score_one():
	for f_name in os.listdir("/home/jf/Documents/EMBERS/GPS_tag/algorithm/city_object_Dec2013"):
		try:
			with open("/home/jf/Documents/EMBERS/GPS_tag/algorithm/city_object_Dec2013/" + f_name) as ff:
				result = json.load( ff)				
				sum_count = 0				
				for key in result:					
					sum_count = sum_count + int(result[key]["count"])
				for k in result:
					result[k]["absent_one"] = float(result[k]["count"])/sum_count - float(1)/(31*48)
				location_file = "/home/jf/Documents/EMBERS/GPS_tag/algorithm/absent_one_Dec2013/%s" % f_name						
				with open(location_file, "w") as out:
					out.write(json.dumps(result))
		except:
			print sys.exc_info()


def convert_city_file():
	for f_name in os.listdir("/home/jf/Documents/EMBERS/GPS_tag/algorithm/city_time_series_Dec2013"):
		try:
			with open("/home/jf/Documents/EMBERS/GPS_tag/algorithm/city_time_series_Dec2013/" + f_name) as ff:
				result = {}
				for line in ff:
					record = {}
					record = json.loads(line)
					created_at = record["created_at"]					
					if created_at not in result:
						result[created_at]={}
						result[created_at]["count"] = record["count"]
						result[created_at]["daynum"] = record["daynum"]
				location_file = "./city_object_Dec2013/%s" % f_name		
				with open(location_file, "w") as out:
					out.write(json.dumps(result))
		except:
			print sys.exc_info()




########################### the following program is to calculate absent score two:  zscore(14) for absenteeism ##############
# to complement the missing counts, add their time, and set count to be 0
def get_time_interval(begin_day, end_day):
	time_interval = []
	day_list = []
	time_list = ['00:00:00', '00:30:00', '01:00:00', '01:30:00', '02:00:00', '02:30:00', '03:00:00', '03:30:00','04:00:00', '04:30:00', '05:00:00', '05:30:00', '06:00:00', '06:30:00', '07:00:00', '07:30:00','08:00:00', '08:30:00', '09:00:00', '09:30:00', '10:00:00', '10:30:00', '11:00:00', '11:30:00','12:00:00', '12:30:00', '13:00:00', '13:30:00', '14:00:00', '14:30:00', '15:00:00', '15:30:00','16:00:00', '16:30:00', '17:00:00', '17:30:00','18:00:00', '18:30:00', '19:00:00', '19:30:00', '20:00:00', '20:30:00', '21:00:00', '21:30:00','22:00:00', '22:30:00', '23:00:00', '23:30:00']
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



def complement_convert_city(begin_day, end_day):	
	for f_name in os.listdir("/home/jf/Documents/EMBERS/GPS_tag/algorithm/city_time_series_2013"):
		try:
			with open("/home/jf/Documents/EMBERS/GPS_tag/algorithm/city_time_series_2013/" + f_name) as ff:
				time_interval = get_time_interval(begin_day, end_day)
				result = {}
				for line in ff:
					record = {}
					record = json.loads(line)
					created_at = record["created_at"]
					if created_at not in result:
						result[created_at]={}
						result[created_at]["count"] = record["count"]
						result[created_at]["daynum"] = record["daynum"]					

				for t in time_interval:
					if t not in result:
						result[t]={}
						result[t]["daynum"] = datetime.datetime.strptime(t, '%Y-%m-%d %H:%M:%S').weekday()
						result[t]["count"] = 0					
						
				location_file = "./2013_city_object/%s" % f_name		
				with open(location_file, "w") as out:
					out.write(json.dumps(result))
		except:
			print sys.exc_info()


#complement_convert_city("2013-01-01", "2013-11-29")



# I insert the "location, time, count, daynum" records into score.db, pretty fast! nice! 0.01 second
def insert_count_db():
	con = lite.connect("/home/jf/Documents/EMBERS/GPS_tag/algorithm/score.db")
	with con:
		cur=con.cursor()
		for f_name in os.listdir("/home/jf/Documents/EMBERS/GPS_tag/algorithm/2013_city_object"):
			try:
				with open("/home/jf/Documents/EMBERS/GPS_tag/algorithm/2013_city_object/" + f_name) as ff:
					result = json.load(ff)
					location = encode(f_name, "utf-8")
					for t in result:
						created_at = t
						count = result[t]["count"]
						daynum = result[t]["daynum"]
						cityData = [created_at, location, int(count), int(daynum)]
						cur.execute("INSERT INTO t_city(created_at, location, count, daynum) VALUES(?,?,?,?)", cityData)
			except:
				print sys.exc_info()
				continue

#insert_count_db()




# I calculate zscore(14) from score.db, but write the zscore into "certain_time_absent_two.json"
# I am using "created_at" as key
def certain_time_absent_score_two():
	con = lite.connect("/home/jf/Documents/EMBERS/GPS_tag/algorithm/score.db")
	day_list = ["2013-09-10", "2013-09-11"]
	time_list = ['09:00:00', '09:30:00', '10:00:00', '10:30:00', '11:00:00', '11:30:00','12:00:00', '12:30:00', '13:00:00', '13:30:00', '14:00:00', '14:30:00', '15:00:00', '15:30:00','16:00:00', '16:30:00', '17:00:00', '17:30:00','18:00:00', '18:30:00', '19:00:00', '19:30:00', '20:00:00', '20:30:00']

	result = {}

	city_dic = json.load( open("./country_gsr_lat_lon.json") ) # sort by country

	for loc in city_dic["Mexico"]:
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

					if created_at not in result:
						result[created_at] = {}
						result[created_at][loc] = end_fix
					else:
						if loc not in result[created_at]:
							result[created_at][loc] = end_fix
						
				except:
					#print sys.exc_info()			
					continue
	with open("./absent_two_20130910_20130911.json", "w") as out:
		out.write(json.dumps(result))


#print datetime.datetime.now()
#certain_time_absent_score_two()
#print datetime.datetime.now()



if __name__ == "__main__":
	ap = argparse.ArgumentParser("")
	ap.add_argument('--db', metavar='DATABASE', type=str, default="/home/jf/Documents/EMBERS/GPS_tag/algorithm/score.db", help='score.db')
	ap.add_argument('--sd',dest="begin_day",metavar="the start day",type=str, default="2013-12-16", help="%Y-%m-%d")
	ap.add_argument('--ed',dest="end_day",metavar="the end day",type=str, default="2013-12-31", help="%Y-%m-%d")
	ap.add_argument('--n',dest="num",metavar="loop day for zscore",type=str, default="14")
	args = ap.parse_args()
	database = args.db
	begin_day = args.begin_day
	end_day = args.end_day
	num =  args.num
	#get_gsr_city_lat_log(database)
	#pure_dic()
	#insert_city_time_series()
	#convert_city_file()
	#get_absent_score_one()
	#complement_convert_city(begin_day, end_day)
	#insert_count_db(database)
	#get_absent_score_two(database, begin_day, end_day)
	


"""
# too slow! very very slow! we have to select a certain day, to calculate zscore. because the cities are too many!
# I calculate zscore(14) from score.db, but write the zscore into "absent_two_2014_half_year.json"
def get_absent_score_two(database, begin_day, end_day):
	con = lite.connect(database)	
	day_list = []
	time_list = ['00:00:00', '00:30:00', '01:00:00', '01:30:00', '02:00:00', '02:30:00', '03:00:00', '03:30:00','04:00:00', '04:30:00', '05:00:00', '05:30:00', '06:00:00', '06:30:00', '07:00:00', '07:30:00','08:00:00', '08:30:00', '09:00:00', '09:30:00', '10:00:00', '10:30:00', '11:00:00', '11:30:00','12:00:00', '12:30:00', '13:00:00', '13:30:00', '14:00:00', '14:30:00', '15:00:00', '15:30:00','16:00:00', '16:30:00', '17:00:00', '17:30:00','18:00:00', '18:30:00', '19:00:00', '19:30:00', '20:00:00', '20:30:00', '21:00:00', '21:30:00','22:00:00', '22:30:00', '23:00:00', '23:30:00']
	end_day = datetime.datetime.strptime(end_day,"%Y-%m-%d")
	begin_day = datetime.datetime.strptime(begin_day,"%Y-%m-%d")
	delta =  end_day - begin_day
	for i in range(delta.days + 1):
		day_list.append(begin_day + timedelta(days=i))
	
	result = {}
	for loc in os.listdir("/home/jf/Documents/EMBERS/GPS_tag/algorithm/complement_city_object_Dec2013"):
		for cal_day in day_list:
			for fix_time in time_list:			
				try:
					one_month = []
					end_fix = 0
					fix_day = cal_day.strftime('%Y-%m-%d')
					created_at = " ".join([fix_day, fix_time])
		
					if cal_day.weekday() < 5:	
						sql = "select count from t_city where location='{}' and created_at<='{}' and created_at like '%{}' and daynum < 5 order by created_at desc limit '{}' ".format(loc, created_at, fix_time, 20) 
					elif cal_day.weekday() > 4:
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

					if created_at not in result:
						result[created_at] = {}
						result[created_at][loc] = end_fix
					else:
						if loc not in result[created_at]:
							result[created_at][loc] = end_fix
						
				except:
					print sys.exc_info()			
					continue

	with open("./absent_two_2014half_year.json", "w") as out:
		out.write(json.dumps(result))

"""
		


