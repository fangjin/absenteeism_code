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

# get the hour time series, need to sum all the days count, then insert into db?





########################### the following program is to calculate absent score two:  zscore(14) for absenteeism ##############
# to complement the missing counts, add their time, and set count to be 0
# first, sum all the days' counts, then complement the missing days.

def get_time_interval(begin_day, end_day):
	time_interval = []
	day_list = []
	time_list = ['00:00:00', '01:00:00', '02:00:00', '03:00:00', '04:00:00', '05:00:00', '06:00:00', '07:00:00', '08:00:00', '09:00:00', '10:00:00', '11:00:00', '12:00:00', '13:00:00', '14:00:00', '15:00:00', '16:00:00', '17:00:00', '18:00:00', '19:00:00', '20:00:00', '21:00:00', '22:00:00', '23:00:00']
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



def complement_sum_daily_city(begin_day, end_day):

	end_day = datetime.datetime.strptime(end_day,"%Y-%m-%d") 
	begin_day = datetime.datetime.strptime(begin_day,"%Y-%m-%d")
	delta =  end_day - begin_day
	day_list = []
	for i in range(delta.days + 1):
		day_list.append( (begin_day + timedelta(days=i)).strftime("%Y-%m-%d") )
	
	for f_name in os.listdir("/home/jf/Documents/EMBERS/GPS_tag/algorithm/daily_city_time_series_45"):
		try:
			with open("/home/jf/Documents/EMBERS/GPS_tag/algorithm/daily_city_time_series_45/" + f_name) as ff:
				result = {}
				for day in day_list:
					result[day] = {}
					result[day]["daynum"] = datetime.datetime.strptime(day, '%Y-%m-%d').weekday()
					result[day]["count"] = 0
				# finish initialize result of each location

				for line in ff:
					record = {}
					record = json.loads(line)
					created_at = record["created_at"].split(" ")[0]
					
					if created_at in result:
						#print "before ", result[created_at]["count"]
						#print "count ", record["count"]
						result[created_at]["count"] = int(result[created_at]["count"]) + int(record["count"])
						#print "after ", result[created_at]["count"]		
						
				location_file = "./daily_city_object_45/%s" % f_name		
				with open(location_file, "w") as out:
					out.write(json.dumps(result))
		except:
			print sys.exc_info()


#complement_sum_daily_city("2014-04-01", "2014-05-31")





def sum_halfhour_daily_city(begin_day, end_day):

	end_day = datetime.datetime.strptime(end_day,"%Y-%m-%d") 
	begin_day = datetime.datetime.strptime(begin_day,"%Y-%m-%d")
	delta =  end_day - begin_day
	day_list = []
	for i in range(delta.days + 1):
		day_list.append( (begin_day + timedelta(days=i)).strftime("%Y-%m-%d") )
	
	for f_name in os.listdir("/home/jf/Documents/EMBERS/GPS_tag/algorithm/2014_123_city_object"):
		try:
			raw = json.load(open("/home/jf/Documents/EMBERS/GPS_tag/algorithm/2014_123_city_object/" + f_name) )			
			#location = encode(f_name, "utf-8")

			result = {}
			for day in day_list:
				result[day] = {}
				result[day]["daynum"] = datetime.datetime.strptime(day, '%Y-%m-%d').weekday()
				result[day]["count"] = 0			

			for key in raw:				
				created_at = key.split(" ")[0]				
				if created_at in result:					
					result[created_at]["count"] = int(result[created_at]["count"]) + int(raw[key]["count"])		
					
			location_file = "./daily_2014_123/%s" % f_name		
			with open(location_file, "w") as out:
				out.write(json.dumps(result))
		except:
			print sys.exc_info()

#sum_halfhour_daily_city("2014-01-01", "2014-03-31")




# I insert the "location, time, count, daynum" records into score.db, pretty fast! nice! 0.01 second
def insert_count_db():
	con = lite.connect("/home/jf/Documents/EMBERS/GPS_tag/algorithm/daily_score.db")
	with con:
		cur=con.cursor()
		for f_name in os.listdir("/home/jf/Documents/EMBERS/GPS_tag/algorithm/daily_2014_123"):
			try:
				with open("/home/jf/Documents/EMBERS/GPS_tag/algorithm/daily_2014_123/" + f_name) as ff:
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
def certain_time_absent_score_two( begin_day, end_day ):
	con = lite.connect("/home/jf/Documents/EMBERS/GPS_tag/algorithm/daily_score.db")

	end_day = datetime.datetime.strptime(end_day,"%Y-%m-%d") 
	begin_day = datetime.datetime.strptime(begin_day,"%Y-%m-%d")
	delta = end_day - begin_day
	day_list = []
	for i in range(delta.days + 1):
		day_list.append( (begin_day + timedelta(days=i)).strftime("%Y-%m-%d") )

	result = {}

	for cal_day in day_list:		
		for loc in os.listdir("/home/jf/Documents/EMBERS/GPS_tag/algorithm/daily_2014_123"):
			#loc = loc.decode("utf-8") 
						
			try:
				one_month = []
				end_fix = 0
				fix_day = datetime.datetime.strptime(cal_day, '%Y-%m-%d')				
	
				if fix_day.weekday() < 5:	
					sql = "select count from t_city where location='{}' and created_at<='{}' and daynum < 5 order by created_at desc limit '{}' ".format(loc, cal_day, 20) 
				elif fix_day.weekday() > 4:
					sql = "select count from t_city where location='{}' and created_at<='{}' and daynum >4 order by created_at desc limit '{}' ".format(loc, cal_day, 8)

				with con:
					cur = con.cursor()
					cur.execute(sql)
					rows = cur.fetchall()

					for row in rows:
						one_month.append(row[0])
				ar = np.array(one_month)

				if np.std(ar):		
					end_fix = stats.zscore(ar)[0] # the zscore of the end_day at fix_time					

				if cal_day not in result:
					result[cal_day] = {}
					result[cal_day][loc] = end_fix
				else:
					if loc not in result[cal_day]:
						result[cal_day][loc] = end_fix
					
			except:
				print sys.exc_info()			
				continue
	with open("./absent_two_20140101_20140531_daily.json", "w") as out:
		out.write(json.dumps(result))


#certain_time_absent_score_two("2014-01-01", "2014-05-31")


def insert_zscore_db():
	con = lite.connect("/home/jf/Documents/EMBERS/GPS_tag/algorithm/daily_zscore.db")
	with con:
		cur = con.cursor()		
		
		raw = json.load( open("./absent_two_20131201_20140131_daily.json") )
		for t in raw:
			try:
				for loc in raw[t]:					
					loc = encode(loc, "utf-8")								
					zscore14 = raw[t][loc]					
			
					sql = "update t_city set zscore14 = '{}' where created_at = '{}' and location = '{}' ".format(zscore14, t, loc.encode('utf-8')) 
					cur.execute(sql)
			except:
				print sys.exc_info()
				continue
			

#insert_zscore_db()





def	get_minimal_score():
	score_dic = json.load( open("./absent_two_20140401_20140531_daily.json") )
	
	specific = "2014-05-01"
	for loc in score_dic[specific]:
		if score_dic[specific][loc]<-2:
			print specific, loc, score_dic[specific][loc]

	for day in score_dic:
		for loc in score_dic[day]:
			if score_dic[day][loc] < -2.7:
				print day, loc, score_dic[day][loc]

#get_minimal_score()



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



		
def weekday_count():
	con = lite.connect("/home/jf/Documents/EMBERS/GPS_tag/algorithm/daily_zscore.db")

	holiday={"Brazil":["2013-01-01", "2013-02-11", "2013-02-12", "2013-03-29", "2013-04-21", "2013-05-01", "2013-05-30", "2013-09-07", "2013-10-12","2013-11-01","2013-11-02","2013-11-15","2013-11-20", "2013-12-25", "2014-01-01"] }

	mydic = {}
	loc_file = json.load(open("/home/jf/Documents/EMBERS/GPS_tag/algorithm/back/absent_score_oneday.json"))
	for loc in loc_file:
		mydic[loc] = {"0":[], "1":[], "2":[], "3":[], "4":[], "5":[], "6":[], "holiday":[] }
	
	sql = "select created_at, location, count, daynum from t_city where created_at>='2013-01-07' and created_at<='2014-01-05' and location like '%Brazil' "
	with con:
		cur=con.cursor()		
		cur.execute(sql)
		rows = cur.fetchall()
		for row in rows:
			created_at = row[0]			
			city = row[1]			
			count = row[2]
			daynum = str(row[3])

			try:					
				if encode(city, "utf-8") in mydic:					
					if created_at not in holiday["Brazil"]:
						mydic[encode(city, "utf-8")][daynum].append(int(count))
					else:
						mydic[encode(city, "utf-8")]["holiday"].append(int(count))
	
			except:
					#print sys.exc_info()
					continue

	with open("./Brazil_weekday_count.json","w") as output_file:
		dump = json.dumps(mydic)
		output_file.write("%s\n" % dump)

#weekday_count()

def plot_weekday_holiday():
	loc_file = json.load(open("./Brazil_weekday_count.json"))

	mydic = {}
	for loc in loc_file:
		mydic[loc] =  {"0": 0, "1":0, "2":0, "3":0, "4":0, "5":0, "6":0, "holiday":0 }
		
		for n in loc_file[loc]:
			if sum(loc_file[loc][n])>0:				
				mydic[loc][n] = sum(loc_file[loc][n])/len(loc_file[loc][n])
	#print mydic

	mydic1 = {}

	for loc in mydic:
		flag = 1
		for n in mydic[loc]:			
			if mydic[loc][n]<100:
				flag = 0
				break
		if flag == 1:
			mydic1[loc] = mydic[loc]

	import matplotlib.pyplot as plt
	mylist = []

	with open("./final_holiday_result_100.txt", "w") as f:
		for loc in mydic1:
					
			string = "\t".join([ str(mydic1[loc]["0"]), str(mydic1[loc]["1"]), str(mydic1[loc]["2"]), str(mydic1[loc]["3"]), str(mydic1[loc]["4"]), str(mydic1[loc]["5"]), str(mydic1[loc]["6"]), str(mydic1[loc]["holiday"]) ])
			#mylist.append( mydic1[loc]["0"], mydic1[loc]["1"], mydic1[loc]["2"], mydic1[loc]["3"], mydic1[loc]["4"], mydic1[loc]["5"], mydic1[loc]["6"], mydic1[loc]["holiday"])
			
			f.write(string + "\n")
	
			

	
def compare_holiday_zscore():
	con = lite.connect("/home/jf/Documents/EMBERS/GPS_tag/algorithm/daily_zscore.db")

	holiday={"Brazil":["2013-01-01", "2013-02-11", "2013-02-12", "2013-03-29", "2013-04-21", "2013-05-01", "2013-05-30", "2013-09-07", "2013-10-12","2013-11-01","2013-11-02","2013-11-15","2013-11-20", "2013-12-25", "2014-01-01"] }

	mydic = {}
	h = 0
	w = 0
	
	sql = "select created_at, daynum from t_city where created_at>='2013-01-07' and created_at<='2014-01-05' and location='Vitória,Espírito Santo,Brazil' and zscore14<-2 "
	with con:
		cur=con.cursor()		
		cur.execute(sql)
		rows = cur.fetchall()
		for row in rows:
			created_at = row[0]			
			city = row[1]			
			
			try:									
				if created_at in holiday["Brazil"]:
					h = h+1
					print created_at, " holiday"
				else:
					w = w+1
					print created_at, " not holiday"
	
			except:
					#print sys.exc_info()
					continue

	
#compare_holiday_zscore()

def convex_hull_lat_score():
	id_score = json.load(open("./all_id_score_two.json")) # "2013-09-11": {"2": 0.4125347305153955, "3":
	loc_lat = json.load(open("./gsr_city_lat_log.json")) # {"Hualahuises,Nuevo Le\u00f3n,Mexico": {"lat": 24.88, "log": -99.67},
	id_loc = json.load(open("./location_id.json")) # {"Ilha Soltera,S\u00e3o Paulo,Brazil": 2, 
	
	mm = ""	
	#time_list = ["2013-05-22", "2013-03-18","2013-06-19","2013-10-17", "2013-05-29","2014-02-12"] # colombia floods
	time_list = ["2013-10-12", "2013-05-07", "2014-05-22", "2014-02-20", "2014-02-23"] # Venezuela 
	for t in time_list:
		for id1 in id_score[t]:
			for loc in id_loc:
			
				loc = encode(loc, "utf-8")
				if id1 == str(id_loc[loc]):				
					if loc in loc_lat:									
						try:
							lat = loc_lat[loc]["lat"]
							lon = loc_lat[loc]["log"]
							if lat:
								score = id_score[t][id1]
								mm = "\t".join( [id1, str(lat), str(lon), str(score)] )
								with open("./"+ t +"_id_lat_score.txt", "a") as out:
									out.write(mm + "\n")
						except:
							continue
	



convex_hull_lat_score()






































	
		
	
