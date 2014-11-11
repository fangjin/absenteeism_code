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
import matplotlib.pyplot as plt
import numpy as np
from scipy import stats
import json
import math
from sklearn.neighbors import NearestNeighbors

					
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
	
	


def	distance_on_unit_sphere(lat1, long1, lat2, long2):
	# Convert latitude and longitude to spherical coordinates in radians.
	degrees_to_radians = math.pi/180.0

	# phi = 90 - latitude
	phi1 = (90.0 - lat1)*degrees_to_radians
	phi2 = (90.0 - lat2)*degrees_to_radians

	# theta = longitude
	theta1 = long1*degrees_to_radians
	theta2 = long2*degrees_to_radians

	# Compute spherical distance from spherical coordinates.
	# For two locations in spherical coordinates (1, theta, phi) and (1, theta, phi)
	# cosine( arc length ) = sin phi sin phi' cos(theta-theta') + cos phi cos phi'
	# distance = rho * arc length

	cos = (math.sin(phi1)*math.sin(phi2)*math.cos(theta1 - theta2) + 
	   math.cos(phi1)*math.cos(phi2))
	arc = math.acos( cos )

	# Remember to multiply arc by the radius of the earth in your favorite set of units to get length.
	# r= 6380 kM, or 4000 miles
	return arc*6380



def get_nearest_neighbors(K):
	city_dic = json.load( open("./gsr_city_lat_log.json") )

	points = []
	for l in city_dic:
		points.append([ city_dic[l]["lat"], city_dic[l]["log"] ])
	points = np.array(points)
	points = np.around(points, decimals=2)	

	nbrs = NearestNeighbors(n_neighbors=K, algorithm='ball_tree').fit(points)
	distances, indices = nbrs.kneighbors(points) # indices shows the position

	for m in range(len(points)-1):
		for loca in city_dic:			
			if city_dic[loca]["lat"] == points[m][0] and city_dic[loca]["log"] == points[m][1]:
				city_dic[loca]["neighbor"] = []
				for n in indices[m]:									
					city_dic[loca]["neighbor"].append( ['%.2f' % elem for elem in list(points[n])] )

	## write name_list into gsr_city_neighbor_lat_log.json
	for loc in city_dic:
		try:
			city_dic[loc]["name_list"] = []
			for pair in city_dic[loc]["neighbor"]:				
				for c in city_dic:
					if str(city_dic[c]["lat"]) == pair[0] and str(city_dic[c]["log"]) == pair[1]:
						city_dic[loc]["name_list"].append(c)
		except:
			print sys.exc_info()
			continue

	with open("./gsr_city_neighbor_namelist.json","w") as output_file:
		json.dump(city_dic, output_file)





"test time period: one month; time interval: half an hour; suppose the time interval are only 30 minutes firstly, expand later"
# I have one gsr city and their neighborhood file, named: gsr_city_neighbor_namelist.json
# I have each city's absenteeism score file, file path is ./city_object_Dec2013/
def nearest_neighbor_algorithm( begin_day, end_day):
	neighbor_dic = json.load( open("./gsr_city_neighbor_namelist.json") )	

	# construct the time interval candidates
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


	# algorithm
	# get loc's neighborhood set's absent score, sum the score less than threshold, store in absent_score ={"loc1":{"time1":{sum_score:, "neighbor_city": ] }}}
	absent_score = {}
	score_thre = 0

	for t in time_interval:
		for loc in neighbor_dic:
			try:					
				K_socre_list = []

				name_list = neighbor_dic[loc]["name_list"]
				# "name" within name_list is unicode
				for name in name_list: 
					# under folder, "name" is str					
					if name.encode("utf-8") in os.listdir("./absent_one_Dec2013"):  									
						with open("./absent_one_Dec2013/" + name.encode("utf-8")) as ff:
							absent_obj = json.load( ff)	
							if t in absent_obj:
								if absent_obj[t]["absent_one"] < score_thre:
									K_socre_list.append(absent_obj[t]["absent_one"])

				# get one neighborhood set's absent_score: K_socre_list
				if loc not in absent_score:
					absent_score[loc] = {t:{"sum_score":float(sum(K_socre_list)), "neighbor_city":name_list }}
				else:
					if t not in absent_score[loc]:				
						absent_score[loc][t] = {"sum_score":float(sum(K_socre_list)), "neighbor_city":name_list }
			except:
				print sys.exc_info()
				continue


	# get all the group's absent score summary
	with open("./absent_score_Dec_2013.json","w") as output_file:
		json.dump(absent_score, output_file)

	mini_score = 2000
	mini_loc = ""
	mini_interval = ""
	for cc in absent_score:
		for tt in absent_score[cc]:			
			if absent_score[cc][tt]["sum_score"] < mini_score:
				mini_score = absent_score[cc][tt]["sum_score"]
				mini_loc = absent_score[cc][tt]["neighbor_city"]
				mini_interval = tt
	print mini_interval, mini_loc, mini_score
	return mini_interval, mini_loc, mini_score



def minimal_group():
	neighbor_dic = json.load( open("./gsr_city_neighbor_namelist.json") )
	absent_score = json.load( open("./absent_score_Dec_2013.json") )
	mini_score = 2000
	mini_loc = ""
	mini_interval = ""
	for cc in absent_score:
		for tt in absent_score[cc]:			
			if absent_score[cc][tt]["sum_score"] < mini_score:
				mini_score = absent_score[cc][tt]["sum_score"]
				mini_loc = absent_score[cc][tt]["neighbor_city"]
				mini_interval = tt
	mini_lat = []
	for loc in mini_loc:
		if loc in neighbor_dic:
			mini_lat.append( [ neighbor_dic[loc]["lat"], neighbor_dic[loc]["log"] ] )
			
		
	print mini_interval, mini_loc, mini_score, mini_lat
	return mini_interval, mini_loc, mini_score, mini_lat


def top_mini_groups():
	neighbor_dic = json.load( open("./gsr_city_neighbor_namelist.json") )
	absent_score = json.load( open("./absent_score_Dec_2013.json") )
	score_list = []
	for cc in absent_score:
		for tt in absent_score[cc]:
			score_list.append(absent_score[cc][tt]["sum_score"])
	top_score_list = sorted(score_list)[0:20]
	print top_score_list

	top_score_dic = {}
	for c in absent_score:
		try:		
			for t in absent_score[c]:
				if absent_score[c][t]["sum_score"] in top_score_list:
					print c
					top_score_dic[c] = t
					#top_score_dic[c] =  {t: {"sum_score": absent_score[c][t]["sum_score"], "neighbor_city": absent_score[c][t]["neighbor_city"]} }
		except:
			continue
	
	print top_score_dic
	with open("./top_score_dic.json","w") as output_file:
		json.dump(top_score_dic, output_file)


	top_lat = []
	for loc in top_score_dic:
		if loc in neighbor_dic:
			top_lat.append( [ neighbor_dic[loc]["lat"], neighbor_dic[loc]["log"] ] )
	print top_lat
	




def baoli_file():
	absent_dic = json.load( open("./absent_score_oneday.json") )
	city_dic = json.load( open("./new_gsr_city_lat_lon.json") )
	with open("./baoli_file_new", "w") as f:
		for city in absent_dic:
			if "2013-12-30 05:30:00" in absent_dic[city]:
				if city in city_dic:
					c = city.strip().split(",")[0]
					record = "\t".join( [c.encode("utf-8"), str(absent_dic[city]["2013-12-30 05:30:00"]["sum_score"]), str(city_dic[city]["lat"]), str(city_dic[city]["log"])] )
					f.write(record)
					f.write("\n")
		
	
	
if __name__ == "__main__":
	ap = argparse.ArgumentParser("")
	ap.add_argument('--sd',dest="begin_day",metavar="the start day",type=str, default="2013-11-30", help="%Y-%m-%d")
	ap.add_argument('--ed',dest="end_day",metavar="the end day",type=str, default="2013-12-31", help="%Y-%m-%d")
	ap.add_argument('--K',dest="neighbor",metavar="nearest neighbor num",type=int, default=11)
	args = ap.parse_args()
	begin_day = args.begin_day
	end_day = args.end_day
	neighbor =  args.neighbor
	#get_nearest_neighbors(neighbor)
	#nearest_neighbor_algorithm( begin_day, end_day)
	#minimal_group()
	#top_mini_groups()
	baoli_file()








