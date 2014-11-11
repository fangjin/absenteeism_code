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




def insert_zscore(database, begin_day, end_day, num):
	day_list = []
	time_list = ['00:00:00', '00:30:00', '01:00:00', '01:30:00', '02:00:00', '02:30:00', '03:00:00', '03:30:00','04:00:00', '04:30:00', '05:00:00', '05:30:00', '06:00:00', '06:30:00', '07:00:00', '07:30:00','08:00:00', '08:30:00', '09:00:00', '09:30:00', '10:00:00', '10:30:00', '11:00:00', '11:30:00','12:00:00', '12:30:00', '13:00:00', '13:30:00', '14:00:00', '14:30:00', '15:00:00', '15:30:00','16:00:00', '16:30:00', '17:00:00', '17:30:00','18:00:00', '18:30:00', '19:00:00', '19:30:00', '20:00:00', '20:30:00', '21:00:00', '21:30:00','22:00:00', '22:30:00', '23:00:00', '23:30:00']
	end_day = datetime.strptime(end_day,"%Y-%m-%d")
	begin_day = datetime.strptime(begin_day,"%Y-%m-%d")
	delta =  end_day - begin_day
	for i in range(delta.days + 1):
		day_list.append(begin_day + timedelta(days=i))
	
	for day in day_list:
		for time in time_list:		
			#get_zscore(database, day, time, num)
			get_zscore_weekday(database, day, time, num)



"translate the unique time to readable date"
def twitTimeToDBTime(t):
	return (datetime.datetime.fromtimestamp(int(t)).strftime('%Y-%m-%d %H:%M:%S'))


def encode(s, coding): 
	try:
		return s.decode(coding)
	except:
		return s


def get_single_city_result(location, database):
	con = lite.connect(database)
	for f in os.listdir("/home/jf/Documents/EMBERS/GPS_tag/time_series/not"):
		with open("/home/jf/Documents/EMBERS/GPS_tag/time_series/not/"+f) as ff, open("output_%s" % location, 'a') as out:
			for line in ff:
				try:
					count = line.rsplit(" ", 1)[1]
					left = line.rsplit(" ", 1)[0]
					time1, city, state, country = left.split(',')
					time1 =  twitTimeToDBTime(time1)
					daynum = datetime.datetime.strptime(time1, '%Y-%m-%d %H:%M:%S').weekday()
					loc_str = ",".join([city, state, country])
					if country == "Brazil" and city == "São Paulo" :			
						cityData = [time1, encode(city, "utf-8"), encode(state, "utf-8"), country, int(count), daynum]
						with con:			
							cur=con.cursor()
							cur.execute("INSERT INTO t_city(created_at, city, state, country, count, daynum) VALUES(?,?,?,?,?,?)", cityData)

					#if ratio(loc_str, location) >= 0.9:
					#	out.write(" ".join([time1, loc_str, count]) )
				except:
					print sys.exc_info()
					continue


	

def get_multiple_city_result(database):
	city_list = ["Rio de Janeiro", "Salvador", "Brasília", "Fortaleza", "Belo Horizonte", "Manaus", "Curitiba", "Recife", "Porto Alegre", "Belém", "Goiânia", "Guarulhos","Campinas","São Gonçalo","São Luís","Maceió","Duque de Caxias","Nova Iguaçu","Natal","Teresina","Campo Grande","São Bernardo do Campo","Osasco","João Pessoa","Jaboatão dos Guararapes","Santo André", "Uberlândia","Contagem","São José dos Campos"]
	con = lite.connect(database)
	for f in os.listdir("/home/jf/Documents/EMBERS/GPS_tag/time_series/test"):
		with open("/home/jf/Documents/EMBERS/GPS_tag/time_series/test/"+f) as ff:
			for line in ff:
				try:
					count = line.rsplit(" ", 1)[1]
					left = line.rsplit(" ", 1)[0]
					time1, city, state, country = left.split(',')
					time1 =  twitTimeToDBTime(time1)
					daynum = datetime.datetime.strptime(time1, '%Y-%m-%d %H:%M:%S').weekday()
					loc_str = ",".join([city, state, country])
					if country == "Brazil" and city in city_list:			
						cityData = [time1, encode(city, "utf-8"), encode(state, "utf-8"), country, int(count), daynum]
						with con:			
							cur=con.cursor()
							cur.execute("INSERT INTO t_city(created_at, city, state, country, count, daynum) VALUES(?,?,?,?,?,?)", cityData)

				except:
					print sys.exc_info()
					continue

	
if __name__ == "__main__":
	ap = argparse.ArgumentParser("")
	#ap.add_argument('--location', metavar='LOCATION', type=str, default="São Paulo,São Paulo,Brazil", help='date (city,state,country)')
	ap.add_argument('--db', metavar='DATABASE', type=str, default="/home/jf/Documents/EMBERS/GPS_tag/mcity.db", help='city.db')	
	ap.add_argument('--sd',dest="start_day",metavar="the start day",type=str, default="2012-08-01", help="%Y-%m-%d")
	ap.add_argument('--ed',dest="end_day",metavar="the end day",type=str, default="2014-01-31", help="%Y-%m-%d")
	ap.add_argument('--n',dest="num",metavar="loop day for zscore",type=str, default="28")
	args = ap.parse_args()
	#location = args.location
	database = args.db
	start_day = args.start_day
	end_day = args.end_day
	num =  args.num
	get_multiple_city_result(database)
	#insert_zscore(database, start_day, end_day, num)
	#update_daynum(database)






def update_daynum(database):
	con = lite.connect(database)
	created_list = []
	try:
		sql1 = "select created_at from t_city"
		with con:
			cur = con.cursor()
			cur.execute(sql1)
			rows = cur.fetchall()
			for row in rows:
				created_list.append(row[0])

		for created_at in created_list:
			
			sql =  "update t_city set daynum='{}' where created_at='{}' ".format(datetime.datetime.strptime(created_at, '%Y-%m-%d %H:%M:%S').weekday(), created_at)
			with con:
				cur = con.cursor()
				cur.execute(sql)
	except:
		print sys.exc_info()


def get_location_str(location):
	"""convert Location List to string with seperator ','."""
	return ",".join([encode(k, "utf-8") for k in location])


import matplotlib as mpl
def plot(database):
	import datetime as DT
	#fig = pylab.figure()
	con = lite.connect(database)
	maxday = 28
	avg_count = [191, 139, 102, 76, 65, 71, 120, 146, 176, 196, 233, 275, 327, 373, 440, 483, 560, 577, 618, 635, 683, 661, 663, 631, 625, 614, 601, 627, 668, 673, 691, 715, 758, 756, 776, 821, 876, 878,937,959, 995,963,867, 729,601, 459, 340, 249]	
	count = []
	dates = []
	total_avg = []
	for i in range(maxday):
		for j in range(len(avg_count)):
			total_avg.append(avg_count[j])

	sql = "select created_at, count from t_city where created_at < '2013-11-04 00:00:01' and created_at >= '2013-11-01 00:00:00' order by created_at"
	with con:
		cur = con.cursor()
		cur.execute(sql)
		rows = cur.fetchall()
		for row in rows:
			dates.append(row[0])
			count.append(row[1])

	x = [DT.datetime.strptime(d,'%Y-%m-%d %H:%M:%S') for d in dates]
	#dates1 = mpl.dates.drange(min(x), max(x), DT.timedelta(days=1))

	fig = plt.figure()
	ax = plt.gca()

	graph = fig.add_subplot(111)
	graph.plot_date(x ,count, 'b-o')

	plt.xticks(x)
	plt.xticks(rotation=90)
	
	from matplotlib.dates import DayLocator
	days = DayLocator()
	ax.xaxis.set_major_locator(days)
	ax.autoscale_view()
	

	graph.set_xticks(x)
	fig.autofmt_xdate()

	#plt.plot(count,'b-o', label='Tweets number', markersize=2)
	#plt.plot(total_avg, 'r-^', label='Average tweets number', markersize=0.5)
	#plt.xlabel('Date (2013-11-01 to 2013-11-30, in every 30 minutes)', fontsize=10)
	#plt.ylabel('Tweets number / Frequency', fontsize=10)
	#plt.legend( loc='upper right', numpoints = 1 )
	#fig.suptitle('Sao Paulo of Brazil Tweets number on Nov, 2013', fontsize=14)
	plt.grid(True)
	plt.show()
	#fig.savefig('tweets_Brazil.pdf')

