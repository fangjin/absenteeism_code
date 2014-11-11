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
import networkx as nx

# nomexico_gsr_City_lat_log.json, very clean file, no duplicate; 871 cities
def remove_mexico():
	city_dic = json.load( open("./gsr_city_lat_log.json") )	

	for key in city_dic.keys():
		if city_dic[key]["lat"]>=12:
			print city_dic[key]["lat"], city_dic[key]["log"]
			del city_dic[key]

	with open("./nomexico_gsr_City_lat_log.json", "w") as out:
		json.dump(city_dic, out)



def transfer_location_to_id():
	city_dic = json.load( open("./gsr_city_lat_log.json") )
	city_list = [key for key in city_dic]
	result = {}
	for i in range(len(city_list)):
		result[city_list[i]] =  i+1

	with open("./location_id1.json","w") as output_file:
		json.dump(result, output_file)



def cluster_file_2D():
	location_dic = json.load( open("./location_id1.json") )
	duplitcate = [100, 139, 294, 637, 741, 1110, 1256, 661, 1099]
	with open("./graph_file_int", "r") as f, open("cluster_file_2D_1", "w") as w:
		for line in f:
			line = line.strip()
			loc1 = line.split("*")[0]
			loc2 = line.split("*")[1]
			id1 = location_dic[loc1.decode("utf-8")]
			id2 = location_dic[loc2.decode("utf-8")]
			distance = line.split("*")[2]
			if id1 not in duplitcate and id2 not in duplitcate:				
				record = " ".join([str(id1), str(id2), str(distance)])
				w.write(record)
				w.write("\n")


def duplicate():
	with open("./cluster_file_2D_1", "r") as f:
		for line in f:
			line = line.strip()
			count = loc2 = line.split(" ")[2]
			if count == "0":
				print line

			
def new_dictionary():
	duplitcate = [100, 139, 294, 637, 741, 1110, 1256, 661, 1099 ]
	nomexico = json.load( open("./nomexico_gsr_City_lat_log.json") )	
	location_dic = json.load( open("./location_id.json") )

	for c in location_dic.keys():
		if location_dic[key] in duplitcate:		
			print c, location_dic[c]
			del location_dic[c]

	with open("./location_id_new.json","w") as output_file:
		json.dump(location_dic, output_file)

	




def new_gsr_city_lat_lon():
	dup_dic = {"Ensenada,Buenos Aires,Argentina": 1099, "Santa Cruz Tlaxcala,Tlaxcala,Mexico": 100, "Alto Lucero,Veracruz,Mexico": 139, "Córdoba,Veracruz,Mexico": 294, "Huixquilucan de Degollado,México,Mexico": 637, "San Juan,San Juan,Argentina": 661, "Tlaltetela,Veracruz,Mexico": 741, "Villas de Irapuato,Guanajuato,Mexico": 1110, "Ixmiquilpan,Hidalgo,Mexico": 1256, "Ensenada,Buenos Aires,Argentina": 1099}
	city_dic = json.load( open("./gsr_city_lat_log.json") )
		
	for c in city_dic.keys():
		if c in dup_dic:
			print c, city_dic[c]
			del city_dic[c]

	with open("./new_gsr_city_lat_lon.json","w") as output_file:
		json.dump(city_dic, output_file)




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


	
def three_D_distance(lat1, lon1, lat2, lon2, score1, score2):
	lat1 = float(lat1)/180
	lat2 = float(lat2)/180
	lon1 = float(lon1)/180
	lon2 = float(lon2)/180
	summary = math.pow(lat1-lat2, 2) + math.pow(lon1-lon2, 2) + math.pow(score1-score2, 2)
	return math.sqrt(summary)

	

def cluster_file_3D():
	duplitcate = [100, 139, 294, 637, 741, 1110, 1256, 661, 1099 ]

	city_dic = json.load( open("./gsr_city_lat_log.json") )
	city_list = [key for key in city_dic]	
	id_dic = json.load( open("./location_id1.json") )
	t = "2013-12-30 05:30:00"

	with open("./cluster_file_3D_1", "w") as g:
		for i in range( len(city_list) ):
			try:
				for j in range(i+1, len(city_list)):
					score1 = 0
					score2 = 0
					id1 = id_dic[city_list[i]]
					id2 = id_dic[city_list[j]]
					if id1 not in duplitcate and id2 not in duplitcate:
						lat1 = city_dic[city_list[i]]["lat"]
						lon1 = city_dic[city_list[i]]["log"]
						lat2 = city_dic[city_list[j]]["lat"]
						lon2 = city_dic[city_list[j]]["log"]

						if city_list[i].encode("utf-8") in os.listdir("./absent_one_Dec2013"): 
							with open("./absent_one_Dec2013/" + city_list[i].encode("utf-8") ) as f1:
								score1_obj = json.load(f1)
								if t in score1_obj:
									score1 = score1_obj[t]["absent_one"]

						if city_list[j].encode("utf-8") in os.listdir("./absent_one_Dec2013"): 
							with open("./absent_one_Dec2013/" + city_list[j].encode("utf-8") ) as f2:
								score2_obj = json.load(f2)
								if t in score2_obj:
									score2 = score2_obj[t]["absent_one"]

						distance = three_D_distance(lat1, lon1, lat2, lon2, score1, score2)
						record = " ".join([str(id1), str(id2), str(distance)])
						g.write(record)
						g.write("\n")
			except:
				print sys.exc_info()
				continue		

	
	
	
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
	return int(arc*6380)


"return a file with all the nodes distance"
def	get_graph_distance():
	#city_dic = json.load( open("./nomexico_gsr_City_lat_log.json") )
	city_dic = json.load( open("./gsr_city_lat_log.json") )
	location_id = json.load(open("./location_id.json") )
	city_list = [key for key in city_dic]

	weight = 0
	with open("./graph/mexico_graph_id_distance", "w") as g:
		for i in range( len(city_list) ):
			for j in range( i+1, len(city_list) ):			
				weight = distance_on_unit_sphere( city_dic[city_list[i]]["lat"], city_dic[city_list[i]]["log"], city_dic[city_list[j]]["lat"], city_dic[city_list[j]]["log"] )
				id1 = location_id[city_list[i]]
				id2 = location_id[city_list[j]]
				if weight == 0:
					print id1, "*****",  id2
				record = " ".join( [str(id1), str(id2), str(weight)] )
				g.write( record )
				g.write("\n")


#get_graph_distance()


def encode(s, coding): 
	try:
		return s.decode(coding)
	except:
		return s

	
# format: {"513": -5.820492431568959e-06, "1026": -0.0005327866397737159,
def id_score_one():
	city_dic = json.load( open("./nomexico_gsr_City_lat_log.json") )
	location_id = json.load( open("./location_id.json") )
	score = {}

	for city in city_dic:
		
		if city.encode("utf-8") in os.listdir("./absent_one_Dec2013"):  									
			with open("./absent_one_Dec2013/" + city.encode("utf-8")) as ff:
				absent_obj = json.load( ff)	
				if "2013-12-30 05:30:00" in absent_obj:
					score[ location_id[city] ] = absent_obj["2013-12-30 05:30:00"]["absent_one"]
	with open("./id_score_one.json","w") as output_file:
		json.dump(score, output_file)
	



# graph algorithm for Mexico
# format: {time: {id: score}, ...}
def id_score_two():	
	location_id = json.load( open("./location_id.json") )             # {"Ilha Soltera,S\u00e3o Paulo,Brazil": 2, 
	score_two = json.load( open("./event_absent_two/absent_two_Brazil_20140501:1800.json") )      # {"2014-06-28 18:00:00": {"San Juan,San Juan,Argentina": 0.15754527228867524, ...}, ""}
	time_list = [k for k in score_two]

	id1 = ""
	result = {}

	for t in time_list:
		if t not in result:
			result[t] = {}
		for city in score_two[t]:
			id1 = location_id[city]	
			result[t][id1] = score_two[t][city]

	with open("./Brazil_id_score_two_20140501.json","w") as output_file:
		json.dump(result, output_file)


id_score_two()		
# 2013-09-10 15:00:00
# 2013-09-11 11:00:00
# 2013-09-10 11:00:00
# 2013-09-11 15:00:00
	

def	get_graph(t):
	G = nx.DiGraph()
	node_score = json.load( open("./Mexico_id_score_two_20130911.json") )
	
	for cid in node_score[t]:								
		G.add_node(cid, score = node_score[t][cid] )
	nodes = [h for h in G.node]

	with open("./graph/mexico_graph_id_distance", "r") as rg:
		for line in rg:
			tmp = line.strip().split(" ",2)
			if tmp[0] in nodes and tmp[1] in nodes:				
				G.add_edge( tmp[0], tmp[1], weight=int(tmp[2]) )
				G.add_edge( tmp[1], tmp[0], weight=int(tmp[2]) )
			
	return G

#get_graph("2013-09-10 15:00:00")



# get the most absenteeism group
def minimal_weight_group(t):
	G = get_graph(t)
	group = []	
	temple = [h for h in G.node] # outside of group
	score = 0
	minimal = 1000
	mininode = ""
	T = 0
	k = 20 # choose 10 group numbers
	for m in G.node:
		if G.node[m]['score'] < minimal:
			minimal = G.node[m]['score']
			mininode = m
			score = score + minimal
	group.append(mininode)
	temple.remove(mininode)
	penalty = 1

	for i in range(k-1):
		score_distance = {}
		for k in temple: # choose the group number which has min(score/distance)
			
			#score_distance[k] = G.node[k]['score']/G[k][mininode]['weight']
			k_distance = {}

			for g in group: # group will be updated				
				k_distance[g] = int(G[k][mininode]['weight'])

			index = max(k_distance, key = k_distance.get )  # get dict key by max value
			penalty = float(k_distance[index]) # get the max diameter? 				
			score_distance[k] = float(G.node[k]['score'])/penalty

		next = min(score_distance, key= score_distance.get) # get dict key by min value
		if score_distance[next] < T:			
			group.append(next)
			score = score +  G.node[next]['score']
			temple.remove(next)
		else:
			break
	print "group ", group 
	return group
	
#minimal_weight_group("2014-05-01 13:00:00")



def top_group():
	#G = get_graph()	
	#group = minimal_weight_group(G)
	#group = ['873', '1014', '999', '176', '688', '403', '775', '133', '202', '1108', '656', '466', '535', '926', '958', '78', '750', '286', '235']
	group = [417, 1232, 166, 185, 1172, 1081, 242, 812, 1292, 362]

	temple = [h for h in G.node]
	rest = [l for l in temple if l not in group ]

	for g in group:
		G.remove_node(g)
		for r in rest:
			if (g,r) in G.edges():
				G.remove_edge(g, r)
				G.remove_edge(r, g)

	print minimal_weight_group(G)
	return minimal_weight_group(G)
#top_group()



def	get_final_lat_lon():
	group = minimal_weight_group("2013-09-11 15:00:00")
	#group = max_weight_group("2014-06-28 17:00:00")
	#group = [516, 129, 1128, 717, 459, 842, 209, 849, 391, 1176]
	#group2 =  [656, 466, 535, 926, 958, 78, 750, 286, 235]
	#group3 = [40, 1004, 900, 350, 1224, 113, 257, 374, 54, 823]
	#group = [417, 1232, 166, 185, 1172, 1081, 242, 812, 1292, 362]
	
	location_id = json.load(open("./location_id.json"))
	city_lat_lon = json.load( open("./gsr_city_lat_log.json") )
	result = {}

	for city in location_id:
		if str(location_id[city]) in group:
			result[city] =  city_lat_lon[city]
			print city
			print city_lat_lon[city]["lat"],",", city_lat_lon[city]["log"]
	print result
		

get_final_lat_lon()




############################# nearest neighbor method ##############################

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
	#get_graph_weight()
	#transfer_location_to_id()
	#cluster_file_2D()
	#cluster_file_3D()
	#new_dictionary()
	#duplicate()



# get the most active group
def max_weight_group(t):
	G = get_graph("2013-09-11 15:00:00")
	group = []	
	temple = [h for h in G.node] # outside of group
	score = 0
	minimal = -10
	mininode = ""
	T = 0
	k = 10
	for m in G.node:
		if G.node[m]['score'] > minimal:
			minimal = G.node[m]['score']
			mininode = m
			score = score + minimal
	print mininode
	group.append(mininode)
	temple.remove(mininode)
	penalty = 1

	for i in range(k-1):
		score_distance = {}
		for k in temple:			
			#score_distance[k] = G.node[k]['score']/G[k][mininode]['weight']
			k_distance = {}
			for g in group:				
				k_distance[g] = int(G[k][g]['weight'])				
			index = max(k_distance, key = k_distance.get )
			penalty = float(k_distance[index])				
			score_distance[k] = float(G.node[k]['score'])/penalty

		next = min(score_distance, key= score_distance.get)
	
		group.append(next)
		score = score +  G.node[next]['score']
		temple.remove(next)
	
	print group
	return group





