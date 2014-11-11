#!/usr/bin/env python
# -*- coding:utf-8 -*-
import json
from unidecode import unidecode
import string
import nltk
import datetime
import sqlite3 as lite
import os
import sys
# format: http://dev.datasift.com/docs/getting-started/data/twitter
# only consider the original tweets

def twitTimeToDBTime1(t):
	TIME_FORMAT = "%a, %d %b %Y %H:%M:%S +0000"
	createdAt = datetime.datetime.strptime(t,TIME_FORMAT)
	return createdAt.strftime("%Y-%m-%d %H:%M:%S")

# decode bring it to high lever, like unicode;
# encode bring it to low level, 8-bit
# sqlite db support unicode, so it is better to use decode
def encode(s, coding): 
	try:
		return s.decode(coding)
	except:
		return s


def get_location_str(location):
	"""convert Location List to string with seperator ','."""
	return ",".join([encode(k, "utf-8") for k in location])


def	read_twitter(filepath, database):	
	with open(filepath) as f:
		con = lite.connect(database)
		for line in f:
			try:
				tweet  = json.loads(line) # by default, json.loads will convert the text into unicode format		
				user_id = retweet = city = state = text = ""				
				created_at = latitude = longitude = 0				
				country =  country_code = "-1"
				flag = False

				if tweet.has_key('embersGeoCode'):
					flag = True
					latitude = tweet['embersGeoCode']['latitude']
					longitude = tweet['embersGeoCode']['longitude']					
					city = encode(tweet['embersGeoCode']['city'], "utf-8")
					state = encode(tweet['embersGeoCode']['admin1'], "utf-8")
					country = tweet['embersGeoCode']['country']
					if country == "Brazil" and city == encode(u"São Manuel", "utf-8"):
			    		
						if tweet['twitter'].has_key('created_at'):
							created_at = twitTimeToDBTime1(tweet['twitter']['created_at'])

						if tweet['twitter'].has_key('place'):
							country_code = tweet['twitter']['place']['country_code']

						# original post, no retweet
						if tweet['twitter'].has_key('user'):
							user_id  = tweet['twitter']['user']['id']
							retweet = 'no'
						if tweet['twitter'].has_key('text'):
							text = encode(tweet['twitter']['text'], "utf-8")

						#retweet means the current person
						if tweet['twitter'].has_key('retweet'):	
							retweet = 'yes'
							if tweet['twitter']['retweet'].has_key('text'):
								text = encode(tweet['twitter']['retweet']['text'], "utf-8")
							if tweet['twitter']['retweet'].has_key('created_at'):
								created_at = twitTimeToDBTime1(tweet['twitter']['retweet']['created_at'])

							if tweet['twitter']['retweet'].has_key('user'):
								if tweet['twitter']['retweet']['user'].has_key('id'):
									user_id  = tweet['twitter']['retweet']['user']['id']
						

						GPSData = [created_at, country, country_code, city, state, user_id, text, retweet, float(latitude), float(longitude)]
						with con:			
							cur = con.cursor()
							try:		
								cur.execute("INSERT INTO t_city(created_at, country, country_code, city, state, user_id, text, retweet, latitude, longitude) VALUES(?,?,?,?,?,?,?,?,?,?)", GPSData)
								print GPSData								
							except:
								print sys.exc_info()
								continue		
	      
			except:
				#print "first ", sys.exc_info()
				continue



for _file in os.listdir("/home/jf/Documents/tweets/South_tweets/1-5"):
	try:		
		read_twitter("/home/jf/Documents/tweets/South_tweets/1-5/"+_file,"/home/jf/Documents/EMBERS_project/GPS_tag/Brazil.db")
		print "file is done: ", _file
	except:
		print sys.exc_info()
		print _file, "  file read error"
		continue


#(<class 'sqlite3.ProgrammingError'>, ProgrammingError('You must not use 8-bit bytestrings unless you use a text_factory that can interpret 8-bit bytestrings (like text_factory = str). It is highly recommended that you instead just switch your application to Unicode strings.',), <traceback object at 0x35d68c0>)



