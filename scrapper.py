"""

@author: Kaustubh
@version: 0.1

All rights reserved.

The redistribution, modification and usage
of this script is not allowed without the
author's written conesnt.

"""



from __future__ import unicode_literals
from multiprocessing import Pool

import re
import requests
import sqlite3
import Queue
import multiprocessing
from bs4 import BeautifulSoup


class scrapper:

	""" Contains all scrapper operations"""

	scrape_url = []
	db_Handler = None
	html_doc = None
	soup = None

	def __init__(self, url, db_Handler):
		self.scrape_url=url
		self.db_Handler = db_Handler
		self.html_doc = requests.get(self.scrape_url)
		self.soup = BeautifulSoup(self.html_doc.content,"lxml")

	def get_movie(self):
		
		""" Fetches the page corresposnding to the URL"""
		
		self.get_info()

	def get_urls(self, url, url_list):
		regex=re.compile('title/tt\d*/?ref_=tt_rec_tti')
		temp_list = []
		temp2_list = []
		print url
		for a in self.soup.find_all('a', href=True):
			temp_list.append(a['href'])
		for i in temp_list:
			if("ref_=tt_rec_tt" in i):
				temp2_list.append(i)
		temp_url_list= self.build_url(temp2_list)
		for x in temp_url_list:
			if (x not in url_list):
				url_list.append(x)
		return url_list

	def get_info(self):

		rating = self.soup.find("div", class_="star-box-giga-star")
		title = self.soup.find("span", class_="itemprop")
		year = self.soup.find("span",class_="nobr")
		rating=rating.text
		year=year.text
		rating = str(rating)
		title=title.text
		movie_info = movie(title, rating, year[1:5])
		self.db_Handler.store_in_db(movie_info)
		print title
		print rating[1:]
		print int(year[1:len(year)-1])

	def build_url(self,urls):
		real_urls = []
		for i in urls:
			real_urls.append("http://www.imdb.com"+i)
		return real_urls
		
class db_store:

	connection= None
	cursor= None
	def __init__(self, db_name):
		self.connection = sqlite3.connect(db_name)
		self.cursor = self.connection.cursor()

	def store_in_db(self,movie):
		self.cursor.execute("INSERT INTO movies(name, rating, year) VALUES(?, ?, ?)",[str(movie.title), float(movie.rating), int(movie.year)])
		self.connection.commit()

class movie:

	title = []
	rating = None;
	year = None;

	def __init__ (self, title, rating, year):
		self.title = title
		self.rating = rating
		self.year = year


def start():
	
	jobs = []
	root_url = "http://www.imdb.com/title/tt0372784/?ref_=tt_rec_tti"
	url_list = []
	url_list.append(root_url)
	db_Handler = db_store('imdb')
	for url in url_list:
		scrape_Handler = scrapper(url, db_Handler)
		t = multiprocessing.Process(target = scrape_Handler.get_urls, args = (url, url_list))
		jobs.append(t)

	for j in jobs:
		j.start()

	for j in jobs:
		j.join()

	print url_list
	
start()
