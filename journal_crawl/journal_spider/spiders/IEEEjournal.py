#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time
import sqlite3
import sys
import re
sys.path.append(sys.path[1].rsplit('/',1)[0])
sys.path.append(sys.path[1].rsplit('/',2)[0])
sys.path.append(sys.path[1].rsplit('/',3)[0]+"/database")

import database_helper
import database_config
import config
import items
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from lxml import etree


class IEEEjournal:

	def __init__(self, url):
		options = Options()
		options.binary_location = '/usr/bin/google-chrome'
		options.add_argument('--headless')
		options.add_argument('--disable-gpu')
		options.add_argument('--no-sandbox')

		self.url = url
		print(str(config.CHROMEDRIVER_FILEPATH))
		self.driver = webdriver.Chrome(executable_path=str(config.CHROMEDRIVER_FILEPATH), options=options)


	def load_page(self, url_list):
		self.driver.get(self.url)
		time.sleep(2)

		js = "window.scrollBy(0, 1000)"

		self.driver.execute_script(js)
		time.sleep(2)

		self.driver.execute_script(js)
		time.sleep(2)

		self.driver.execute_script(js)
		time.sleep(2)

		url_list = self.parse_page(self.driver.page_source, url_list)

		return url_list


	def parse_page(self, html, url_list):
		str_temp = re.split('punumber=', html)

		journal_codes = []
		for item in str_temp:
			if item[0].isdigit() == True:
				journal_code = re.split('"', item)[0]
				if journal_code not in journal_codes:
					journal_codes.append(journal_code)

		for code in journal_codes:
			url = "https://ieeexplore.ieee.org/xpl/aboutJournal.jsp?punumber="+code
			url_list.append(url)

		return url_list


	def load_page2(self):
		self.driver.get(self.url)
		self.driver.maximize_window()
		time.sleep(2)

		js = "window.scrollBy(0, 1000)"
		self.driver.execute_script(js)
		time.sleep(1)

		try:
			element = self.driver.find_element_by_id("publicationContacts-header")
			self.driver.execute_script("arguments[0].click();", element)
			return self.parse_page2(self.driver.page_source)
		except: 
			print("editorial board not provided for :"+str(self.url))
			return "NA", "NA"
		

	def parse_page2(self, html):
		text = etree.HTML(html)
		_url = text.xpath("//ul/li/a/@href") 	
		output_url = self.url

		for item in _url:
			if "editor" in item.lower() or "board" in item.lower():
				output_url = item
				externalPage = True
				break

		title_raw = etree.tostring(text.xpath("//title")[0])
		title = re.split('<title>', re.split(' \|', str(title_raw))[0])[1]

		return title, output_url


	def load_page3(self, line_list, title):

		try:
			self.driver.get(self.url)
		except TimeoutException:
			print("Timeout Page: "+self.url)
			return line_list

		time.sleep(2)

		js = "window.scrollBy(0, 1000)"

		self.driver.execute_script(js)
		time.sleep(2)

		self.driver.execute_script(js)
		time.sleep(2)

		self.driver.execute_script(js)
		time.sleep(2)

		return self.parse_page3(self.driver.page_source, line_list, title)


	def parse_page3(self, html, line_list, title):
		lines = re.split('<div|</div|<br>|<br >|<br/>|<br />|</p><p>|<p></p>|<p>|</p>', html)
		print(len(lines))

		for line in lines:
			line_list.append((title, line))

		return line_list


	def close(self):
		time.sleep(2)
		self.driver.quit()


if __name__ == "__main__":
	
	HOMEPAGE_EXTRACT = 	False
	EDITOR_PAGE_EXTRACT = False
	LINE_EXTRACT = True

	url_list = [] # a list of urls of journal home pages
	journal_list = [] # a list of journal editorial boards, in the form of pair of title and url
	line_list = []


	# The below section extracts home pages from IEEE compilation page
	if HOMEPAGE_EXTRACT:		
		loader = IEEEjournal("https://ieeexplore.ieee.org/browse/periodicals/topic?refinements=ContentType:Journals&selectedValue=Topic:Computing%20and%20Processing")
		url_list = loader.load_page(url_list)
		loader.close()

		loader2 = IEEEjournal("https://ieeexplore.ieee.org/browse/periodicals/topic?selectedValue=Topic:Computing%20and%20Processing&refinements=ContentType:Journals&pageNumber=2")
		url_list = loader2.load_page(url_list)
		loader2.close()

		loader3 = IEEEjournal("https://ieeexplore.ieee.org/browse/periodicals/topic?selectedValue=Topic:Computing%20and%20Processing&refinements=ContentType:Journals&pageNumber=3")
		url_list = loader3.load_page(url_list)
		loader3.close()

		loader4 = IEEEjournal("https://ieeexplore.ieee.org/browse/periodicals/topic?selectedValue=Topic:Computing%20and%20Processing&refinements=ContentType:Journals&pageNumber=4")
		url_list = loader4.load_page(url_list)
		loader4.close()

		loader5 = IEEEjournal("https://ieeexplore.ieee.org/browse/periodicals/topic?selectedValue=Topic:Computing%20and%20Processing&refinements=ContentType:Journals&pageNumber=5")
		url_list = loader5.load_page(url_list)
		loader5.close()

		if len(url_list) <= 100:
			print(str(len(url_list))+" pages loaded. Loading incomplete, please run the code again")
			EDITOR_PAGE_EXTRACT = False
		else:
			print(str(len(url_list))+" pages will be parsed")
			EDITOR_PAGE_EXTRACT = True
	
	# the below section extracts editorial board pages from journal home page urls
	if EDITOR_PAGE_EXTRACT:
		for url in url_list:
			loader = IEEEjournal(url)
			journal_list.append(loader.load_page2())
			loader.close()

		print(journal_list)
		print(len(journal_list))
	

		for title, url in journal_list:
			if title != "NA" and journal_list != "NA":
				database_helper.DatabaseHelper.addJournal(items.JournalInfoItem(title=title, publisher="IEEE", url=url), database_config.DB_FILEPATH)

	# the below section extracts line information from journal editorial boards
	if LINE_EXTRACT:
		publisher = "IEEE"
		urlstitles = database_helper.DatabaseHelper.getJournalUrlsTitles(database_config.DB_FILEPATH, publisher)

		for url, title in urlstitles:
			loader = IEEEjournal(url)
			line_list = loader.load_page3(line_list, title)
			loader.close()
	
		for title, line in line_list:
			if re.search('[a-zA-Z]', line) is not None: # contains English character
				line = line.replace("<"," <").replace(">","> ").replace(","," ,").replace(":"," : ").replace("  "," ")
				database_helper.DatabaseHelper.addLine(items.JournalLineItem(publisher="IEEE", title=title, line=line), database_config.DB_FILEPATH)

	'''
	loader = IEEEjournal("https://ieeexplore.ieee.org/xpl/aboutJournal.jsp?punumber=8423754")
	url_list = loader.load_page2()
	loader.close()
	'''
