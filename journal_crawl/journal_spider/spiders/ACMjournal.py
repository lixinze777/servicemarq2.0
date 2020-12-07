#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time
import sqlite3
import sys
sys.path.append(sys.path[1].rsplit('/',1)[0])
sys.path.append(sys.path[1].rsplit('/',2)[0])
sys.path.append(sys.path[1].rsplit('/',3)[0]+"/database")

import database_helper
import database_config
import config
import items
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from lxml import etree


class ACMjournal:

	def __init__(self):

		options = Options()
		options.binary_location = '/usr/bin/google-chrome'
		options.add_argument('--headless')
		options.add_argument('--disable-gpu')
		options.add_argument('--no-sandbox')

		self.url = "https://dl.acm.org/journals"
		self.driver = webdriver.Chrome(executable_path=str(config.CHROMEDRIVER_FILEPATH), options=options)

	def load_page(self):

		self.driver.get(self.url)
		time.sleep(3)

		js = "window.scrollBy(0, 1000)"

		# js = "window.scrollTo(0,document.body.scrollHeight)" 
		self.driver.execute_script(js)
		time.sleep(3)
	
		self.driver.execute_script(js)
		time.sleep(3)
		
		# self.driver.save_screenshot("2.jpg")
		self.parse_page(self.driver.page_source)


	def parse_page(self, html):

		text = etree.HTML(html)
		keyword_list = text.xpath('//h4[@class="search__item-title"]//a/@href')		

		url_list = []
		for keyword in keyword_list:
			url = "https://dl.acm.org" + keyword + "/editorial-board"
			url_list.append(url)

		text = etree.HTML(html)
		journal_list = text.xpath('//span[@class="browse-title"]/text()')

		counter = 0
		while counter < len(url_list):
			title = journal_list[counter]
			url = url_list[counter]
			database_helper.DatabaseHelper.addJournal(items.JournalInfoItem(title=title, publisher="ACM", url=url), database_config.DB_FILEPATH)
			counter = counter + 1


	def close(self):
		time.sleep(2)
		self.driver.quit()


if __name__ == "__main__":
	loader = ACMjournal()
	loader.load_page()
	loader.close()
