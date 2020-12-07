import scrapy
import re
import sqlite3
import os

import sys
sys.path.append("....") 

from database.database_helper import DatabaseHelper
from database.database_config import DB_FILEPATH
from ..items import JournalLineItem

class ACMSpider(scrapy.Spider):

	name = "ACM"

	def start_requests(self):
		publisher = "ACM"
		urls = DatabaseHelper.getJournalUrls(DB_FILEPATH, publisher)

		for url in urls:
			yield scrapy.Request(url=url[0], callback=self.parse)


	def parse(self, response):

		page = response.url.split("/")[-2]

		filename = 'editorialboards-%s.html' % page

		with open(filename, 'wb') as f:
			f.write(response.body)

		self.log('Saved file %s' % filename)

		title = response.xpath('//title/text()').get().split("|")[0]

		board = response.xpath('//div[@class="row"]').get()
		
		lines = re.split('div', board)

		for line in lines:
			if re.search('[a-zA-Z]', line) is not None: # contains English character
				line = line.replace("<"," <").replace(">","> ").replace(","," ,").replace(":"," : ").replace("  "," ")
				DatabaseHelper.addLine(JournalLineItem(publisher="ACM", title=title, line=line), DB_FILEPATH)

		os.system("rm "+filename)



