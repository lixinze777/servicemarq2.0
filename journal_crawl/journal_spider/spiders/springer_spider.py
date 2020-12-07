import scrapy
import re
import sqlite3
import os

import sys
sys.path.append("....") 

from database.database_helper import DatabaseHelper
from database.database_config import DB_FILEPATH
from ..items import JournalLineItem

class SpringerSpider(scrapy.Spider):

	name = "springer"

	def start_requests(self):
		publisher = "Springer"
		urls = DatabaseHelper.getJournalUrls(DB_FILEPATH, publisher)

		for url in urls:
			yield scrapy.Request(url=url[0], callback=self.parse)


	def parse(self, response):

		page = response.url.split("/")[-2]

		filename = 'editorialboards-%s.html' % page

		with open(filename, 'wb') as f:
			f.write(response.body)

		self.log('Saved file %s' % filename)

		#tagger = SequenceTagger.load('ner

		title = response.xpath('//title/text()').get().split("|")[0]

		#yield{'journal': journal,}
		
		board = response.xpath('//div[@id="editorialboard"]').get()
		try:
			lines = re.split('<br>|</p><p>|<p></p>|<p>|</p>', board)

			for line in lines:
				line = line.replace("<"," <").replace(">","> ").replace(","," ,").replace(":"," : ").replace("  "," ")
				DatabaseHelper.addLine(JournalLineItem(publisher="Springer", title=title, line=line), DB_FILEPATH)
		except:
			pass

		os.system("rm "+filename)



