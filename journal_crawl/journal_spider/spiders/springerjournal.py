import scrapy
import re
import os
import sqlite3

import sys
sys.path.append("....") 

from database.database_helper import DatabaseHelper
from database.database_config import DB_FILEPATH
from ..items import JournalInfoItem

class SpringerJournal(scrapy.Spider):

	name = "springerjournal"

	def start_requests(self):

		urls = [
			'https://www.springer.com/gp/computer-science/all-journals-in-computer-science',
		]

		for url in urls:

			yield scrapy.Request(url=url, callback=self.parse)


	def parse(self, response):

		page = response.url.split("/")[-2]

		filename = 'AllJournals-%s.html' % page

		with open(filename, 'wb') as f:
			self.log('Saved file %s' % filename)

		alljournals = response.xpath('//div[contains(@class, "product-information")]')

		counter = 0
		while counter < len(alljournals):
			str_target = alljournals[counter].get()
			str_temp = re.split('com/', str_target)[1]
			journal_code = re.split('"', str_temp)[0]
			url = 'https://www.springer.com/journal/'+journal_code+'/editors'
			str_temp = re.split("</a></h3>", str_target)[0] 
			title = re.split(">", str_temp)[-1]

			DatabaseHelper.addJournal(JournalInfoItem(title=title, publisher="Springer", url=url), DB_FILEPATH)
			
			counter = counter + 1

		os.system("rm "+filename)
