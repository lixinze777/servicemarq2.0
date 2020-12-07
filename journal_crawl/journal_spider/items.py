# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class JournalInfoItem(scrapy.Item):
    title = scrapy.Field()
    publisher = scrapy.Field()
    url = scrapy.Field()

class JournalLineItem(scrapy.Item):
	publisher = scrapy.Field()
	title = scrapy.Field()
	line = scrapy.Field()

class TaggedLineItem(scrapy.Item):
	publisher = scrapy.Field()
	title = scrapy.Field()
	line = scrapy.Field()

class CrawledItem(scrapy.Item):
	publisher = scrapy.Field()
	title = scrapy.Field()
	role = scrapy.Field()
	name = scrapy.Field()
	affiliation = scrapy.Field()