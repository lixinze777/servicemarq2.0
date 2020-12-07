# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class WikiConferenceItem(scrapy.Item):
    """
    Conference information as scraped from wikicfp
    """
    series = scrapy.Field()
    title = scrapy.Field()
    url = scrapy.Field()
    timetable = scrapy.Field()
    year = scrapy.Field()
    wayback_url = scrapy.Field()
    categories = scrapy.Field()
    accessible = scrapy.Field()
    crawled = scrapy.Field()


class ConferencePage(scrapy.Item):
    """
    ConferencePage information for each individual webpage
    """
    conf_id = scrapy.Field()
    url = scrapy.Field()
    html = scrapy.Field()
    content_type = scrapy.Field()
    processed = scrapy.Field()
