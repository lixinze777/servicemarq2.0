import pandas as pd
import urllib
import scrapy
import sys
sys.path.append("....") 

from database.database_helper import DatabaseHelper
from database.database_config import DB_FILEPATH

from cfp_crawl.config import crawl_settings
from cfp_crawl.cfp_spider.items import WikiConferenceItem
from cfp_crawl.cfp_spider.spiders.utils import get_url_status
from cfp_crawl.cfp_spider.spiders.wikicfp_parser import WikiConfParser


class BaseCfpSpider(scrapy.spiders.CrawlSpider):

    custom_settings = crawl_settings

    def process_wikiconf(self, response):
        """
        Process individual conference page within wikicfp
            - Parse conference page and save basic conference info to database

        Returns link of conference page to facilitate crawling
        """
        parsed_conference: WikiConferenceItem = WikiConfParser.parse_conf(response)
        conf_id = DatabaseHelper.add_wikicfp_conf(parsed_conference, DB_FILEPATH)
        url = parsed_conference['url']
        if url:  # Check accessibilty of both direct URL and WaybackMachine URL
            return self.process_conference_url(url, conf_id, parsed_conference['wayback_url'])

    def process_conference_url(self, conf_url: str, conf_id: int, wayback_url):
        """
        Check if conference url is accessible, else checks availability on Waybackmachine Archive
        """
        # Metadata in case of request error
        meta = {
            'conf_id': conf_id
        }
        # Set arbitrary browser agent in header since certain sites block against crawlers
        try:
            if get_url_status(conf_url) == 200:
                DatabaseHelper.mark_accessibility(
                    conf_id, "Accessible URL", DB_FILEPATH)
        except Exception as e:
            DatabaseHelper.mark_accessibility(
                conf_id, e.__class__.__name__, DB_FILEPATH)
            return scrapy.spiders.Request(url=wayback_url, dont_filter=True, meta=meta,	
                                            callback=self.process_wayback_url)

    def process_wayback_url(self, response):
        """
        Process Wayback URL and marks as Wayback Accessible for Conference if successful
        """
        conf_id = response.meta['conf_id']
        DatabaseHelper.mark_accessibility(
            conf_id, "Wayback Accessible", DB_FILEPATH)
