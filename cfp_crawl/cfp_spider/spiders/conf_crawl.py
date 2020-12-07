import re
import scrapy
import sqlite3
import time
from typing import List, Tuple
from scrapy.spiders import CrawlSpider, Request
from selenium import webdriver
import sys
sys.path.append("....") 

from database.database_helper import DatabaseHelper
from database.database_config import DB_FILEPATH
from cfp_crawl.cfp_spider.items import ConferencePage
from cfp_crawl.cfp_spider.spiders.utils import get_content_type, get_relevant_urls, get_url_status
from cfp_crawl.config import crawl_settings, REQUEST_HEADERS, CHROMEDRIVER_FILEPATH


class ConferenceCrawlSpider(scrapy.spiders.CrawlSpider):
    """
    Retrieves urls from `WikicfpConferences` table and crawls each Conference homepage
    """

    name = "confcrawl"
    custom_settings = crawl_settings

    def __init__(self):
        super(ConferenceCrawlSpider, self).__init__()
        self.start_requests()
        # Selenium Driver
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument('--headless')
        # Remove DevToolsActivePort error
        chrome_options.add_argument('--no-sandbox')
        self.driver = webdriver.Chrome(
            CHROMEDRIVER_FILEPATH, chrome_options=chrome_options)

    def start_requests(self):
        """
        Get all Conference Homepage URLs from database and yields scrapy Requests
        - Use wayback url if original url does not indicate year of conference
        """
        DatabaseHelper.create_db(DB_FILEPATH)
        conn = sqlite3.connect(str(DB_FILEPATH))
        cur = conn.cursor()
        confs = cur.execute(
            "SELECT * FROM WikicfpConferences WHERE crawled='No'").fetchall()
        cur.close()
        conn.close()
        for conf in confs:
            conf_id, url, wayback_url, accessibility = conf[0], conf[3], conf[6], conf[8]
            # Two consecutive digits usually indicates conference year in url
            if accessibility == "Accessible URL" and re.search('\d{2}', url):
                access_url = url
            elif wayback_url != "Not Available":
                access_url = wayback_url
            else:
                continue
            yield Request(url=access_url, dont_filter=True,
                          meta={'conf_id': conf_id},
                          callback=self.parse,
                          errback=self.parse_page_error,
                          headers=REQUEST_HEADERS)

    def parse(self, response):
        """
        Parses conference homepage and determines whether found URLs are valid for further crawling
        """
        conf_id = response.meta['conf_id']
        content_type = get_content_type(response)
        DatabaseHelper.mark_crawled(conf_id, DB_FILEPATH)
        self.add_conf_page(conf_id, response)
        if content_type != 'pdf':
            # Crawl relevant links
            for url in get_relevant_urls(response, self.driver):
                if get_url_status(url) != 200:
                    DatabaseHelper.add_page(
                        ConferencePage(conf_id=conf_id, url=url, html="",
                                       content_type="Inaccessible"), DB_FILEPATH)
                elif not DatabaseHelper.page_saved(url, DB_FILEPATH):
                    yield Request(url=url, dont_filter=True, meta={'conf_id': conf_id},
                                  callback=self.parse_aux_conf_page,
                                  errback=self.parse_page_error)
                else:
                    pass

    def parse_aux_conf_page(self, response):
        """
        Parses auxiliary conference pages
        """
        conf_id = response.request.meta['conf_id']
        self.add_conf_page(conf_id, response)

    def add_conf_page(self, conf_id: int, response: 'Response'):
        """
        Adds Conference Page to database
        """
        content_type = get_content_type(response)
        if content_type == 'pdf':
            page_id = DatabaseHelper.add_page(
                ConferencePage(conf_id=conf_id, url=response.url, html="",
                               content_type=content_type, processed="No"), DB_FILEPATH)
        else:
            self.driver.get(response.url)
            time.sleep(3)  # Ensure javascript loads
            page_html = self.driver.page_source
            # Add Conference Homepage to database
            page_id = DatabaseHelper.add_page(
                ConferencePage(conf_id=conf_id, url=response.url, html=page_html,
                               content_type=content_type, processed="No"), DB_FILEPATH)

    def parse_page_error(self, error):
        print("============================")
        print("Error processing:")
        print("Page with conference id: {}, url: {}".format(
            error.request.meta['conf_id'],
            error.request.url))
        print(repr(error))
        print("============================")
