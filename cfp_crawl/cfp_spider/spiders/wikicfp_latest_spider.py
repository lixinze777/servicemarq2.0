import re
import scrapy
from scrapy.spiders import Rule
from scrapy.linkextractors import LinkExtractor
from .base_wikicfp_spider import BaseCfpSpider


class WikicfpLatestSpider(BaseCfpSpider):
    name = 'latest'
    # allowed_domains = ['wikicfp.com']
    start_urls = ['http://www.wikicfp.com/cfp/allcfp']
    num_conf_crawled = 0

    rules = (
        # Traverse all pages
        Rule(LinkExtractor(allow='cfp/allcfp'),
             callback='parse_wikicfp_page', follow=True),
        # Individual Conference page on wikicfp
        Rule(LinkExtractor(allow='cfp/servlet/event.showcfp'),
             callback='parse_wikicfp_page'),
    )

    def parse_wikicfp_page(self, response):
        """
        Parses Conferences on wikicfp domain and follow links to actual conference page if link exists
        """
        # Processing of individual CFP page within wikicfp
        if re.search('cfp/servlet/event.showcfp', response.url):  # Conference page
            self.num_conf_crawled += 1
            # Series information exists only for series crawl in wikicfp_all
            response.meta['series'] = "Unknown"
            yield self.process_wikiconf(response)
