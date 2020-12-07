import argparse
import scrapy

import sys
sys.path.append(sys.path[1]+"/database")
sys.path.append(sys.path[1]+"/journal_crawl")
sys.path.append(sys.path[1]+"/journal_crawl/journal_spider")
from scrapy.crawler import CrawlerProcess
from database.database_config import DB_FILEPATH, CRAWL_FILEPATH
from database.database_helper import DatabaseHelper
from cfp_crawl.cfp_spider.spiders.base_wikicfp_spider import BaseCfpSpider
from cfp_crawl.cfp_spider.spiders.wikicfp_all_spider import WikicfpAllSpider
from cfp_crawl.cfp_spider.spiders.wikicfp_latest_spider import WikicfpLatestSpider
from cfp_crawl.cfp_spider.spiders.conf_crawl import ConferenceCrawlSpider
from journal_crawl.journal_spider.spiders.springerjournal import SpringerJournal
from journal_crawl.journal_spider.spiders.ACMjournal import ACMjournal
from journal_crawl.journal_spider.spiders.springer_spider import SpringerSpider
from journal_crawl.journal_spider.spiders.ACM_spider import ACMSpider

parser = argparse.ArgumentParser(description='')
parser.add_argument('crawler', type=str, help="Specifies crawler type")
args = parser.parse_args()
crawl_type = args.crawler

# Start crawl
process = CrawlerProcess(settings={})
spider_type = {
    'wikicfp_all': WikicfpAllSpider,
    'wikicfp_latest': WikicfpLatestSpider,
    'conf_crawl': ConferenceCrawlSpider,
    'journal_all': SpringerJournal,
    'springer_crawl': SpringerSpider,
    'acm_crawl': ACMSpider,
}
if crawl_type not in spider_type.keys():
    print("Unspecified crawl type")
    print("Usage:\n\t python crawl <crawler_type>\n\t\
        'wikicfp_all': WikicfpAllSpider\n\t\
        'wikicfp_latest': WikicfpLatestSpider\n\t\
        'conf_crawl': ConferenceCrawlSpider\n\t\
        'journal_all': SpringerJournalSpider\n\t\
        'springer_crawl': SpringerSpider\n\t\
        'acm_crawl': ACMSpider"
    )

else:
    if crawl_type == 'journal_all':
        DatabaseHelper.create_db(DB_FILEPATH)  # Create necessary DB tables
        ACMloader = ACMjournal()
        ACMloader.load_page()
        ACMloader.close()
    if crawl_type == 'wikicfp_all' or crawl_type == 'wikicfp_latest':
        DatabaseHelper.create_db(DB_FILEPATH)  # Create necessary DB tables
    process.crawl(spider_type[crawl_type])
    process.start()
