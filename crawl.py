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
from journal_crawl.journal_spider.spiders.IEEEjournal import IEEEjournal

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

if crawl_type == "ieee_crawl":
    url_list = [] # a list of urls of journal home pages
    journal_list = [] # a list of journal editorial boards, in the form of pair of title and url
    line_list = []

    loader = IEEEjournal("https://ieeexplore.ieee.org/browse/periodicals/topic?refinements=ContentType:Journals&selectedValue=Topic:Computing%20and%20Processing")
    url_list = loader.load_page(url_list)
    loader.close()

    loader2 = IEEEjournal("https://ieeexplore.ieee.org/browse/periodicals/topic?selectedValue=Topic:Computing%20and%20Processing&refinements=ContentType:Journals&pageNumber=2")
    url_list = loader2.load_page(url_list)
    loader2.close()

    loader3 = IEEEjournal("https://ieeexplore.ieee.org/browse/periodicals/topic?selectedValue=Topic:Computing%20and%20Processing&refinements=ContentType:Journals&pageNumber=3")
    url_list = loader3.load_page(url_list)
    loader3.close()

    loader4 = IEEEjournal("https://ieeexplore.ieee.org/browse/periodicals/topic?selectedValue=Topic:Computing%20and%20Processing&refinements=ContentType:Journals&pageNumber=4")
    url_list = loader4.load_page(url_list)
    loader4.close()

    loader5 = IEEEjournal("https://ieeexplore.ieee.org/browse/periodicals/topic?selectedValue=Topic:Computing%20and%20Processing&refinements=ContentType:Journals&pageNumber=5")
    url_list = loader5.load_page(url_list)
    loader5.close()

    if len(url_list) <= 100:
        print(str(len(url_list))+" pages loaded. Loading incomplete, please run the code again")
        EDITOR_PAGE_EXTRACT = False
    else:
        print(str(len(url_list))+" pages will be parsed")
        EDITOR_PAGE_EXTRACT = True

    for url in url_list:
        loader = IEEEjournal(url)
        journal_list.append(loader.load_page2())
        loader.close()

    print(journal_list)
    print(len(journal_list))

    for title, url in journal_list:
        if title != "NA" and journal_list != "NA":
            database_helper.DatabaseHelper.addJournal(items.JournalInfoItem(title=title, publisher="IEEE", url=url), database_config.DB_FILEPATH)

    publisher = "IEEE"
    urlstitles = database_helper.DatabaseHelper.getJournalUrlsTitles(database_config.DB_FILEPATH, publisher)

    for url, title in urlstitles:
        loader = IEEEjournal(url)
        line_list = loader.load_page3(line_list, title)
        loader.close()

    for title, line in line_list:
        if re.search('[a-zA-Z]', line) is not None: # contains English character
            line = line.replace("<"," <").replace(">","> ").replace(","," ,").replace(":"," : ").replace("  "," ")
            database_helper.DatabaseHelper.addLine(items.JournalLineItem(publisher="IEEE", title=title, line=line), database_config.DB_FILEPATH)
    
    
if crawl_type not in spider_type.keys():
    print("Unspecified crawl type")
    print("Usage:\n\t python crawl <crawler_type>\n\t\
        'wikicfp_all': WikicfpAllSpider\n\t\
        'wikicfp_latest': WikicfpLatestSpider\n\t\
        'conf_crawl': ConferenceCrawlSpider\n\t\
        'journal_all': SpringerJournalSpider\n\t\
        'springer_crawl': SpringerSpider\n\t\
        'acm_crawl': ACMSpider\n\t\
        'ieee_crawl': IEEEjournal"
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
