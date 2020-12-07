from pathlib import Path
import sys
sys.path.append("../database")
import database_config

# Set arbitrary browser agent in header since certain sites block against crawlers
REQUEST_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36'}

# Filepath directory for CHROMEDRIVER

curr_dir = Path().parent.resolve()
CHROMEDRIVER_FILEPATH = Path.joinpath(curr_dir, 'driver/chromedriver')

# Scrapy custom settings
crawl_settings = {
    'LOG_LEVEL': 'INFO',
    'DOWNLOAD_DELAY': 0.5,
    'DOWNLOAD_TIMEOUT': 30,
    'LOG_FILE': database_config.LOG_FILEPATH,
    'JOBDIR': database_config.CRAWL_FILEPATH,
    'CLOSESPIDER_TIMEOUT': 18000 # Remember to set this to resume crawl as needed
}