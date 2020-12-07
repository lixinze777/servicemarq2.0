# Mining Call For Papers

## Setup

_python version: 3.6_

Installation of `pdftotext` dependencies
```
https://pypi.org/project/pdftotext/
```

Installation of python packages:
```
pip install -r requirements.txt
```

## Conference Crawling
### Run Crawler
```
python crawl.py <CRAWL_TYPE>
```

Additional configurations of the crawl are located at: `cfp_crawl/config.py`, also specifies crawl log and data save directories

`<CRAWL_TYPE>` options:

`wikicfp_latest` crawls details of the most recent conferences on the homepage of wikicfp at http://www.wikicfp.com

`wikicfp_all` traverses through and scapes information from every conference series on wikicfp starting from http://www.wikicfp.com/cfp/series?t=c&i=A

`conf_crawl` assumes a database populated with basic conference information obtained from either `wikicfp_latest`/`wikicfp_all` and proceeds to store the HTML information of the specified conferences. Crawls for directory specified in `cfp_crawl/config.py`.

### Notes on conf_crawl
Selenium chromedriver is needed to better simulate organic access of conference sites (e.g. waiting for the loading of javascript elements). The chromedriver should match your chrome version can be downloaded https://chromedriver.chromium.org/. Move the executable into this repo or as specified in `cfp_crawl/config.py`.