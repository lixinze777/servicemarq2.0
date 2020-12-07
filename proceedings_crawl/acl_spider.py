import argparse
import re
import pdftotext
import sqlite3
import scrapy
import urllib
from pathlib import Path
from scrapy.crawler import CrawlerProcess
from scrapy import Request
from urllib.parse import urljoin


class ACLSpider(scrapy.spiders.CrawlSpider):
    """Crawler for ACL Proceedings
    """

    custom_settings = {
        'DOWNLOAD_DELAY': 0.1,
    }

    def __init__(self, db_filepath: str):
        self.db_filepath = db_filepath
        super(ACLSpider).__init__()

    name = "hwz_spider"
    # Crawl links are limited to this domain
    domain_name = "https://www.aclweb.org/"
    start_urls = ["https://www.aclweb.org/anthology/"]

    def parse(self, response):
        """Parsing rules for crawl of ACL anthology
        - Retrieves basic conference details and proceeding PDF urls for further processing
        """
        if "events" in response.url: # Individual Conference Iterations
            conference_title = response.xpath("//h2[contains(@id, 'title')]/text()").get()
            conference_abbrev = re.sub('-2019', '', response.url.split('/')[-2]).strip().upper() # Abbreviation of conference, e.g. PACLIC
            pdf_url_els = response.xpath("//h4/span/a[contains(@class, 'badge-primary')]/@href")
            pdf_urls = list(map(lambda url: url.get(), pdf_url_els))
            for pdf_url in pdf_urls:
                pdf_tag_href = "#" + re.sub('.pdf', '', pdf_url.split("/")[-1]).lower()
                pdf_title = response.xpath("//a[contains(@href, '{}')]/text()".format(pdf_tag_href)).get()
                pdf_title = re.sub(':', '-', pdf_title)
                self.process_pdf(pdf_url, conference_title, conference_abbrev, pdf_title)
        else: # ACL Anthology home
            page_links = response.xpath('//a/@href')
            valid_links = list(
                filter(lambda link: "events" in link.get(), page_links))
            urls = list(map(lambda link: urljoin(
                self.domain_name, link.get()), valid_links))
            for url in urls:
                yield Request(url)
                break

    def process_pdf(self, pdf_url, conference_title, conference_abbrev, pdf_title):
        """Saves PDFs proceedings and processes them before deleting them

        Args:
            pdf_url (str): Url to PDF
            conference_title (str): Full title of Conference
            conference_abbrev (str): Abbreviation of Conference name
            pdf_title (str): Name of saved PDF file
        """
        pdf_folder = Path("./{}".format(conference_title)
                          ).mkdir(parents=True, exist_ok=True)
        pdf_filepath = "./{}/{}.pdf".format(conference_title, pdf_title)
        pdf_file = Path(pdf_filepath)
        if not pdf_file.is_file():
            print("Downloading: {}".format(pdf_title))
            urllib.request.urlretrieve(pdf_url, pdf_filepath)

        with sqlite3.connect(self.db_filepath) as cnx:
            series = re.sub('\([0-9]+\)', '', conference_title).strip()
            year = re.findall('([0-9]+)', conference_title)[0].strip()
            title = re.sub('Proceedings of the', f'{conference_abbrev}: The', pdf_title)
            cur = cnx.cursor()
            cur.execute("INSERT INTO WikicfpConferences(series, title, url, year, accessible, crawled) VALUES\
                        (?, ?, ?, ?, ?, ?)", (series, pdf_title, pdf_url, year, 'Accessible PDF', 'No'))
            cnx.commit()
            conf_id = cur.lastrowid

        self.extract_pdf_content(pdf_filepath, conf_id, pdf_url)

        pdf_file.unlink() # Delete PDF due to space constraints

    def extract_pdf_content(self, pdf_filepath, conf_id, pdf_url):
        """Extracts individual lines from PDF proceedings up to Table of Contents

        Args:
            pdf_filepath (str): Filepath to saved pdf
            conf_id (int): conf_id in database for extracted info for PDF
            pdf_url (str): URL to PDF
        """
        print("Extracting lines from: {}".format(pdf_filepath))

        with open(pdf_filepath, "rb") as f:
            pdf = pdftotext.PDF(f)

        all_lines = []
        for i, page in enumerate(pdf):
            if 'Table of Contents' in page:
                break
            # Add Lines
            lines = page.split('\n')
            lines = list(map(lambda l: l.strip(), lines))
            lines = list(filter(lambda l: l != "", lines))
            all_lines += lines

        with sqlite3.connect(self.db_filepath) as cnx:
            cur = cnx.cursor()
            # Add ConferencePages
            cur.execute("INSERT INTO ConferencePages (conf_id, url, content_type, processed) VALUES\
                            (?, ?, ?, ?)", (conf_id, pdf_url, 'pdf', 'No')) # Ignore adding of actual text
            page_id = cur.lastrowid

            for line_num, line_text in enumerate(all_lines):
                cur.execute("INSERT INTO PageLines (page_id, line_num, line_text, tag, indentation) VALUES\
                            (?, ?, ?, ?, ?)", (page_id, line_num, line_text, 'p', 1))
            cnx.commit()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('db_filepath', type=str,
                        help="Specify database file to predict lines")
    args = parser.parse_args()

    db_filepath = str(Path(args.db_filepath).resolve())

    process = CrawlerProcess(settings={})
    process.crawl(ACLSpider, db_filepath=db_filepath)
    process.start()
