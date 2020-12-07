import re
import scrapy
from scrapy import Request
from .base_wikicfp_spider import BaseCfpSpider


class WikicfpAllSpider(BaseCfpSpider):
    domain_name = "http://www.wikicfp.com/"
    name = 'wikicfp_all'
    start_urls = ['http://www.wikicfp.com/cfp/series?t=c&i=A']

    def parse(self, response):
        """
        Parses pages starting from page A of Conference Series pages
          - cfp/servlet/event: Individual conference
          - cfp/series: Consolidation of multiple programs
          - cfp/program: Singular program possibly containing CFPs
        """
        if re.search('cfp/servlet/event.showcfp', response.url):  # Individual Conference page
            yield self.process_wikiconf(response)
        else:
            table_main = response.xpath(
                '//div[contains(@class, "contsec")]/center/table')

            if re.search('cfp/series', response.url):  # List of Conference series
                series_links_row = table_main.xpath('./tr//tr')[2]
                series_links = series_links_row.xpath('.//a')
                for link in series_links:
                    link_url = link.xpath('./@href').get()
                    yield Request("".join([self.domain_name, link_url]))

                program_link_rows = table_main.xpath('./tr')[2].xpath('.//tr')
                for program_link in program_link_rows:
                    program_url = program_link.xpath('.//a/@href').get()
                    yield Request("".join([self.domain_name, program_url]))

            elif re.search('cfp/program', response.url):  # Conference Program
                try:
                    program_table = table_main.xpath(
                        './tr/td[contains(@align, "center")]')[1]
                except:
                    return

                conf_links = program_table.xpath('.//a')
                conf_series = response.xpath("//html/body//h2/text()").get()
                for conf_link in conf_links:
                    conf_url = conf_link.xpath('./@href').get()
                    yield Request("".join([self.domain_name, conf_url]), meta={'series': conf_series})
            else:
                pass
