import json
import re
import urllib
from typing import List
from cfp_crawl.cfp_spider.items import WikiConferenceItem

import sys
sys.path.append("....") 
from database.database_config import DB_FILEPATH

class WikiConfParser:

    @staticmethod
    def parse_conf(response):
        """
        Parses only for conference pages, omits for allcfp pages
        """

        if 'allcfp' in response.url:
            return

        # table index varies since certain pages might not contain links
        table_index = {
            "TITLE": 1,
            "LINK": 2,
            "TIMETABLE": 4,
            "MAIN": 7
        }

        # Get table containing CFP info
        table_main = response.xpath(
            '//div[contains(@class, "contsec")]/center/table')
        table_rows = table_main.xpath('tr')

        # Get title
        conference_title: str = table_rows[table_index["TITLE"]].xpath(
            'td/h2//span[contains(@property, "v:description")]/text()').get().strip()

        # Certain conference pages might not contain link considering it is not mandatory
        conference_link: str = table_rows[table_index["LINK"]].xpath(
            'td/a/@href').get()
        if not conference_link:
            table_index["TIMETABLE"] = table_index["TIMETABLE"] - 1
            table_index["MAIN"] = table_index["MAIN"] - 1
        else:
            conference_link = conference_link.strip()

        # Inner table containing timetable and category info are highly nested
        inner_table: 'Selector' = table_rows[table_index["TIMETABLE"]].xpath(
            './/tr//table')[0]
        timetable_info, category_info = WikiConfParser.get_innertable_info(
            inner_table)  # timetable_info: Dict, category_info: List

        # Year and Wayback_URL processing for if link does not work
        year, wayback_url = WikiConfParser.get_wayback_info(
            timetable_info, conference_link)

        # Main block of information
        cfp_main_block = table_rows[table_index["MAIN"]]

        conference: WikiConferenceItem = WikiConferenceItem(
            series=response.meta['series'],
            title=conference_title,
            url=conference_link,
            timetable=timetable_info,
            year=year,
            wayback_url=wayback_url,
            categories=category_info,
            accessible='Unknown',
            crawled='No'
        )

        return conference

    @staticmethod
    def get_wayback_info(timetable: str, conference_link: str):
        """
        Get year from timetable and wayback url for latest timestamp to
        facilitate Conference crawling when link is unable to be directly accessed
        """
        year = -1
        wayback_url = "Not Available"
        if type(timetable) == dict:  # Handling of NaN
            year_matched = re.search(r'\b(19|20)\d{2}', timetable['When'])
            if year_matched:
                year = year_matched.group()

        if year != -1:
            wayback_url_check = "https://archive.org/wayback/available?url={}/&timestamp={}".format(
                conference_link, year)
        else:
            wayback_url_check = "https://archive.org/wayback/available?url={}".format(
                conference_link, year)

        # Get wayback_url only if available
        try:
            wb_url_check_res = urllib.request.urlopen(wayback_url_check)
            if wb_url_check_res.status == 200:
                parsed_response = wb_url_check_res.read().decode('utf-8')
                json_response = json.loads(parsed_response)
                archived_snapshot = json_response['archived_snapshots']
                # No archived snapshot means not available on wayback
                if archived_snapshot:
                    wayback_url = archived_snapshot['closest']['url']
        except Exception as err:
            wayback_url = "Not Available"

        return year, wayback_url

    @staticmethod
    def get_innertable_info(inner_table: 'Selector'):
        """
        Gets information of inner table containing timetable and category information
        Arguments:
            inner_table: root `tr` with 2 nested `tr`s of timetable and category info
        """
        timetable, category_info = tuple(inner_table.xpath('./tr'))
        # Get all info on category_info
        category_info = category_info.xpath(
            './/a[contains(@href, "call")]/text()').getall()
        # Get all info from timetable
        time_locale_fields = ["When", "Where"]
        deadline_fields = ["Submission Deadline", "Notification Due",
                           "Final Version Due", "Abstract Registration Due"]
        timetable_info = {}
        timetable_info_rows = timetable.xpath('.//tr')
        for info_row in timetable_info_rows:
            field_key = info_row.xpath('./th/text()').get().strip()
            if field_key in time_locale_fields:
                field_value = info_row.xpath('td/text()').get().strip()
            elif field_key in deadline_fields:
                field_value_parent = info_row.xpath(
                    './td/span')  # TBD field is not nested
                if field_value_parent:
                    field_value = field_value_parent.xpath(
                        './span[@property="v:startDate"]/text()').get().strip()
                else:
                    field_value = info_row.xpath('./td/text()').get().strip()
            else:
                raise("Undefined inner table property: {}".format(field_key))
            timetable_info[field_key] = field_value

        return timetable_info, category_info
