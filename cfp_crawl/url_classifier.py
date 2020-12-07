import re
from typing import List
from urllib.parse import urlparse
from selenium import webdriver


class URLClass:
    COMMITTEE = 'org'
    SPEAKERS = 'speakers'
    ADMINISTRATIVE = 'admin'
    UNKNOWN = 'unk'


# Regex string representations of possible keywords
org = 'organi[a-z]+|committee[a-z]*|prog[a-z]*|chair|about|people'
speakers = 'author[s]*|speaker[a-z]*|tutorial|workshop'

def classify_link(link_text, link_url):
    text = link_text.lower() if link_text else ""
    url = link_url.lower() if link_url else ""
    if re.search(org, url) or re.search(org, text):
        return URLClass.COMMITTEE
    elif re.search(speakers, url) or re.search(speakers, text):
        return URLClass.SPEAKERS
    else:
        return URLClass.UNKNOWN