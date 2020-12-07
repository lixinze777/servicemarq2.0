import re
import time
from urllib.request import Request, urlopen
from urllib.parse import urlparse, urljoin

from cfp_crawl.config import REQUEST_HEADERS
from cfp_crawl.url_classifier import classify_link, URLClass


def get_content_type(response: 'Response'):
    """ Get content type of Url
    """
    content_type = response.headers.get('content-type')
    content_type = content_type.decode('utf-8') if content_type else ''
    content_type = 'pdf' if 'application/pdf' in content_type else 'html'
    return content_type


def get_url_status(url: str):
    """ Get response status code for url
    - Hardcode error code
    """
    try:
        conf_link_res = urlopen(Request(url, headers=REQUEST_HEADERS))
        return conf_link_res.status
    except Exception as e:
        return 404


def relevant_url(base_url, url, url_text):
    """ Returns url if relevant, else empty string
    - Check if url is in the same domain as base conf url
    - Check if url and url element's text is relevant
    """
    if same_domain(base_url, url):
        full_link = urljoin(base_url, url)
        url_class = classify_link(url_text, url)
        if url_class != URLClass.UNKNOWN:
            return full_link
    return ""


def get_relevant_urls(response: 'Response', driver: 'Webdriver'):
    """ Retrieves the relevant links from a Conference Homepage
    - Check for relevancy of both url and url element's text
    - Attempts retrieval using selenium if none found for plain HTML
    """
    conf_url = response.url
    relevant_urls = []
    # Get links directly from HTML
    for link_selector in response.xpath('//*[@href]'):
        url_el_text = link_selector.xpath('text()').get()
        url = link_selector.xpath('@href').get()
        # Only retrieve links on the same domain
        pos_rel_url = relevant_url(conf_url, url, url_el_text)
        if pos_rel_url:
            relevant_urls.append(pos_rel_url)
    if relevant_urls:
        return relevant_urls

    # Fallback to selenium if no relevant links found
    driver.get(response.url)
    time.sleep(3)  # Ensure javascript loads
    for link_element in driver.find_elements_by_xpath("//*[@href]"):
        url_el_text = link_element.get_attribute('text')
        url = link_element.get_attribute('href')
        pos_rel_url = relevant_url(conf_url, url, url_el_text)
        if pos_rel_url:
            relevant_urls.append(pos_rel_url)

    return relevant_urls


def same_domain(conf_home: str, aux_link: str):
    """ Checks if two urls are from the same domain
    - Handles diff/same domains for Wayback as well
    """
    WAYBACK_DOMAIN = 'http://web.archive.org'
    conf_home_domain = urlparse(conf_home).netloc
    aux_link_domain = urlparse(aux_link).netloc
    if WAYBACK_DOMAIN in conf_home_domain:  # Compare for wayback
        # Get split result of /web/.../
        actual_conf_url = re.split('\/web\/[0-9]*\/', conf_home)[1]
        actual_aux_link_url = re.split('\/web\/[0-9]*\/', aux_link)[1]
        actual_conf_domain = urlparse(actual_conf_url)
        actual_aux_link_domain = urlparse(actual_aux_link_url)
        return not actual_aux_link_domain or actual_conf_domain == actual_aux_link_domain
    else:  # Normal comparison
        return not aux_link_domain or conf_home_domain == aux_link_domain
