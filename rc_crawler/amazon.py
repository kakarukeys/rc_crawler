from typing import Tuple
from lxml.html import document_fromstring
from .crawler import Target


BASE_URL = "https://www.amazon.com"
SEARCH_URL_TEMPLATE = "/s/ref=nb_sb_noss_1?url=search-alias%3Daps&field-keywords={}"
FOLLOW_NEXT_MAX = 2

CRAWL_DEVICE_TYPE = "mobile"

# reqs / secs
RATE_LIMIT_PARAMS = [{"max_rate": 2, "time_period": 5}]


def generate_search_url(keyword: str) -> Tuple[str, str]:
    """ Returns search url, referer url """
    return BASE_URL + SEARCH_URL_TEMPLATE.format(keyword.replace(' ', '+')), BASE_URL + '/'


def extract_search_results(target: Target, html: str) -> dict:
    """ Returns a dictionary of useful info from search results <html> """
    tree = document_fromstring(html)
    output = {}

    try:
        total_listings_str = tree.cssselect("#s-slick-result-header span")[0].text_content()
        output["total_listings"] = int(''.join(c for c in total_listings_str if c.isdigit()))
    except (IndexError, ValueError):
        output["total_listings"] = None

    if target.follow_next_count < FOLLOW_NEXT_MAX:
        try:
            output["next_url"] = BASE_URL + tree.cssselect(".a-pagination > li:nth-child(2) > a")[0].attrib["href"].lstrip()
        except (IndexError, KeyError):
            output["next_url"] = None

    try:
        output["listing_urls"] = [BASE_URL + link.attrib["href"].lstrip() for link in tree.cssselect("a.aw-search-results")]
    except KeyError:
        output["listing_urls"] = []

    return output


def extract_listing(target: Target, html: str) -> dict:
    """ Returns a dictionary of useful info from listing page <html> """
    tree = document_fromstring(html)

    try:
        title = tree.cssselect("title")[0].text_content()
    except IndexError:
        title = None

    return {"title": title}
