from typing import Tuple
from lxml.html import document_fromstring
from rc_crawler.crawler import Target


BASE_URL = "http://www.aliexpress.com"
SEARCH_URL_TEMPLATE = "/wholesale?catId=0&initiative_id=&SearchText={}"
FOLLOW_NEXT_MAX = 2

CRAWL_DEVICE_TYPE = "desktop"

# reqs / secs
RATE_LIMIT_PARAMS = [{"max_rate": 2, "time_period": 10}, {"max_rate": 150, "time_period": 1*60*60}]


def generate_search_url(keyword: str) -> Tuple[str, str]:
    """ Returns search url, referer url """
    return BASE_URL + SEARCH_URL_TEMPLATE.format(keyword.replace(' ', '+')), BASE_URL + '/'


def _extract_price_distribution(tree):
    """ Returns aliexpress's % buyers vs price range
        tree: html element tree
        returns: {(price_from, price_to): percentage}
    """
    price_histogram = tree.cssselect("#price-range-list > li")
    price_distribution = {}

    try:
        for el in price_histogram:
            range_el = el.find_class("histogram-height")[0]

            try:
                price_from = float(range_el.attrib["price-range-from"])
            except ValueError:
                price_from = None

            try:
                price_to = float(range_el.attrib["price-range-to"])
            except ValueError:
                price_to = None

            percentage_str = el.find_class("ui-histogram-ballon")[0].text_content()
            percentage = float(percentage_str.split('%')[0])

            price_distribution[(price_from, price_to)] = percentage
    except (IndexError, KeyError, ValueError):
        pass

    return price_distribution


def extract_search_results(html: str, target: Target, **kwargs) -> dict:
    """ Returns a dictionary of useful info from search results <html> """
    tree = document_fromstring(html)
    output = {}

    try:
        total_listings_str = tree.cssselect(".search-count")[0].text_content()
        output["total_listings"] = int(''.join(c for c in total_listings_str if c.isdigit()))
    except (IndexError, ValueError):
        output["total_listings"] = None

    output["price_distribution"] = _extract_price_distribution(tree)

    if target.follow_next_count < FOLLOW_NEXT_MAX:
        try:
            output["next_url"] = "http:" + tree.cssselect("a.page-next")[0].attrib["href"].lstrip()
        except (IndexError, KeyError):
            output["next_url"] = None

    try:
        output["listing_urls"] = {"http:" + link.attrib["href"].lstrip() for link in tree.cssselect("a.product")}
    except KeyError:
        output["listing_urls"] = set()

    return output


def extract_listing(html: str, **kwargs) -> dict:
    """ Returns a dictionary of useful info from listing page <html> """
    tree = document_fromstring(html)

    try:
        title = tree.cssselect("title")[0].text_content()
    except IndexError:
        title = None

    return {"title": title}
