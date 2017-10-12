from typing import Generator
from lxml.html import document_fromstring


BASE_URL = "http://thieve.co"
SEARCH_URL_TEMPLATE = "/?filter=Most%20Popular"
CRAWL_DEVICE_TYPE = "desktop"

# reqs / secs
RATE_LIMIT_PARAMS = [{"max_rate": 2, "time_period": 10}]


def generate_search_url() -> Generator[dict, None, None]:
    """ yields target params """
    for offset in range(0, 100, 24):
        yield {
            "keyword": "most popular",
            "url": BASE_URL + SEARCH_URL_TEMPLATE + ("&offset={}".format(offset) if offset else ''),
            "referer": BASE_URL + '/'
        }


def _extract_product_item(item):
    try:
        title = item.cssselect(".title")[0].text_content()
    except KeyError:
        title = None

    try:
        price = float(item.cssselect(".price")[0].text_content()[1:])
    except (KeyError, ValueError):
        price = None

    try:
        like_count = int(item.cssselect(".like-count")[0].text_content())
    except (KeyError, ValueError):
        like_count = None

    if title:
        return {"title": title, "price": price, "like_count": like_count}


def extract_search_results(html: str, **kwargs) -> dict:
    """ Returns a dictionary of useful info from search results <html> """
    tree = document_fromstring(html)
    products = filter(bool, map(_extract_product_item, tree.cssselect(".product-feed .product-item")))
    return {"products": list(products)}
