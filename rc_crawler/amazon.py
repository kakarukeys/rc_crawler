from lxml.html import document_fromstring


BASE_URL = "https://www.amazon.com"
SEARCH_URL_TEMPLATE = "/s/ref=nb_sb_noss_1?url=search-alias%3Daps&field-keywords={}"
CRAWL_DEVICE_TYPE = "mobile"
FOLLOW_NEXT_MAX = 2


def generate_search_url(keyword):
    """ Returns search url, referer url

    :param keyword: search keyword
    :rtype: a tuple
    """
    return BASE_URL + SEARCH_URL_TEMPLATE.format(keyword.replace(' ', '+')), BASE_URL


def extract_search_results(target, html):
    """ Returns a dictionary of useful info from search results ``html``

    :param target: Target object
    :param html: page html string
    :rtype: a dict
    """
    tree = document_fromstring(html)
    output = {}

    try:
        total_listings_str = tree.cssselect("#s-slick-result-header span")[0].text_content()
        output["total_listings"] = int(''.join(c for c in total_listings_str if c.isdigit()))
    except (IndexError, ValueError):
        output["total_listings"] = None

    if target.follow_next_count < FOLLOW_NEXT_MAX:
        try:
            output["next_url"] = BASE_URL + tree.cssselect(".a-pagination > li:nth-child(2) > a")[0].attrib["href"]
        except (IndexError, KeyError):
            output["next_url"] = None

    try:
        output["listing_urls"] = [BASE_URL + link.attrib["href"] for link in tree.cssselect("a.aw-search-results")]
    except KeyError:
        output["listing_urls"] = []

    return output


def extract_listing(target, html):
    """ Returns a dictionary of useful info from listing page ``html``

    :param target: Target object
    :param html: page html string
    :rtype: a dict
    """
    tree = document_fromstring(html)

    try:
        title = tree.cssselect("title")[0].text_content()
    except IndexError:
        title = None

    return {"title": title}
