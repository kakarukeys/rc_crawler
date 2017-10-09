from typing import Tuple
import logging

from lxml.html import document_fromstring, tostring

from rc_crawler.crawler import Target, AntiScrapingError

logger = logging.getLogger("rc_crawler.amazon")


BASE_URL = "http://www.amazon.com"
SEARCH_URL_TEMPLATE = "/s/ref=nb_sb_noss?url=search-alias%3Daps&field-keywords={}"
FOLLOW_NEXT_MAX = 6

CRAWL_DEVICE_TYPE = "mobile"

# reqs / secs
RATE_LIMIT_PARAMS = [{"max_rate": 2, "time_period": 5}]


def generate_search_url(keyword: str) -> Tuple[str, str]:
    """ Returns search url, referer url """
    return BASE_URL + SEARCH_URL_TEMPLATE.format(keyword.replace(' ', '+')), BASE_URL + '/'


def check_if_scraping_is_blocked(html):
    if len(html) < 7218:
        raise AntiScrapingError("Server is returning a blocked page")


def _has_results(tree):
    try:
        return "sorry" not in tree.cssselect("#results h4")[0].text_content()
    except IndexError:
        return True


def extract_search_results(html: str, target: Target, **kwargs) -> dict:
    """ Returns a dictionary of useful info from search results <html> """
    check_if_scraping_is_blocked(html)

    tree = document_fromstring(html)
    output = {}

    if _has_results(tree):
        try:
            total_listings_str = tree.cssselect("#s-slick-result-header span")[0].text_content()
            output["total_listings"] = int(''.join(c for c in total_listings_str if c.isdigit()))
        except (IndexError, ValueError):
            output["total_listings"] = None

        if target.follow_next_count < FOLLOW_NEXT_MAX:
            try:
                next_url_button = tree.cssselect(".a-pagination > li:nth-child(2)")[0]

                if "a.disabled" not in next_url_button.classes:
                    output["next_url"] = BASE_URL + next_url_button.cssselect('a')[0].attrib["href"].lstrip()

            except (IndexError, KeyError):
                output["next_url"] = None

        output["listing_urls"] = set()
        listings = tree.cssselect("#resultItems > li") # linear layout

        if listings:
            for li in listings:
                try:
                    listing_url = BASE_URL + li.cssselect("a.aw-search-results")[0].attrib["href"].lstrip()
                except (IndexError, KeyError) as e:
                    logger.warning("could not extract listing url from {0}, target: {1}".format(tostring(li), target))
                    logger.exception(e)
                else:
                    output["listing_urls"].add(listing_url)

        else:
            logger.info("multi-item row layout detected, target: {}".format(target))
            listings = tree.cssselect("#resultItems li")

            for li in listings:
                try:
                    listing_url = BASE_URL + li.cssselect("a.sx-grid-link")[0].attrib["href"].lstrip()
                except (IndexError, KeyError) as e:
                    logger.warning("could not extract listing url from {0}, target: {1}".format(tostring(li), target))
                    logger.exception(e)
                else:
                    output["listing_urls"].add(listing_url)

    return output


def extract_listing(html: str, **kwargs) -> dict:
    """ Returns a dictionary of useful info from listing page <html> """
    check_if_scraping_is_blocked(html)

    tree = document_fromstring(html)

    try:
        title = tree.cssselect("title")[0].text_content()
    except IndexError:
        title = None

    return {"title": title}
