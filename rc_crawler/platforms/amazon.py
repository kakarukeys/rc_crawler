from typing import Tuple
import logging

from lxml.html import document_fromstring, tostring, HtmlElement

from rc_crawler.crawler import Target, AntiScrapingError

logger = logging.getLogger("rc_crawler.amazon")


BASE_URL = "http://www.amazon.com"
SEARCH_URL_TEMPLATE = "/s/ref=nb_sb_noss?url=search-alias%3Daps&field-keywords={}"
FOLLOW_NEXT_MAX = 5

CRAWL_DEVICE_TYPE = "mobile"

# reqs / secs
RATE_LIMIT_PARAMS = [
    {"max_rate": 2, "time_period": 10},
    {"max_rate": 200, "time_period": 1*60*60},
    {"max_rate": 1500, "time_period": 12*60*60},
]

CAPTCHA_SOLVER_CONFIG = "-psm 6 uppercase_letters"


def generate_search_url(keyword: str) -> Tuple[str, str]:
    """ Returns search url, referer url """
    return BASE_URL + SEARCH_URL_TEMPLATE.format(keyword.replace(' ', '+')), BASE_URL + '/'


def _extract_captcha_challenge(tree, **kwargs):
    """ Returns information needed for answering a captcha challenge """
    try:
        form = tree.cssselect("form")[0]
        captcha_image_url = form.cssselect("img")[0].attrib["src"]
    except (IndexError, KeyError) as e:
        raise AntiScrapingError("Unable to find captcha image url on the challenge page") from e

    return {
        "captcha_image_url": captcha_image_url,

        "submission_form": {
            "action": BASE_URL + form.action,
            "method": form.method,
            "data": dict(form.fields),
        }
    }


def read_html(extractor):
    def wrapped(html, *args, **kwargs):
        if len(html) < 7218:
            extractor_to_use = _extract_captcha_challenge
        else:
            extractor_to_use = extractor

        tree = document_fromstring(html)

        return extractor_to_use(tree, *args, **kwargs)

    return wrapped


def _has_results(tree):
    try:
        return "sorry" not in tree.cssselect("#results h4")[0].text_content()
    except IndexError:
        return True


@read_html
def extract_search_results(tree: HtmlElement, target: Target, **kwargs) -> dict:
    """ Returns a dictionary of useful info from search results <tree> """
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

                if "a-disabled" not in next_url_button.classes:
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

        elif tree.cssselect("#resultItems li"):
            logger.error("multi-item row layout detected, for which listing urls extraction logic is not implemented, target: {}".format(
                target))

    return output


@read_html
def extract_listing(tree: HtmlElement, **kwargs) -> dict:
    """ Returns a dictionary of useful info from listing page <tree> """
    try:
        title = tree.cssselect("title")[0].text_content()
    except IndexError:
        title = None

    return {"title": title}
