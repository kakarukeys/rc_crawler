from datetime import datetime
from typing import Tuple
import logging
import re

from lxml.html import document_fromstring
import dateparser

logger = logging.getLogger("rc_crawler.bing")


BASE_URL = "http://www.bing.com"
SEARCH_URL_TEMPLATE = "/videos/search?q={}&FORM=BVLH1"

CRAWL_DEVICE_TYPE = "desktop"

# reqs / secs
RATE_LIMIT_PARAMS = [{"max_rate": 2, "time_period": 5}]

VIDEO_INFO_RE = re.compile(r"(?P<title>[^·]+) from [^·]+|(?P<views>[\d,]+)\+? views|uploaded on (?P<uploaded_on>[^·]+)")


def generate_search_url(keyword: str) -> Tuple[str, str]:
    """ Returns search url, referer url """
    return BASE_URL + SEARCH_URL_TEMPLATE.format(keyword.replace(' ', '+')), BASE_URL + '/'


def parse_video_info_string(video_info_str, relative_base=False):
    """ Parses <video_info_str> assuming now is <relative_base>

        relative_base: datetime, defaults to datetime now.

        returns title, views, uploaded_on in a dictionary.
        value is None, if missing or can't parse.
    """
    fragments = video_info_str.split('·')

    result = {"title": None, "views": None, "uploaded_on": None}

    for s in fragments:
        m = VIDEO_INFO_RE.match(s.strip())

        if m:
            result.update({k: v for k, v in m.groupdict().items() if v})

    if result["views"]:
        try:
            result["views"] = int(result["views"].replace(',', ''))
        except ValueError:
            result["views"] = None

    if result["uploaded_on"]:
        try:
            result["uploaded_on"] = datetime.strptime(result["uploaded_on"], "%d/%m/%Y")
        except ValueError:  # try intelligent parser
            result["uploaded_on"] = dateparser.parse(result["uploaded_on"], settings={"RELATIVE_BASE": relative_base})

    return result


def extract_search_results(html: str, run_timestamp: int, **kwargs) -> dict:
    """ Returns a dictionary of useful info from search results <html> """
    tree = document_fromstring(html)

    videos_info = []

    try:
        videos_info_strings = [el.attrib["aria-label"] for el in tree.cssselect("a.dv_i")]
    except KeyError:
        pass
    else:
        for s in videos_info_strings:
            result = parse_video_info_string(s, relative_base=datetime.utcfromtimestamp(run_timestamp))

            if result["views"] and result["uploaded_on"]:
                videos_info.append(result)
            else:
                logger.warning("incomplete parsing of video info: {}".format(s))

                if not result["views"] and " views" in s:
                    logger.error("regex unable to extract view count from video info: {}".format(s))

                if not result["uploaded_on"] and "uploaded on" in s:
                    logger.error("regex unable to extract uploaded_on from video info: {}".format(s))

    return {"videos_info": videos_info}
