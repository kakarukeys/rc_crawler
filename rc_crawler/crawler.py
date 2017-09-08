from enum import Enum
from typing import NamedTuple
import logging

import aiohttp
import ujson

from .persist import back_by_storage
from .rate_limiter import limit_actions
from .user_agents import USER_AGENTS


HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'en-US,en;q=0.5',
    'Connection': 'keep-alive',
}

RETRY_MAX = 1
RETRY_STATUS_CODES = {500, 502, 503, 504, 408}


# message class for scraper coroutines
class Target(NamedTuple):
    keyword: str
    url: str
    referer: str
    category: str = None
    retry_count: int = 0
    follow_next_count: int = 0


class TargetPriority(Enum):
    DEFAULT = 0
    RETRY = 1
    STOPPER = 20


class PageCategory(Enum):
    SEARCH = "search_results"
    LISTING = "listing"


async def put_seed_urls(generate_search_url, keyword_file, input_queue):
    """ put seed search urls in queue

        generate_search_url: function: keyword -> url, referer
        keyword_file: opened file handle
        input_queue: processing queue
    """
    logger = logging.getLogger("rc_crawler.put_seed_urls")

    for line in keyword_file:
        keyword = line.strip()

        if keyword:
            logger.debug("keyword: {}".format(keyword))
            url, referer = generate_search_url(keyword)

            await input_queue.put((
                TargetPriority.DEFAULT.value,
                Target(keyword=keyword, url=url, referer=referer, category=PageCategory.SEARCH.value)
            ))


async def fetch(session: aiohttp.ClientSession, url: str, extra_headers: dict={}) -> None:
    """ fetch html content from <url>
        returns {"outcome": ..., (optional) "html": ...}
    """
    logger = logging.getLogger("rc_crawler.fetch")

    logger.debug("sending request to {0} with extra headers {1}".format(url, extra_headers))

    try:
        async with session.get(url, headers=extra_headers) as response:
            html = await response.text()

            if response.status == 200:
                return {"outcome": "success", "html": html}
            else:
                logger.error("non-200 response, url: {0}, request headers: {1}, status: {2}, html: {3}".format(
                    url, response.request_info.headers, response.status, html))

                if response.status in RETRY_STATUS_CODES:
                    return {"outcome": "retry"}
                else:
                    return {"outcome": "failure"}

    except (aiohttp.ClientConnectionError, aiohttp.ClientPayloadError) as e:
        logger.warning("need to retry fetch due to aiohttp exception, url: {0}".format(url))
        logger.exception(e)
        return {"outcome": "retry"}

    except aiohttp.ClientResponseError as e:
        logger.error("fetch failed due to aiohttp exception, url: {0}".format(url))
        logger.exception(e)
        return {"outcome": "failure"}


async def harvest(output, target, input_queue, run_timestamp):
    """ harvest extracted output from html, follow links and save data
        (function modifies output)
    """
    logger = logging.getLogger("rc_crawler.harvest")

    for key, value in output.items():
        if not value:
            logger.error("could not extract {0} from target: {1}".format(key, target))

    # follow links
    next_url = output.pop("next_url", None)

    if next_url:
        await input_queue.put((TargetPriority.DEFAULT.value, Target(
            url=next_url,
            referer=target.url,
            category=PageCategory.SEARCH.value,
            follow_next_count=target.follow_next_count + 1,
            keyword=target.keyword
        )))

    listing_urls = output.pop("listing_urls", [])

    for l_url in listing_urls:
        await input_queue.put((TargetPriority.DEFAULT.value, Target(
            url=l_url,
            referer=target.url,
            category=PageCategory.LISTING.value,
            keyword=target.keyword
        )))

    # save data
    output["timestamp"] = run_timestamp
    output["keyword"] = target.keyword
    output["url"] = target.url
    logger.debug("output persisted: {}".format(output))


def scrape_online(run_timestamp, device_type, rate_limit_params):
    """ decorator for data extraction functions to perform complete web scraping

        run_timestamp: UNIX timestamp when the crawl started

        (platform-dependent arguments)
        device_type: pose as desktop/tablet/mobile browser?
        rate_limit_params: [{max_rate: ..., time_period: ...}, ...]

        (decoratee)
        extractors: {page_category: function extract_<page_category>: html -> {key: value}}

        returns a coroutine that takes the following argument
            input_queue: an asyncio priority queue as an inbox for the coroutine
    """
    # install middlewares
    download = back_by_storage(run_timestamp)(
        limit_actions(rate_limit_params)(
            fetch))

    def decorator(extractors):
        async def scrape_coro(input_queue):
            coro_id = str(hex(id(locals())))[-6:]   # for logging use only, may not be unique
            logger = logging.getLogger("rc_crawler.scrape.{}".format(coro_id))

            user_agent = USER_AGENTS[device_type][0]

            async with aiohttp.ClientSession(headers=HEADERS, json_serialize=ujson.dumps) as session:
                logger.info("starting aiohttp client session with headers {}".format(HEADERS))

                while True:
                    _, target = await input_queue.get()

                    if target is None:
                        logger.info("exiting scraper coroutine...")
                        break

                    logger.debug("downloading content from {0} url {1}, keywords: {2}, referring from {3}{4}".format(
                        target.category, target.url, target.keyword, target.referer, ", retrying" if target.retry_count else ''))

                    extra_headers = {"Referer": target.referer, "User-Agent": user_agent}
                    result = await download(session, target.url, extra_headers)

                    if result["outcome"] == "retry" and target.retry_count < RETRY_MAX:
                        logger.warning("fetch failed, scheduling for retry: {}".format(target.url))

                        await input_queue.put((TargetPriority.RETRY.value, Target(
                            keyword=target.keyword,
                            url=target.url,
                            referer=target.referer,
                            category=target.category,
                            retry_count=target.retry_count + 1,
                            follow_next_count=target.follow_next_count
                        )))

                    elif result["outcome"] == "success":
                        logger.debug("fetch succeeded, harvesting from html content: {}".format(target.url))

                        output = extractors[target.category](result["html"])
                        await harvest(output, target, input_queue, run_timestamp)

        return scrape_coro
    return decorator
