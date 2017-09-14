from enum import Enum
from itertools import cycle
from typing import NamedTuple
import asyncio
import logging

import aiohttp
import ujson

from .exceptions import AntiScrapingError
from .persist import back_by_storage
from .rate_limiter import limit_actions
from .agents import renew_agent
from .utils import describe_exception


HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'en-US,en;q=0.5',
    'Connection': 'keep-alive',
}

RETRY_MAX = 2
PROXY_FAILURE_COUNT_MAX = 2
RETRY_STATUS_CODES = {408, 500, 502, 503, 504}

# the urls we follow should not have produced these codes, when not using proxy
PROXY_ERROR_STATUS_CODES = {403, 404, 407, 515}


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


class FetchOutcome(Enum):
    SUCCESS = "success"
    FAILURE = "failure"

    # requires retry:
    RETRY = "retry"
    ANTI_SCRAPING = "anti_scraping"
    PROXY_FAILURE = "proxy_failure"
    MAYBE_PROXY_FAILURE = "maybe_proxy_failure"


async def fetch(session: aiohttp.ClientSession, url: str, extra_headers: dict={}, proxy: str=None) -> None:
    """ fetch html content fromR <url>
        returns {"outcome": ..., (optional) "html": ..., (optional) "reason": "reason for failure"}
    """
    logger = logging.getLogger("rc_crawler.fetch")

    logger.debug("sending request to {0} with extra headers {1}, proxy: {2}".format(
        url, extra_headers, proxy))

    try:
        async with session.get(url, headers=extra_headers, proxy=proxy) as response:
            html = await response.text()

            if response.status == 200:
                result = {"outcome": FetchOutcome.SUCCESS, "html": html}
            else:
                logger.error("non-200 response, url: {0}, request headers: {1}, status: {2}, html: {3}, proxy: {4}".format(
                    url, response.request_info.headers, response.status, html, proxy))

                if response.status in PROXY_ERROR_STATUS_CODES:
                    result = {"outcome": FetchOutcome.PROXY_FAILURE, "reason": response.status}
                elif response.status in RETRY_STATUS_CODES:
                    result = {"outcome": FetchOutcome.RETRY, "reason": response.status}
                else:
                    result = {"outcome": FetchOutcome.FAILURE, "reason": response.status}

    except aiohttp.ClientHttpProxyError as e:
        # subclass of ClientResponseError
        result = {"outcome": FetchOutcome.PROXY_FAILURE, "reason": describe_exception(e)}

    except (aiohttp.ClientPayloadError, aiohttp.ClientResponseError) as e:
        result = {"outcome": FetchOutcome.RETRY, "reason": describe_exception(e)}

    except aiohttp.ClientConnectorError as e:
        # subclass of ClientConnectionError
        if isinstance(e.__cause__, ConnectionResetError):
            result = {"outcome": FetchOutcome.RETRY, "reason": describe_exception(e)}
        else:
            # includes ClientProxyConnectionError as cause
            result = {"outcome": FetchOutcome.MAYBE_PROXY_FAILURE, "reason": describe_exception(e)}

    except (asyncio.TimeoutError, aiohttp.ClientConnectionError) as e:
        # includes ServerDisconnectedError, ClientOSError
        result = {"outcome": FetchOutcome.MAYBE_PROXY_FAILURE, "reason": describe_exception(e)}

    return result


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
        logger.debug("target: {0}, next_url: {1}".format(target, next_url))

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


class Scraper:
    """ Actor to perform complete web scraping

        run_timestamp: UNIX timestamp when the crawl started

        (platform-dependent arguments)
        device_type: pose as desktop/tablet/mobile browser?
        rate_limit_params: [{max_rate: ..., time_period: ...}, ...]
        extractors: {page_category: function extract_<page_category>: html -> {key: value}}
    """
    def __init__(self, run_timestamp, device_type, rate_limit_params, extractors):
        actor_id = str(hex(id(self)))[-6:]   # for logging use only, may not be unique
        self.logger = logging.getLogger("rc_crawler.scrape.{}".format(actor_id))

        self.input_queue = asyncio.PriorityQueue()  # actor inbox

        self.run_timestamp = run_timestamp
        self.device_type = device_type
        self.extractors = extractors

        # install middlewares
        self.download = back_by_storage(run_timestamp)(
            limit_actions(rate_limit_params)(
                fetch))

    async def send(self, *args, **kwargs):
        await self.input_queue.put(*args, **kwargs)

    async def on_receive(self, target, session, user_agent, proxy=None):
        self.logger.debug("downloading content from {0} url {1}, keywords: {2}{3}".format(
            target.category, target.url, target.keyword, ", retrying" if target.retry_count else ''))

        extra_headers = {"Referer": target.referer, "User-Agent": user_agent}
        result = await self.download(session, target.url, extra_headers=extra_headers, proxy=proxy)

        if result["outcome"] == FetchOutcome.SUCCESS:
            try:
                output = self.extractors[target.category](target, result["html"])
            except AntiScrapingError as e:
                result = {"outcome": FetchOutcome.ANTI_SCRAPING, "reason": describe_exception(e)}
            else:
                self.logger.debug("download succeeded, harvesting from html content: {}".format(target.url))
                await harvest(output, target, self.input_queue, self.run_timestamp)

        elif result["outcome"] == FetchOutcome.FAILURE:
            self.logger.error("download failed: {0}, please analyze: {1}, skip to next one".format(target.url, result["reason"]))

        # check if retry is required
        if result["outcome"] not in (FetchOutcome.SUCCESS, FetchOutcome.FAILURE):
            if target.retry_count < RETRY_MAX:
                self.logger.warning("download failed because of {0}, scheduling for retry: {1}".format(
                    result["reason"], target.url))

                await self.input_queue.put((TargetPriority.RETRY.value, Target(
                    keyword=target.keyword,
                    url=target.url,
                    referer=target.referer,
                    category=target.category,
                    retry_count=target.retry_count + 1,
                    follow_next_count=target.follow_next_count
                )))

            else:
                self.logger.warning("download failed because of {0}, retried max number of times: {1}".format(
                    result["reason"], target.url))

        return result

    async def start(self):
        user_agent, proxy = renew_agent(self.device_type)
        proxy_failure_count = 0

        self.logger.info("starting aiohttp client session with headers {0}, user agent {1} and proxy {2}".format(
            HEADERS, user_agent, proxy))

        async with aiohttp.ClientSession(headers=HEADERS, json_serialize=ujson.dumps) as session:
            while True:
                _, target = await self.input_queue.get()

                if target is None:
                    break

                result = await self.on_receive(target, session, user_agent, proxy)

                if result["outcome"] == FetchOutcome.MAYBE_PROXY_FAILURE and proxy_failure_count < PROXY_FAILURE_COUNT_MAX:
                    proxy_failure_count += 1

                elif result["outcome"] in (FetchOutcome.ANTI_SCRAPING, FetchOutcome.PROXY_FAILURE, FetchOutcome.MAYBE_PROXY_FAILURE):
                    self.logger.warning("{0}, changing agent from {1}, {2},".format(result["outcome"], user_agent, proxy))

                    user_agent, proxy = renew_agent(self.device_type)
                    session.cookie_jar.clear()
                    proxy_failure_count = 0

                    self.logger.warning("to {0}, {1}...".format(user_agent, proxy))

            self.logger.info("exiting scraper...")


async def put_seed_urls(generate_search_url, keyword_file, scrapers):
    """ send seed search urls to scrapers in cycle

        generate_search_url: function: keyword -> url, referer
        keyword_file: opened file handle
        scrapers: scraper actors
    """
    logger = logging.getLogger("rc_crawler.put_seed_urls")

    scrapers_in_cycle = cycle(scrapers)

    for line in keyword_file:
        keyword = line.strip()

        if keyword:
            logger.debug("keyword: {}".format(keyword))
            url, referer = generate_search_url(keyword)

            await next(scrapers_in_cycle).send((
                TargetPriority.DEFAULT.value,
                Target(keyword=keyword, url=url, referer=referer, category=PageCategory.SEARCH.value)
            ))
