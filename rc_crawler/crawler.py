from enum import Enum
from itertools import cycle
from typing import NamedTuple
import asyncio
import logging

import aiohttp
import ujson

from .exceptions import AntiScrapingError, ProxyError
from .persist import back_by_storage
from .rate_limiter import limit_actions
from .agents import renew_agent


HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'en-US,en;q=0.5',
    'Connection': 'keep-alive',
}

RETRY_MAX = 1
RETRY_STATUS_CODES = {500, 502, 503, 504, 408}
PROXY_ERROR_STATUS_CODES = {407, 515}


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


async def fetch(session: aiohttp.ClientSession, url: str, extra_headers: dict={}, proxy: str=None) -> None:
    """ fetch html content from <url>
        returns {"outcome": ..., (optional) "html": ...}
    """
    logger = logging.getLogger("rc_crawler.fetch")

    logger.debug("sending request to {0} with extra headers {1}".format(url, extra_headers))

    try:
        async with session.get(url, headers=extra_headers, proxy=proxy) as response:
            html = await response.text()

            if response.status == 200:
                return {"outcome": "success", "html": html}
            else:
                logger.error("non-200 response, url: {0}, request headers: {1}, status: {2}, html: {3}, proxy: {4}".format(
                    url, response.request_info.headers, response.status, html, proxy))

                if response.status in PROXY_ERROR_STATUS_CODES:
                    raise ProxyError("fetch failed due to proxy problem, url: {0}, proxy: {1}".format(url, proxy))
                elif response.status in RETRY_STATUS_CODES:
                    return {"outcome": "retry"}
                else:
                    return {"outcome": "failure"}

    except asyncio.TimeoutError:
        if proxy:
            raise ProxyError("fetch failed due to proxy problem, url: {0}, proxy: {1}".format(url, proxy))
        else:
            return {"outcome": "retry"}

    except (aiohttp.ClientConnectionError, aiohttp.ClientPayloadError) as e:
        if isinstance(e.__cause__, aiohttp.ClientProxyConnectionError):
            raise ProxyError("fetch failed due to proxy problem, url: {0}, proxy: {1}".format(url, proxy)) from e

        logger.warning("need to retry fetch due to aiohttp exception, url: {0}, proxy: {1}".format(url, proxy))
        logger.exception(e)
        return {"outcome": "retry"}

    except aiohttp.ClientHttpProxyError as e:
        raise ProxyError("fetch failed due to proxy problem, url: {0}, proxy: {1}".format(url, proxy)) from e

    except aiohttp.ClientResponseError as e:
        logger.error("fetch failed due to aiohttp exception, url: {0}, proxy: {1}".format(url, proxy))
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
        self.logger.debug("downloading content from {0} url {1}, keywords: {2}, referring from {3}{4}".format(
            target.category, target.url, target.keyword, target.referer, ", retrying" if target.retry_count else ''))

        extra_headers = {"Referer": target.referer, "User-Agent": user_agent}
        result = await self.download(session, target.url, extra_headers=extra_headers, proxy=proxy)

        if result["outcome"] == "retry" and target.retry_count < RETRY_MAX:
            self.logger.warning("download failed, scheduling for retry: {}".format(target.url))

            await self.input_queue.put((TargetPriority.RETRY.value, Target(
                keyword=target.keyword,
                url=target.url,
                referer=target.referer,
                category=target.category,
                retry_count=target.retry_count + 1,
                follow_next_count=target.follow_next_count
            )))

        elif result["outcome"] == "success":
            self.logger.debug("download succeeded, harvesting from html content: {}".format(target.url))

            output = self.extractors[target.category](target, result["html"])
            await harvest(output, target, self.input_queue, self.run_timestamp)

        else:
            self.logger.error("download failed: {}, please analyze, skip to next one".format(target.url))

    async def start(self):
        self.logger.info("starting aiohttp client session with headers {}".format(HEADERS))

        user_agent, proxy = renew_agent(self.device_type)

        async with aiohttp.ClientSession(headers=HEADERS, json_serialize=ujson.dumps) as session:
            while True:
                _, target = await self.input_queue.get()

                if target is None:
                    break

                try:
                    await self.on_receive(target, session, user_agent, proxy)
                except (AntiScrapingError, ProxyError) as e:
                    if isinstance(e, AntiScrapingError):
                        msg = "anti-scraping mechanism triggered"
                    else:
                        msg = "proxy error occurred"

                    self.logger.warning("{}, changing proxy...".format(msg))
                    self.logger.exception(e)

                    user_agent, proxy = renew_agent(self.device_type)
                    session.cookies.clear()

                    await self.input_queue.put((TargetPriority.RETRY.value, target))

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
