from enum import Enum
from typing import NamedTuple
import logging
import random
import time

import aiohttp
import ujson

from .persist import save_page
from .rate_limiter import AsyncLeakyBucket
from .user_agents import USER_AGENTS

leaky_bucket = AsyncLeakyBucket(max_rate=2, time_period=5)    # 2 reqs / 5 secs


HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'en-US,en;q=0.8,zh-TW;q=0.6,zh;q=0.4',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
}

RETRY_MAX = 1
RETRY_STATUS_CODES = {500, 502, 503, 504, 408}


# message class for scraper coroutines
class Target(NamedTuple):
    url: str
    referer: str
    retry_count: int = 0
    follow_next_count: int = 0
    data: dict = {}


class TargetPriority(Enum):
    DEFAULT = 0
    RETRY = 1
    STOPPER = 20


async def seed_search_urls(generate_search_url, keyword_file, search_url_queue):
    """ put seed search urls in queue

    :param generate_search_url: function: keyword -> url, referer
    :param keyword_file: opened file handle
    :param search_url_queue: processing queue
    """
    logger = logging.getLogger("rc_crawler.seed_search_urls")

    for line in keyword_file:
        keyword = line.strip()

        if keyword:
            logger.debug("keyword: {}".format(keyword))
            url, referer = generate_search_url(keyword)
            await search_url_queue.put((TargetPriority.DEFAULT.value, Target(url=url, referer=referer, data={"keyword": keyword})))


async def fetch(session, url, device_type, extra_headers={}):
    """ fetch html content from url

    :param session: aiohttp client session
    :param url: url to request
    :param device_type: pose as desktop/tablet/mobile browser?
    :param extra_headers: dictionary {name: value}

    :rtype: a dictionary containing outcome and html.
    """
    logger = logging.getLogger("rc_crawler.fetch")

    await leaky_bucket.acquire(amount=random.random() + 1)    # rate limiting to avoid detection

    headers = {"User-Agent": USER_AGENTS[device_type][0]}
    headers.update(extra_headers)
    logger.debug("sending request to {0} with extra headers {1}".format(url, headers))

    try:
        async with session.get(url, headers=headers) as response:
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


def scrape_online(device_type):
    """ decorator for html processing coroutine so that its html input is fetched live from a url

    :param process_html_coro: a function that takes the following arguments
        :param target: Target object the html content was fetched from
        :param html: html content
        :param input_queue: an asyncio queue as an inbox for the coroutine
        and other arguments

    :rtype: a new coroutine that takes only input_queue and other arguments
    """
    def decorator(process_html_coro):
        async def scrape_coro(input_queue, *args, **kwargs):
            coro_id = str(hex(id(locals())))[-6:]   # for logging use only, may not be unique
            logger = logging.getLogger("rc_crawler.scrape_online.{}".format(coro_id))

            async with aiohttp.ClientSession(headers=HEADERS, json_serialize=ujson.dumps) as session:
                logger.info("starting aiohttp client session with headers {}".format(HEADERS))

                while True:
                    _, target = await input_queue.get()

                    if target is None:
                        logger.info("exiting scraper coroutine...")
                        break

                    logger.debug("fetching content from {0} referring from {1}{2}".format(
                        target.url, target.referer, ", retrying" if target.retry_count else ''))

                    result = await fetch(session, target.url, device_type, {"Referer": target.referer})

                    if result["outcome"] == "retry" and target.retry_count < RETRY_MAX:
                        logger.warning("fetch failed, scheduling for retry: {}".format(target.url))
                        await input_queue.put((TargetPriority.RETRY.value, Target(
                            url=target.url,
                            referer=target.referer,
                            retry_count=target.retry_count + 1,
                            follow_next_count=target.follow_next_count,
                            data=target.data
                        )))
                    elif result["outcome"] == "success":
                        logger.debug("fetch succeeded, extracting from html content: {}".format(target.url))
                        await process_html_coro(target, result["html"], input_queue, *args, **kwargs)

        return scrape_coro
    return decorator


def propagate_crawl(extract):
    """ decorator for extractor function so that its output is fed back into queues for further crawls

    :param extract: function: target, html -> {"total_listings": ..., "next_url": ..., "listing_urls":...}

    :rtype: a new coroutine that takes the following arguments
        :param target: Target object the html content was fetched from
        :param html: html content
        :param search_url_queue: an asyncio queue for putting search urls
        :param listing_url_queue: an asyncio queue for putting listing urls
        and other arguments
    """
    async def process_html_coro(target, html, search_url_queue, listing_url_queue, *args, **kwargs):
        logger = logging.getLogger("rc_crawler.propagate_crawl")

        output = extract(target, html)

        total_listings = output["total_listings"]

        if total_listings is None:
            logger.error("could not extract total listings, target: {0}, html: {1}".format(target, html))

        if "next_url" in output:
            next_url = output["next_url"]

            if next_url:
                await search_url_queue.put((TargetPriority.DEFAULT.value, Target(
                    url=next_url, referer=target.url, follow_next_count=target.follow_next_count + 1), data=target.data
                ))
            else:
                logger.error("could not extract next url, target: {0}, html: {1}".format(target, html))

        listing_urls = output["listing_urls"]

        if listing_urls:
            data = {"total_listings": total_listings}
            data.update(target.data)

            for l_url in listing_urls:
                await listing_url_queue.put((TargetPriority.DEFAULT.value, Target(
                    url=l_url, referer=target.url, data=data
                )))
        else:
            logger.error("could not extract listing urls, target: {0}, html: {1}".format(target, html))

    return process_html_coro


def persist_harvest(extract):
    """ decorator for extractor function so that its output is persisted to database

    :param extract: function: target, html -> {key: value}

    :rtype: a new coroutine that takes the following arguments
        :param target: Target object the html content was fetched from
        :param html: html content
        :param listing_url_queue: an asyncio queue for putting listing urls
        :param run_timestamp: UNIX timestamp where the crawl started
        and other arguments
    """
    async def persist_output_coro(target, html, listing_url_queue, run_timestamp, *args, **kwargs):
        logger = logging.getLogger("rc_crawler.persist_harvest")

        await save_page(run_timestamp, target, html)

        output = extract(target, html)
        output["timestamp"] = run_timestamp
        output.update(target.data)

        logger.debug("output persisted: {}".format(output))

    return persist_output_coro
