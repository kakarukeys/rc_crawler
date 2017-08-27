#!/usr/bin/env python
# -*- coding: utf-8 -*-
from collections import namedtuple
import aiohttp
import asyncio
import logging
import random
import time

import click
import ujson

fh = logging.FileHandler("rc_crawler.log")
fh.setLevel(logging.DEBUG)

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)

logger = logging.getLogger("rc_crawler")
logger.setLevel(logging.DEBUG)

logger.addHandler(fh)
logger.addHandler(ch)


AMAZON_SEARCH_URL_TEMPLATE = "https://www.amazon.com/s/ref=nb_sb_noss_1?url=search-alias%3Daps&field-keywords={}"

HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'en-US,en;q=0.8,zh-TW;q=0.6,zh;q=0.4',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Linux; Android 5.0.2; Mi 4i Build/LRX22G) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.125 Mobile Safari/537.36',
}

RETRY_MAX = 1


# message class for scraper coroutines
Target = namedtuple("Target", ["retry_count", "url", "referer"])


def generate_search_url(keyword):
    return AMAZON_SEARCH_URL_TEMPLATE.format(keyword.replace(' ', '+'))


async def seed_search_urls(keyword_file, search_url_queue):
    """ put seed search urls in queue

    :param keyword_file: opened file handle
    :param search_url_queue: processing queue
    """
    for line in keyword_file:
        keyword = line.strip()

        if keyword:
            logger.debug("keyword: {}".format(keyword))
            url = generate_search_url(keyword)
            await search_url_queue.put(Target(retry_count=0, url=url, referer="https://www.amazon.com/"))


async def fetch(session, url, extra_headers=None):
    """ fetch html content from url

    :param session: aiohttp client session
    :param url: url to request

    :rtype: a dictionary containing outcome and html.
    """
    logger = logging.getLogger("rc_crawler.fetch")

    rest_duration = random.random() + 1
    logger.debug("artificially sleep for {} seconds to avoid detection".format(rest_duration))
    time.sleep(rest_duration)

    logger.debug("sending request to {0} with extra headers {1}".format(url, extra_headers))

    try:
        async with session.get(url, headers=extra_headers) as response:
            html = await response.text()

            rest_duration = random.random() + 1
            logger.debug("artificially sleep for {} seconds to avoid detection".format(rest_duration))
            time.sleep(rest_duration)

            if response.status == 200:
                return {"outcome": "success", "html": html}
            else:
                logger.error("non-200 response, url: {0}, request headers: {1}, status: {2}, html: {3}".format(
                    url, response.request_info.headers, response.status, html))
                return {"outcome": "failure"}
    except (aiohttp.ClientConnectionError, aiohttp.ClientPayloadError) as e:
        logger.warning("need to retry fetch due to aiohttp exception, url: {0}".format(url))
        logger.exception(e)
        return {"outcome": "retry"}
    except aiohttp.ClientResponseError as e:
        logger.error("fetch failed due to aiohttp exception, url: {0}".format(url))
        logger.exception(e)
        return {"outcome": "failure"}


def scrape_online(extract_coro):
    """ decorator for extractor coroutine so that its html input is fetched live from a url

    :param extract_coro: a function that takes the following arguments
        :param url: url the html content was fetched from
        :param html: html content
        :param input_queue: an asyncio queue as an inbox for the coroutine
        and other arguments

    :rtype: a new coroutine that takes only input_queue and other arguments
    """
    async def scrape_coro(input_queue, *args, **kwargs):
        coro_id = str(hex(id(locals())))[-6:]   # for logging use only, may not be unique
        logger = logging.getLogger("rc_crawler.scrape_online.{}".format(coro_id))

        async with aiohttp.ClientSession(headers=HEADERS, json_serialize=ujson.dumps) as session:
            logger.info("starting aiohttp client session with headers {}".format(HEADERS))

            while True:
                target = await input_queue.get()

                if target is None:
                    logger.info("exiting scraper coroutine...")
                    break

                logger.debug("fetching content from {0} referring from {1}{2}".format(
                    target.url, target.referer, ", retrying" if target.retry_count else ''))

                result = await fetch(session, target.url, {"Referer": target.referer})

                if result["outcome"] == "retry" and target.retry_count < RETRY_MAX:
                    logger.warning("fetch failed, scheduling for retry: {}".format(target.url))
                    await input_queue.put(Target(retry_count=target.retry_count + 1, url=target.url, referer=target.referer))
                elif result["outcome"] == "success":
                    logger.debug("fetch succeeded, extracting from html content: {}".format(target.url))
                    await extract_coro(target.url, result["html"], input_queue, *args, **kwargs)

    return scrape_coro


@scrape_online
async def extract_search_results(url, html, search_url_queue, listing_url_queue):
    print(url)
    print(html[:100])
    print("---")
    await listing_url_queue.put(Target(retry_count=0, url="https://www.dotdash.com?ref={}".format(url), referer=url))


@scrape_online
async def extract_listing(url, html, listing_url_queue):
    print(url)
    print(html[:100])
    print("---")


async def start_crawler(keyword_file, num_results_scraper, num_listing_scraper):
    """ start all the components of the crawler

    :param keyword_file: a file handle for seed keywords
    :param num_results_scraper: parallelism for search results scraper
    :param num_listing_scraper: parallelism for listing scraper
    """
    logger.info("starting {} listing scrapers...".format(num_listing_scraper))

    listing_url_queue = asyncio.Queue()
    listing_scrapers = asyncio.gather(
        *[extract_listing(listing_url_queue) for i in range(num_listing_scraper)]
    )
    listing_scraping_tasks = asyncio.ensure_future(listing_scrapers)

    logger.info("starting {} search results scrapers...".format(num_results_scraper))

    search_url_queue = asyncio.Queue()
    search_results_scrapers = asyncio.gather(
        *[extract_search_results(search_url_queue, listing_url_queue) for i in range(num_results_scraper)]
    )
    search_results_scraping_tasks = asyncio.ensure_future(search_results_scrapers)

    logger.info("starting to generate keywords from {}".format(keyword_file.name))

    await seed_search_urls(keyword_file, search_url_queue)

    logger.info("putting stoppers in queue for results scrapers...")

    for i in range(num_results_scraper):
        await search_url_queue.put(None)

    logger.info("waiting for results scrapers to complete all tasks...")

    await search_results_scraping_tasks

    logger.info("putting stoppers in queue for listing scrapers...")

    for i in range(num_listing_scraper):
        await listing_url_queue.put(None)

    logger.info("waiting for listing scrapers to complete all tasks...")

    await listing_scraping_tasks

    logger.info("exiting crawler...")


@click.command()
@click.argument("keyword_file", type=click.File('r'))
@click.option("--num-results-scraper", default=1, help="Number of search results page scrapers")
@click.option("--num-listing-scraper", default=10, help="Number of listing page scrapers")
def main(keyword_file, num_results_scraper, num_listing_scraper):
    """ Start crawler to mine for product data off Amazon platform. """
    crawler = start_crawler(keyword_file, num_results_scraper, num_listing_scraper)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(crawler)
    loop.close()


if __name__ == "__main__":
    main()
