#!/usr/bin/env python
# -*- coding: utf-8 -*-
from importlib import import_module
import asyncio
import inspect
import logging
import time

import click

from .crawler import put_seed_urls, TargetPriority, scrape_online

logger = logging.getLogger("rc_crawler")


def configure_logging(platform: str) -> None:
    fh = logging.FileHandler("rc_crawler_{}.log".format(platform))
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


def get_extractors(platform_module):
    """ get extractor functions from <platform_module>

        platform_module: python module containing platform-dependent scraping logic

        returns: {page_category: function}
    """
    return {
        n.lstrip("extract_"): f
        for n, f in inspect.getmembers(platform_module, inspect.isfunction)
        if n.startswith("extract_")
    }


async def start_crawler(platform_module, keyword_file, run_timestamp, num_scrapers):
    """ assemble and start all the components of the crawler

        platform_module: python module containing platform-dependent scraping logic
        keyword_file: a file handle for seed keywords
        run_timestamp: UNIX timestamp in seconds
        num_scrapers: parallelism for scraper
    """
    logger.info("starting crawler, run timestamp: {}".format(run_timestamp))
    logger.info("starting {} scrapers...".format(num_scrapers))

    input_queue = asyncio.PriorityQueue()

    scrape = scrape_online(run_timestamp, platform_module.CRAWL_DEVICE_TYPE, platform_module.RATE_LIMIT_PARAMS)(
        get_extractors(platform_module))

    scrapers = asyncio.gather(*[scrape(input_queue) for i in range(num_scrapers)])
    scraping_tasks = asyncio.ensure_future(scrapers)

    logger.info("starting to generate keywords from {}".format(keyword_file.name))

    await put_seed_urls(platform_module.generate_search_url, keyword_file, input_queue)

    logger.info("putting stoppers in queue for results scrapers...")

    for _ in range(num_scrapers):
        # put lower priority, so that urls are processed first
        await input_queue.put((TargetPriority.STOPPER.value, None))

    logger.info("waiting for scrapers to complete all tasks...")

    await scraping_tasks

    logger.info("exiting crawler...")


@click.command()
@click.argument("platform")
@click.argument("keyword_file", type=click.File('r'))
@click.option("--run-timestamp", type=int, default=lambda: int(time.time()), help="Timestamp to mark this crawl")
@click.option("--num-scrapers", type=int, default=1, help="Number of scrapers")
def main(platform, *args, **kwargs):
    """ Start crawler to mine for product data off eCommerce platform. """
    configure_logging(platform)
    platform_module = import_module('.' + platform, package="rc_crawler")

    crawler = start_crawler(platform_module, *args, **kwargs)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(crawler)
    loop.close()


if __name__ == "__main__":
    main()
