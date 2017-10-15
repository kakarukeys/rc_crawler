#!/usr/bin/env python
# -*- coding: utf-8 -*-
from importlib import import_module
import asyncio
import inspect
import logging
import time

import click

from .browser import back_by_storage, limit_actions
from .crawler import put_seed_urls, TargetPriority, Scraper


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
    logger = logging.getLogger("rc_crawler")

    logger.info("starting crawler, run timestamp: {}".format(run_timestamp))
    logger.info("starting {} scrapers...".format(num_scrapers))

    scrapers = [Scraper(
        run_timestamp,
        platform_module.CRAWL_DEVICE_TYPE,
        get_extractors(platform_module),
        middlewares=[limit_actions(platform_module.RATE_LIMIT_PARAMS), back_by_storage(run_timestamp)],
        captcha_solver_config=getattr(platform_module, "CAPTCHA_SOLVER_CONFIG", None)
    ) for i in range(num_scrapers)]

    scraping_tasks = asyncio.ensure_future(asyncio.gather(*[sc.start() for sc in scrapers]))

    logger.info("starting to generate seed urls{}...".format(
        " from keyword file {} ".format(keyword_file.name) if keyword_file else ''))

    await put_seed_urls(scrapers, platform_module.generate_search_url, keyword_file)

    logger.info("putting stoppers in queue for scrapers...")

    for sc in scrapers:
        # put lower priority, so that urls are processed first
        await sc.send((TargetPriority.STOPPER.value, None))

    logger.info("waiting for scrapers to complete all tasks...")

    await scraping_tasks

    logger.info("exiting crawler...")


@click.command()
@click.argument("platform")
@click.option("--keyword-file", type=click.File('r'), help="File containing seed keywords")
@click.option("--run-timestamp", type=int, default=lambda: int(time.time()), help="Timestamp to mark this crawl")
@click.option("--num-scrapers", type=int, default=1, help="Number of scrapers")
def main(platform: str, *args, **kwargs) -> None:
    """ Start crawler to mine for product data off eCommerce platform. """
    configure_logging(platform)
    platform_module = import_module('.platforms.' + platform, package="rc_crawler")

    crawler = start_crawler(platform_module, *args, **kwargs)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(crawler)
    loop.close()


if __name__ == "__main__":
    main()
