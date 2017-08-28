#!/usr/bin/env python
# -*- coding: utf-8 -*-
from importlib import import_module
import asyncio
import logging
import time

import click

from .crawler import seed_search_urls, TargetPriority, scrape_online, propagate_crawl, persist_harvest


def configure_logging(platform):
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


async def start_crawler(platform_module, keyword_file, num_results_scraper, num_listing_scraper):
    """ assemble and start all the components of the crawler

    :param platform_module: python module containing platform-dependent scraping logic
    :param keyword_file: a file handle for seed keywords
    :param num_results_scraper: parallelism for search results scraper
    :param num_listing_scraper: parallelism for listing scraper
    """
    logger = logging.getLogger("rc_crawler")

    run_timestamp = int(time.time())

    logger.info("starting crawler, run timestamp: {}".format(run_timestamp))
    logger.info("starting {} listing scrapers...".format(num_listing_scraper))

    listing_url_queue = asyncio.PriorityQueue()

    scrape_listings = scrape_online(platform_module.CRAWL_DEVICE_TYPE)(
        persist_harvest(platform_module.extract_listing)
    )
    listing_scrapers = asyncio.gather(
        *[scrape_listings(listing_url_queue, run_timestamp) for i in range(num_listing_scraper)]
    )
    listing_scraping_tasks = asyncio.ensure_future(listing_scrapers)

    logger.info("starting {} search results scrapers...".format(num_results_scraper))

    search_url_queue = asyncio.PriorityQueue()

    scrape_search_results = scrape_online(platform_module.CRAWL_DEVICE_TYPE)(
        propagate_crawl(platform_module.extract_search_results)
    )
    search_results_scrapers = asyncio.gather(
        *[scrape_search_results(search_url_queue, listing_url_queue) for i in range(num_results_scraper)]
    )
    search_results_scraping_tasks = asyncio.ensure_future(search_results_scrapers)

    logger.info("starting to generate keywords from {}".format(keyword_file.name))

    await seed_search_urls(platform_module.generate_search_url, keyword_file, search_url_queue)

    logger.info("putting stoppers in queue for results scrapers...")

    for i in range(num_results_scraper):
        # put lower priority, so that urls are processed first
        await search_url_queue.put((TargetPriority.STOPPER.value, None))

    logger.info("waiting for results scrapers to complete all tasks...")

    await search_results_scraping_tasks

    logger.info("putting stoppers in queue for listing scrapers...")

    for i in range(num_listing_scraper):
        await listing_url_queue.put((TargetPriority.STOPPER.value, None))

    logger.info("waiting for listing scrapers to complete all tasks...")

    await listing_scraping_tasks

    logger.info("exiting crawler...")


@click.command()
@click.argument("platform")
@click.argument("keyword_file", type=click.File('r'))
@click.option("--num-results-scraper", default=1, help="Number of search results page scrapers")
@click.option("--num-listing-scraper", default=10, help="Number of listing page scrapers")
def main(platform, keyword_file, num_results_scraper, num_listing_scraper):
    """ Start crawler to mine for product data off eCommerce platform. """
    configure_logging(platform)
    platform_module = import_module('.' + platform, package="rc_crawler")

    crawler = start_crawler(platform_module, keyword_file, num_results_scraper, num_listing_scraper)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(crawler)
    loop.close()


if __name__ == "__main__":
    main()
