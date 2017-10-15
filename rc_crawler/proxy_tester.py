#!/usr/bin/env python
# -*- coding: utf-8 -*-
import asyncio
import logging

import aiohttp
import async_timeout
import click

from .browser import HEADERS, USER_AGENTS

fh = logging.FileHandler("proxy_tester.log")
fh.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)

logger = logging.getLogger("proxy_tester")
logger.setLevel(logging.DEBUG)
logger.addHandler(fh)

logger2 = logging.getLogger("rc_crawler")
logger2.setLevel(logging.DEBUG)
logger2.addHandler(fh)


NUM_THREADS = 80
HEADERS["User-Agent"] = USER_AGENTS["desktop"][0]

TEST_URLS = [
    "http://www.1688.com",
    "http://www.alibaba.com",
    "http://www.aliexpress.com",
    "http://www.amazon.ca",
    "http://www.amazon.co.uk",
    "http://www.amazon.com",
    "http://www.amazon.de",
    "http://www.amazon.fr",
    "http://www.amazon.it",
    "http://www.ebay.com.sg",
    "http://www.flipkart.com",
    "http://www.jd.com",
]

MAX_ERROR_COUNT = int(len(TEST_URLS) / 6)


async def dump_proxy_in_queue(proxy_list, input_queue):
    seen = set()

    for line in proxy_list:
        ip = line.strip()

        if ip and ip not in seen:
            seen.add(ip)

            logger.debug("IP: {}".format(ip))
            await input_queue.put("http://" + ip)


async def test_connect(input_queue):
    while True:
        proxy = await input_queue.get()

        if proxy is None:
            break

        logger.info("starting aiohttp client session...")

        async with aiohttp.ClientSession(headers=HEADERS) as session:
            error_count = 0

            for url in TEST_URLS:
                logger.info("sending request to {0} with proxy {1}...".format(url, proxy))

                try:
                    with async_timeout.timeout(20):
                        async with session.get(url, proxy=proxy) as response:
                            await response.read()

                            if response.status == 200:
                                logger.info("proxy: {0}, url: {1}, success".format(proxy, url))
                            else:
                                logger.warning("proxy: {0}, url: {1}, issue: {2}".format(proxy, url, response.status))
                                error_count += 1

                except Exception as e:
                    logger.warning("proxy: {0}, url: {1}, error: {2} because of {3}, {4}".format(
                        proxy, url, type(e).__name__, type(e.__cause__).__name__, str(e)))
                    error_count += 1

            if error_count <= MAX_ERROR_COUNT:
                logger.info("{0} is good".format(proxy))
                print(proxy)
            else:
                logger.warning("{0} is bad".format(proxy))

    logger.info("exiting tester...")


async def start_proxy_tester(proxy_list):
    """ assemble and start all the components of the proxy tester
        proxy_list: a file handle containing proxy IPs
    """
    logger.info("starting proxy tester...")

    input_queue = asyncio.Queue()
    testers = [test_connect(input_queue) for i in range(NUM_THREADS)]
    tasks = asyncio.ensure_future(asyncio.gather(*testers))

    logger.info("starting to dump proxy server URL to queue...")
    await dump_proxy_in_queue(proxy_list, input_queue)

    logger.info("putting stoppers in queue for testers...")

    for _ in range(NUM_THREADS):
        await input_queue.put(None)

    logger.info("waiting for testers to complete all tasks...")
    await tasks

    logger.info("exiting proxy tester...")


@click.command()
@click.argument("proxy_list", type=click.File('r'))
def main(proxy_list):
    """ test whether a proxy is functional """
    proxy_tester = start_proxy_tester(proxy_list)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(proxy_tester)
    loop.close()


if __name__ == "__main__":
    main()
