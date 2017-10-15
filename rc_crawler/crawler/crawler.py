from enum import Enum
from itertools import cycle
from typing import NamedTuple, Callable, List, Tuple, Generator, TextIO, Union
import asyncio
import logging

from rc_crawler.browser import Browser, FetchOutcome
from rc_crawler.utils import describe_exception
from .captcha import solve_captcha
from .exceptions import AntiScrapingError


RETRY_MAX = 2
PROXY_FAILURE_COUNT_MAX = 2

# message class for scraper coroutines
class Target(NamedTuple):
    keyword: str
    url: str
    referer: str
    params: Union[dict, List[tuple], str, None] = None
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
        extractors: {page_category: function extract_<page_category>: html, target, run_timestamp -> {key: value}}
        middlewares: a list of decorators for Browser object's fetch method
    """
    def __init__(self, run_timestamp, device_type, extractors, middlewares=[], captcha_solver_config=None):
        actor_id = str(hex(id(self)))[-6:]   # for logging use only, may not be unique
        self.logger = logging.getLogger("rc_crawler.scrape.{}".format(actor_id))

        self.input_queue = asyncio.PriorityQueue()  # actor inbox

        self.run_timestamp = run_timestamp
        self.extractors = extractors
        self.captcha_solver_config = captcha_solver_config

        self.browser = Browser(device_type)
        self.download = self.browser.fetch
        self.middlewares = middlewares

        # install middlewares
        for f in self.middlewares:
            self.download = f(self.download)

    async def send(self, *args, **kwargs):
        await self.input_queue.put(*args, **kwargs)

    async def answer_captcha_challenge(self, challenge, referer):
        extra_headers = {"Referer": referer}
        captcha_image_url = challenge["captcha_image_url"]

        fetch_result = await self.browser.fetch(captcha_image_url, extra_headers=extra_headers, filetype="binary")

        if fetch_result["outcome"] == FetchOutcome.SUCCESS:
            try:
                answer = await solve_captcha(fetch_result["content"], self.captcha_solver_config)
            except ValueError as e:
                challenge_result = {"outcome": "failure", "reason": str(e)}

            if answer:
                submission_form = challenge["submission_form"]

                if submission_form["method"] == "GET":
                    # the input field which has value None is the answer field
                    query_params = {k: v or answer for k, v in submission_form["data"].items()}

                    self.logger.info("submission to captcha challenge {0} is {1}".format(captcha_image_url, query_params))

                    await self.browser.fetch(url=submission_form["action"], params=query_params, extra_headers=extra_headers)

                    challenge_result = {"outcome": "success"}
                else:
                    challenge_result = {"outcome": "failure", "reason": "non-GET request is not yet supported"}
            else:
                challenge_result = {"outcome": "failure", "reason": "unable to recognize characters in captcha"}
        else:
            challenge_result = {"outcome": "failure", "reason": "failed to fetch captcha image due to {}".format(
                fetch_result["reason"])}

        return challenge_result

    async def handle_download_success(self, target, html, from_cache):
        """ extract and harvest <html>, answer any captcha challenge presented """
        try:
            output = self.extractors[target.category](html, target=target, run_timestamp=self.run_timestamp)
        except AntiScrapingError as e:
            amended_result = {
                "outcome": FetchOutcome.ANTI_SCRAPING,
                "reason": describe_exception(e),
                "switch_agent": not from_cache,
            }
        else:
            if "captcha_image_url" in output:
                if from_cache:
                    amended_result = {
                        "outcome": FetchOutcome.ANTI_SCRAPING,
                        "reason": "captcha challenge, from saved page",
                        "switch_agent": False,
                    }
                else:
                    self.logger.info("captcha detected at {}, attempting to answer it...".format(target.url))
                    result = await self.answer_captcha_challenge(challenge=output, referer=target.url)

                    if result["outcome"] == "success":
                        amended_result = {
                            "outcome": FetchOutcome.ANTI_SCRAPING,
                            "reason": "captcha challenge, answered",
                            "switch_agent": False,
                        }
                    else:
                        amended_result = {
                            "outcome": FetchOutcome.ANTI_SCRAPING,
                            "reason": "captcha challenge, unanswered due to {}".format(result["reason"]),
                            "switch_agent": True,
                        }
            else:
                self.logger.debug("extraction succeeded, harvesting from extracted content: {}".format(target.url))
                await harvest(output, target, self.input_queue, self.run_timestamp)
                amended_result = {"outcome": FetchOutcome.SUCCESS}

            return amended_result

    async def on_receive(self, target):
        self.logger.debug("downloading content from {0} url {1}, keywords: {2}{3}".format(
            target.category or '', target.url, target.keyword, ", retrying" if target.retry_count else ''))

        extra_headers = {"Referer": target.referer}

        result = await self.download(
            target.url, params=target.params, read_from_cache=not target.retry_count, extra_headers=extra_headers
        )

        if result["outcome"] == FetchOutcome.SUCCESS and target.category:
            # possibly changing the result upon further scrutiny
            result = await self.handle_download_success(target, result["content"], result["from_cache"])
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
        async with self.browser:
            self.logger.info("starting browser {}".format(self.browser))

            proxy_failure_count = 0

            while True:
                _, target = await self.input_queue.get()

                if target is None:
                    break

                result = await self.on_receive(target)

                if result["outcome"] == FetchOutcome.MAYBE_PROXY_FAILURE and proxy_failure_count < PROXY_FAILURE_COUNT_MAX:
                    proxy_failure_count += 1

                elif result["outcome"] == FetchOutcome.ANTI_SCRAPING and result["switch_agent"] or \
                    result["outcome"] in (FetchOutcome.PROXY_FAILURE, FetchOutcome.MAYBE_PROXY_FAILURE):

                    self.logger.warning("{0}, changing browser from {1}".format(result["outcome"].value, self.browser))

                    self.browser.switch_agent()
                    proxy_failure_count = 0

                    self.logger.warning("to {}...".format(self.browser))

            self.logger.info("exiting scraper...")


def gen_target_params(generate_search_url: Callable[[str], Tuple[str, str]], keyword_file: TextIO) \
        -> Generator[dict, None, None]:
    """ yield target params {"keyword": ... "url": ..., "referer": ...} from keywords in <keyword_file> """
    for line in keyword_file:
        keyword = line.strip()

        if keyword:
            url, referer = generate_search_url(keyword)
            yield {"keyword": keyword, "url": url, "referer": referer}


async def put_seed_urls(scrapers, generate_search_url, keyword_file=None):
    """ send seed urls to scrapers in cycle

        scrapers: scraper actors
        generate_search_url:
            if keyword_file is provided, function: keyword -> url, referer
            if not, generator function yielding {"keyword": ... "url": ..., "referer": ...}
        keyword_file (optional): opened file handle
    """
    logger = logging.getLogger("rc_crawler.put_seed_urls")

    scrapers_in_cycle = cycle(scrapers)

    if keyword_file:
        target_params = gen_target_params(generate_search_url, keyword_file)
    else:
        try:
            target_params = generate_search_url()
        except TypeError:
            logger.error("you did not specify a keyword file")
            raise

    for params in target_params:
        logger.debug("seed keyword: {keyword}".format(**params))

        await next(scrapers_in_cycle).send((
            TargetPriority.DEFAULT.value,
            Target(category=PageCategory.SEARCH.value, **params)
        ))
