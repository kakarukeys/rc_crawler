from base64 import urlsafe_b64encode
from pathlib import Path
from typing import Tuple
from urllib.parse import urlsplit, parse_qs, urlencode, SplitResult
import logging

import aiofiles

from .browser import FetchOutcome

logger = logging.getLogger("rc_crawler.persist")

STORAGE_PATH = "pages"
QUERY_PARAM_VALUE_MAX_LENGTH = 30


def build_concise_url(url_parts: SplitResult) -> str:
    """ shorten the url while keeping important details """
    query_params = parse_qs(url_parts.query)
    trimmed_query_params = {n: l for n, l in query_params.items() if sum(len(v) for v in l) < QUERY_PARAM_VALUE_MAX_LENGTH}
    new_qs = urlencode(trimmed_query_params, doseq=True)

    if new_qs:
        return "{0}?{1}".format(url_parts.path, new_qs)
    else:
        return url_parts.path


def get_filepath(url: str, run_timestamp: int) -> Tuple[Path, str]:
    """ get where the page should be saved

        url: url where the html is fetched from
        run_timestamp: UNIX timestamp when the crawl started

        returns <STORAGE_PATH>/<platform>/<run timestamp>, <base64 encoded url>.html
    """
    url_parts = urlsplit(url)
    netloc_terms = url_parts.netloc.split('.')

    # guess the platform name from url
    platform = netloc_terms[1 if len(netloc_terms) > 2 else 0]

    concise_url = build_concise_url(url_parts)

    dirpath = Path(STORAGE_PATH) / platform / str(run_timestamp)
    filename = urlsafe_b64encode(concise_url.encode()).decode()

    assert len(filename) < 256, "length of filename generated has exceeded Linux filesystem limit"

    return dirpath, filename


def back_by_storage(run_timestamp):
    """ middleware to cache page to filesystem storage

        run_timestamp: UNIX timestamp when the crawl started

        (decoratee)
        next_handler: coroutine that fetches html from a url

        returns a new coroutine that uses filesystem as cache when doing the fetching
    """
    def middleware_factory(next_handler):
        async def middleware(url, *args, read_from_cache=True, **kwargs):
            dirpath, filename = get_filepath(url, run_timestamp)
            page_filepath = dirpath / filename

            if page_filepath.exists() and read_from_cache:
                logger.info("reading from {0} instead of fetching from {1}".format(page_filepath, url))

                async with aiofiles.open(page_filepath) as f:
                    html = await f.read()
                    return {"outcome": FetchOutcome.SUCCESS, "content": html, "from_cache": True}

            result = await next_handler(url, *args, **kwargs)
            result["from_cache"] = False

            if result["outcome"] == FetchOutcome.SUCCESS:
                logger.debug("save html from {1} to {0}".format(page_filepath, url))

                dirpath.mkdir(parents=True, exist_ok=True)

                async with aiofiles.open(page_filepath, 'w') as f:
                    await f.write(result["content"])

            return result

        return middleware
    return middleware_factory
