from base64 import urlsafe_b64encode
from pathlib import Path
from typing import Tuple
from urllib.parse import urlsplit
import logging

import aiofiles

logger = logging.getLogger("rc_crawler.persist")

STORAGE_PATH = "pages"


def get_filepath(url: str, run_timestamp: int) -> Tuple[Path, str]:
    """ get where the page should be saved

        url: url where the html is fetched from
        run_timestamp: UNIX timestamp when the crawl started

        returns <STORAGE_PATH>/<platform>/<run timestamp>, <base64 encoded url>.html
    """
    url_parts = urlsplit(url)
    platform = url_parts.netloc.split('.')[1]
    concise_url = "{0.scheme}://{0.netloc}{0.path}".format(url_parts)

    dirpath = Path(STORAGE_PATH) / platform / str(run_timestamp)
    filename = urlsafe_b64encode(concise_url.encode()).decode() + ".html"
    return dirpath, filename


def back_by_storage(run_timestamp):
    """ middleware to cache page to filesystem storage

        run_timestamp: UNIX timestamp when the crawl started

        (decoratee)
        next_handler: coroutine that fetches html from a url

        returns a new coroutine that uses filesystem as cache when doing the fetching
    """
    def middleware_factory(next_handler):
        async def middleware(session, url, *args, **kwargs):
            dirpath, filename = get_filepath(url, run_timestamp)
            page_filepath = dirpath / filename

            if page_filepath.exists():
                logging.info("reading from {0} instead of fetching from {1}".format(page_filepath, url))

                async with aiofiles.open(page_filepath) as f:
                    html = await f.read()
                    return {"outcome": "success", "html": html}

            result = await next_handler(session, url, *args, **kwargs)

            if result["outcome"] == "success":
                logging.debug("save html from {1} to {0}".format(page_filepath, url))

                dirpath.mkdir(parents=True, exist_ok=True)

                async with aiofiles.open(page_filepath, 'w') as f:
                    await f.write(html)

            return result

        return middleware
    return middleware_factory
