from enum import Enum
import asyncio
import logging

import aiohttp

from .utils import describe_exception


RETRY_STATUS_CODES = {408, 500, 502, 503, 504}

# the urls we follow should not have produced these codes, when not using proxy
PROXY_ERROR_STATUS_CODES = {403, 404, 407, 515}


class FetchOutcome(Enum):
    SUCCESS = "success"
    FAILURE = "failure"

    # requires retry:
    RETRY = "retry"
    ANTI_SCRAPING = "anti_scraping"
    PROXY_FAILURE = "proxy_failure"
    MAYBE_PROXY_FAILURE = "maybe_proxy_failure"


async def fetch(session: aiohttp.ClientSession, url: str, extra_headers: dict={}, proxy: str=None) -> None:
    """ fetch html content from <url>
        returns {"outcome": ..., (optional) "html": ..., (optional) "reason": "reason for failure"}
    """
    logger = logging.getLogger("rc_crawler.fetch")

    logger.debug("sending request to {0} with extra headers {1}, proxy: {2}".format(
        url, extra_headers, proxy))

    try:
        async with session.get(url, headers=extra_headers, proxy=proxy) as response:
            html = await response.text()

            if response.status == 200:
                result = {"outcome": FetchOutcome.SUCCESS, "html": html}
            else:
                logger.error("non-200 response, url: {0}, request headers: {1}, status: {2}, html: {3}, proxy: {4}".format(
                    url, response.request_info.headers, response.status, html, proxy))

                if response.status in PROXY_ERROR_STATUS_CODES:
                    result = {"outcome": FetchOutcome.PROXY_FAILURE, "reason": response.status}
                elif response.status in RETRY_STATUS_CODES:
                    result = {"outcome": FetchOutcome.RETRY, "reason": response.status}
                else:
                    result = {"outcome": FetchOutcome.FAILURE, "reason": response.status}

    except aiohttp.ClientHttpProxyError as e:
        # subclass of ClientResponseError
        result = {"outcome": FetchOutcome.PROXY_FAILURE, "reason": describe_exception(e)}

    except (aiohttp.ClientPayloadError, aiohttp.ClientResponseError) as e:
        result = {"outcome": FetchOutcome.RETRY, "reason": describe_exception(e)}

    except aiohttp.ClientConnectorError as e:
        # subclass of ClientConnectionError
        if isinstance(e.__cause__, ConnectionResetError):
            result = {"outcome": FetchOutcome.RETRY, "reason": describe_exception(e)}
        else:
            # includes ClientProxyConnectionError as cause
            result = {"outcome": FetchOutcome.MAYBE_PROXY_FAILURE, "reason": describe_exception(e)}

    except (asyncio.TimeoutError, aiohttp.ClientConnectionError) as e:
        # includes ServerDisconnectedError, ClientOSError
        result = {"outcome": FetchOutcome.MAYBE_PROXY_FAILURE, "reason": describe_exception(e)}

    return result
