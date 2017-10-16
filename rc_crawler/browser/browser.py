from enum import Enum
from ssl import SSLError
import asyncio
import logging

import aiohttp
import ujson

from rc_crawler.utils import describe_exception
from .agents import renew_agent

logger = logging.getLogger("rc_crawler.browser")


RETRY_STATUS_CODES = {408, 500, 502, 503, 504}

# the urls we follow should not have produced these codes, when not using proxy
PROXY_ERROR_STATUS_CODES = {403, 404, 407, 515}

HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Encoding': 'gzip, deflate',
    'Accept-Language': 'en-US,en;q=0.5',
    'Connection': 'keep-alive',
}


class FetchOutcome(Enum):
    SUCCESS = "success"
    FAILURE = "failure"

    # requires retry:
    RETRY = "retry"
    ANTI_SCRAPING = "anti_scraping"
    PROXY_FAILURE = "proxy_failure"
    MAYBE_PROXY_FAILURE = "maybe_proxy_failure"


class Browser:
    def __init__(self, device_type):
        """ a browser context consists of a aiohttp client session, a header that mimics
            <device_type> user agent and a proxy used to make any requests.
        """
        self.device_type = device_type

        self.session = None
        self.user_agent = None
        self.proxy = None

    def __str__(self):
        return "<Browser: device_type={0}, user_agent={1}, proxy={2}, default_headers={3}>".format(
            self.device_type, self.user_agent, self.proxy, HEADERS)

    def switch_agent(self):
        self.user_agent, self.proxy = renew_agent(self.device_type)
        self.session.cookie_jar.clear()

    async def fetch(self, url, params=None, extra_headers=None, filetype="text"):
        """ fetch content from <url> with query <params>
            filetype: text/json/binary
            returns {"outcome": ..., (optional) "content": ..., (optional) "reason": "reason for failure"}
        """
        headers = {"User-Agent": self.user_agent}
        headers.update(extra_headers or {})

        logger.debug("sending request to {0}, params: {1}, extra headers: {2}, proxy: {3}".format(
            url, params, extra_headers, self.proxy))

        try:
            async with self.session.get(url, headers=headers, proxy=self.proxy, params=params) as response:
                content = await getattr(response, "read" if filetype == "binary" else filetype)()

                if response.status == 200:
                    result = {"outcome": FetchOutcome.SUCCESS, "content": content}
                else:
                    logger.error("non-200 response, url: {0}, request headers: {1}, status: {2}, content: {3}".format(
                        url, response.request_info.headers, response.status, content))

                    if response.status in PROXY_ERROR_STATUS_CODES:
                        result = {"outcome": FetchOutcome.PROXY_FAILURE, "reason": response.status}
                    elif response.status in RETRY_STATUS_CODES:
                        result = {"outcome": FetchOutcome.RETRY, "reason": response.status}
                    else:
                        result = {"outcome": FetchOutcome.FAILURE, "reason": response.status}

        except aiohttp.ClientHttpProxyError as e:
            # subclass of ClientResponseError
            result = {"outcome": FetchOutcome.PROXY_FAILURE, "reason": describe_exception(e)}

        except (ConnectionResetError, SSLError, aiohttp.ClientPayloadError, aiohttp.ClientResponseError) as e:
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

    async def __aenter__(self):
        self.session = aiohttp.ClientSession(headers=HEADERS, json_serialize=ujson.dumps)
        self.switch_agent()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.session.close()
