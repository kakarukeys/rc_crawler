from base64 import b64encode
import asyncio
import logging

import aiohttp
import ujson

logger = logging.getLogger("rc_crawler.anti_captcha")


CLIENT_KEY = None
API_BASE_URL = "https://api.anti-captcha.com/"
CREATE_TASK_SEND_INTERVAL = 10
GET_TASK_RESULT_POLL_INTERVAL = 2.5


class AntiCaptchaException(Exception):
    """ base exception class for anti-captcha solver """


class AntiCaptchaServiceError(AntiCaptchaException):
    """ anti-captcha service is unavailable """


class AntiCaptchaAPIError(AntiCaptchaException):
    def __init__(self, error_id: int, error_code: str, error_description: str) -> None:
        super().__init__("[{0} - {1}]: {2}".format(error_code, error_id, error_description))
        self.error_id = error_id
        self.error_code = error_code
        self.error_description = error_description


class CaptchaSolver:
    def __init__(self):
        self.session = None

    async def _request(self, endpoint: str, data: dict) -> dict:
        url = API_BASE_URL + endpoint

        try:
            async with self.session.post(url, json=data) as response:
                result = await response.json()

                if result["errorId"]:
                    logger.error("api error happens when requesting {0} with {1}".format(url, data))
                    raise AntiCaptchaAPIError(result["errorId"], result["errorCode"], result["errorDescription"])

                return result

        except aiohttp.ClientResponseError as e:
            raise AntiCaptchaServiceError("anti-captcha server returns HTTP 4xx or HTTP 5xx") from e

    async def create_task(self, body: str) -> int:
        data = {
            "clientKey": CLIENT_KEY,
            "task": {"type":"ImageToTextTask", "body": body, "case": True},
        }

        while True:
            try:
                result = await self._request("createTask", data)
            except AntiCaptchaAPIError as e:
                if e.error_code == "ERROR_NO_SLOT_AVAILABLE":
                    logger.exception(e)
                else:
                    raise
            else:
                return result["taskId"]

            await asyncio.sleep(CREATE_TASK_SEND_INTERVAL)

    async def get_task_result(self, task_id: int) -> str:
        data = {
            "clientKey": CLIENT_KEY,
            "taskId": task_id
        }

        while True:
            result = await self._request("getTaskResult", data)

            if result["status"] == "ready":
                return result["solution"]["text"]

            await asyncio.sleep(GET_TASK_RESULT_POLL_INTERVAL)

    async def solve_captcha(self, image_binary: bytes) -> str:
        body = b64encode(image_binary).decode()

        task_id = await self.create_task(body)
        logger.debug("submitted task {} to anti-captcha.com".format(task_id))

        return await self.get_task_result(task_id)

    async def __aenter__(self):
        self.session = aiohttp.ClientSession(json_serialize=ujson.dumps, raise_for_status=True)
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.session.close()
