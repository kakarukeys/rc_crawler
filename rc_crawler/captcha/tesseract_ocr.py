import asyncio
from io import BytesIO
from PIL import Image

import pytesseract

from rc_crawler.utils import DummyAsyncContextManager


def ocr(image_binary: bytes, config: dict) -> str:
    """ return the characters contained in <image_binary> """
    with BytesIO(image_binary) as b:
        try:
            with Image.open(b) as img:
                return pytesseract.image_to_string(img, config=config)
        except OSError:
            raise ValueError("invalid image binary string: {}".format(image_binary))


class CaptchaSolver(DummyAsyncContextManager):
    def __init__(self, config: dict) -> None:
        self.config = config

    async def solve_captcha(self, image_binary: bytes) -> str:
        solution = ocr(image_binary, self.config)
        await asyncio.sleep(4)  # imitate human
        return solution
