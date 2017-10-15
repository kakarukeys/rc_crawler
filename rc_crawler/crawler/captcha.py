import asyncio
from io import BytesIO
from PIL import Image

import pytesseract


async def solve_captcha(image_binary, config):
    """ return the characters contained in <image_binary> """
    with BytesIO(image_binary) as b:
        try:
            with Image.open(b) as img:
                solution = pytesseract.image_to_string(img, config=config)
                await asyncio.sleep(4)  # imitate human
                return solution

        except OSError:
            raise ValueError("invalid image binary string: {}".format(image_binary))
