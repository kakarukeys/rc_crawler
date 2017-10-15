import asyncio
from io import BytesIO
from PIL import Image

import pytesseract


def solve_captcha_ocr(image_binary, config):
    """ return the characters contained in <image_binary> """
    with BytesIO(image_binary) as b:
        try:
            with Image.open(b) as img:
                return pytesseract.image_to_string(img, config=config)
        except OSError:
            raise ValueError("invalid image binary string: {}".format(image_binary))


async def solve_captcha(*args, **kwargs):
    solution = solve_captcha_ocr(*args, **kwargs)
    await asyncio.sleep(4)  # imitate human
    return solution
