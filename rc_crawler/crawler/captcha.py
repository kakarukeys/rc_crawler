from io import BytesIO
from PIL import Image

import pytesseract


def solve_captcha(image_binary, config):
    """ return the characters contained in <image_binary> """
    with BytesIO(image_binary) as b:
        try:
            with Image.open(b) as img:
                return pytesseract.image_to_string(img, config=config)
        except OSError:
            raise ValueError("invalid image binary string: {}".format(image_binary))
