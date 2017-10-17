import os.path

import pytest
import rc_crawler.captcha.tesseract_ocr as cp


@pytest.fixture
def example_captcha(request):
    test_module_file = request.module.__file__
    filepath = os.path.join(os.path.dirname(test_module_file), "example_captcha.jpeg")

    with open(filepath, "rb") as f:
        yield f.read()


def test_ocr(example_captcha):
    with pytest.raises(ValueError):
        cp.ocr(b'', config="-psm 6")

    with pytest.raises(ValueError):
        cp.ocr(b'x', config="-psm 6")

    assert cp.ocr(example_captcha, config="-psm 6") == "MJGPLP"
