import os.path

import pytest
import rc_crawler.crawler.captcha as cp


@pytest.fixture
def example_captcha(request):
    test_module_file = request.module.__file__
    filepath = os.path.join(os.path.dirname(test_module_file), "example_captcha.jpeg")

    with open(filepath, "rb") as f:
        yield f.read()


def test_solve_captcha(example_captcha):
    with pytest.raises(ValueError):
        cp.solve_captcha(b'')

    with pytest.raises(ValueError):
        cp.solve_captcha(b'x')

    assert cp.solve_captcha(example_captcha) == "MJGPLP"
