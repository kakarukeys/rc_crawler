from urllib.parse import urlsplit

import pytest
import rc_crawler.persist as ps


def test_build_concise_url():
    for url, expected in [
            ("https://www.google.com/search#abc",
             "/search"),

            ("https://www.google.com/search/foo=bar#abc",
             "/search/foo=bar"),

            ("https://www.google.com/search/foo=bar?hello=world#abc",
             "/search/foo=bar?hello=world"),

            ("https://www.google.com/search/foo=bar?long_string={}&hello=world#abc".format('x' * 30),
             "/search/foo=bar?hello=world"),

            ("https://www.google.com/search/foo=bar?long_string={}&hello=world&long_string={}#abc".format('x' * 15, 'x' * 15),
             "/search/foo=bar?hello=world"),
        ]:
        url_parts = urlsplit(url)
        concise_url = ps.build_concise_url(url_parts)
        assert concise_url == expected
