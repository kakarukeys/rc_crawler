from datetime import datetime

import pytest
import rc_crawler.platforms.bing as bing


def test_parse_video_info_string():
    result = bing.parse_video_info_string("Gorgeous Bar Height Dining Table Set from YouTube · High Definition · Duration:  4 minutes 50 seconds  · 37 views · uploaded on 5/7/2016 · uploaded by Awesome Home Decor")

    assert result == {
        "title": "Gorgeous Bar Height Dining Table Set",
        "views": 37,
        "uploaded_on": datetime(2016, 7, 5)
    }

    result = bing.parse_video_info_string("Faux Leather Belt Tutorial from YouTube · High Definition · Duration:  12 minutes 55 seconds  · 1,000+ views · uploaded on 22/9/2016 · uploaded by scrap queen")

    assert result == {
        "title": "Faux Leather Belt Tutorial",
        "views": 1000,
        "uploaded_on": datetime(2016, 9, 22),
    }

    result = bing.parse_video_info_string("Ub40 - Red Red Wine (Live) from Dailymotion · Duration:  4 minutes 53 seconds  · 34,000+ views · uploaded on 26/1/2007")

    assert result == {
        "title": "Ub40 - Red Red Wine (Live)",
        "views": 34000,
        "uploaded_on": datetime(2007, 1, 26)
    }

    result = bing.parse_video_info_string("UB40 - Red Red Wine (1983) from Sapo Video · Duration:  3 minutes 23 seconds  · 404 views · uploaded on 3/9/2011 · uploaded by jteixeira")

    assert result == {
        "title": "UB40 - Red Red Wine (1983)",
        "views": 404,
        "uploaded_on": datetime(2011, 9, 3)
    }

    result = bing.parse_video_info_string("UB40 - Red red wine from daum.net · 390 views · uploaded on 15/4/2007 · uploaded by utopia")

    assert result == {
        "title": "UB40 - Red red wine",
        "views": 390,
        "uploaded_on": datetime(2007, 4, 15)
    }

    # missing info
    result = bing.parse_video_info_string("Вход from mail.ru")

    assert result == {
        "title": "Вход",
        "views": None,
        "uploaded_on": None
    }

    # missing info
    result = bing.parse_video_info_string("Simply Bamboo Large Roll Top Bread Box Storage Box from Dailymotion · Duration:  8 seconds  · uploaded on 28/3/2017 · uploaded by Therelmradinaen")

    assert result == {
        "title": "Simply Bamboo Large Roll Top Bread Box Storage Box",
        "views": None,
        "uploaded_on": datetime(2017, 3, 28)
    }

    # missing info
    result = bing.parse_video_info_string("Русская дорога from ucoz.ru · Duration:  2 minutes 48 seconds  · 10 views")

    assert result == {
        "title": "Русская дорога",
        "views": 10,
        "uploaded_on": None
    }

    # human-friendly date
    result = bing.parse_video_info_string("A diamond the size of a tennis ball finally sold for $53M from USATODAY · Duration:  45 seconds  · uploaded on 1 day ago", relative_base=datetime(2012, 12, 21))

    assert result == {
        "title": "A diamond the size of a tennis ball finally sold for $53M",
        "views": None,
        "uploaded_on": datetime(2012, 12, 20)
    }
