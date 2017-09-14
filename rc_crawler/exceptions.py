class CrawlerException(Exception):
    """ base exception class for rc_crawler project """


class AntiScrapingError(CrawlerException):
    """ anti-scraper mechanism is triggered """
