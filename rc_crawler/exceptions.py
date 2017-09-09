class CrawlerException(Exception):
    """ base exception class for rc_crawler project """


class AntiScrapingError(CrawlerException):
    """ anti-scraper mechanism is triggered """


class ProxyError(CrawlerException):
    """ scraping can't continue due to proxy problem """
