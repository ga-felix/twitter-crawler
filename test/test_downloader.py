from unittest import TestCase
from src.downloader import Crawler, Caller, Keys


class CrawlerTests(TestCase):

    MINIMUM_TWEET_AMOUNT = 10
    MINIMUM_PAGE_AMOUNT = 1

    def has_requested_page_number(self, iterations: int):
        with self.subTest():
            self.assertEqual(iterations, self.MINIMUM_PAGE_AMOUNT)

    def has_requested_tweet_number(self, payload: dict):
        with self.subTest():
            self.assertEqual(len(payload), self.MINIMUM_TWEET_AMOUNT)

    def has_requested_string(self, payload: dict):
        with self.subTest():
            has_string = True
            for tweet in payload['data']:
                if 'teste' not in tweet['text']:
                    has_string = False
            self.assertTrue(has_string)

    def test_full_search_tweets(self):
        query, pages = 'teste lang:pt', 0
        for payload in Crawler(Caller(), Keys()).full_search_tweets(query, max_results=self.MINIMUM_TWEET_AMOUNT, pages=self.MINIMUM_PAGE_AMOUNT):
            self.has_requested_string(payload)
            self.has_requested_tweet_number(payload)
            pages += 1
        self.has_requested_page_number(pages)
