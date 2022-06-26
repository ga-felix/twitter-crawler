from unittest import TestCase
from src.downloader import Crawler, Caller, Keys


class CrawlerTests(TestCase):

    TWEET_AMOUNT = 10
    PAGE_AMOUNT = 1
    TEST_KEYWORD = 'brasil'

    def has_requested_page_number(self):
        with self.subTest():
            self.assertEqual(len(self._payloads), self.PAGE_AMOUNT)

    def has_requested_tweet_number(self):
        with self.subTest():
            first_page = self._payloads[0]['data']
            self.assertEqual(len(first_page), self.TWEET_AMOUNT)

    def has_requested_string(self):
        with self.subTest():
            has_string = True
            for payload in self._payloads:
                for tweet in payload['data']:
                    if self.TEST_KEYWORD not in tweet['text'].lower():
                        has_string = False
            self.assertTrue(has_string)

    def test_full_search_tweets(self):
        query = f'\"{self.TEST_KEYWORD}\" lang:pt -is:retweet -is:quote'
        self._payloads = [payload for payload in Crawler(Caller(), Keys()).full_search_tweets(
            query, max_results=self.TWEET_AMOUNT, pages=self.PAGE_AMOUNT)]
        self.has_requested_string()
        self.has_requested_page_number()
        self.has_requested_tweet_number()
