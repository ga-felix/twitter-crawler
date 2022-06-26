from threading import Lock
from datetime import datetime
from requests import request
from time import sleep
from os import getenv


class Crawler:

    def __init__(self, caller, keys):
        self.caller = caller
        self.keys = keys
        self.header = self.keys.get_header_with_bearer_token()

    base_url = 'https://api.twitter.com/2'
    expansions = 'author_id,referenced_tweets.id.author_id,referenced_tweets.id'
    tweet_fields = 'id,text,created_at,author_id,public_metrics,referenced_tweets,lang'
    user_fields = 'id,username,description,public_metrics,verified,created_at'

    def append_parameter_if_exists(self, parameters, value, name):
        if value is not None:
            parameters[name] = value

    def full_search_tweets(self, query, start_time=None, end_time=None, max_results=None, pages=None):
        parameters = {
            'query': query,
            'expansions': self.expansions,
            'tweet.fields': self.tweet_fields,
            'user.fields': self.user_fields,
            'next_token': {}
        }
        self.append_parameter_if_exists(parameters, start_time, 'start_time')
        self.append_parameter_if_exists(parameters, end_time, 'end_time')
        self.append_parameter_if_exists(parameters, max_results, 'max_results')
        url = self.base_url + '/tweets/search/all'
        return self.caller.download(
            url, self.header, parameters, pages if pages else -1)


class Caller:

    TWITTER_API_REFRESH_TOKEN_WINDOW = 15 * 60  # That is 15 minutes =)

    def get(self, url, header, parameters):
        response = request(
            "GET", url, headers=header, params=parameters)
        if response.status_code != 200:
            raise Exception(response.status_code, response.text)
        return response.json()

    def pages(self, url, header, parameters, pages):
        page_count = 0
        while True:
            response = self.get(url, header, parameters)
            yield response
            page_count += 1
            if not self.can_paginate(response, page_count, pages):
                break
            parameters['next_token'] = response['meta']['next_token']

    def can_paginate(self, response, page_count, max_pages):
        has_meta = 'meta' in response.keys()
        has_next_token = 'next_token' in response['meta'].keys()
        return has_meta and has_next_token and page_count != max_pages

    def download(self, url, header, parameters, pages):
        return self.limit_handler(url, header, parameters, pages)

    def limit_handler(self, url, header, parameters, pages):
        pages = self.pages(url, header, parameters, pages)
        started, count = self.now(), 0
        while True:
            try:
                yield next(pages)
                count += 1
            except StopIteration:
                break
            except Exception as e:
                sleep_time = self.TWITTER_API_REFRESH_TOKEN_WINDOW - \
                    self.seconds_passed_since(started)
                sleep(sleep_time)
                started = self.now()
                continue

    def seconds_passed_since(self, started):
        return (self.now() - started).total_seconds()

    def now(self):
        return datetime.now()


class Singleton(type):

    _lock = Lock()
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            with cls._lock:
                if cls not in cls._instances:
                    cls._instances[cls] = super(
                        Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class Keys(metaclass=Singleton):

    lock = Lock()

    def __init__(self):
        self.tokens = self.read_tokens()

    def read_tokens(self):
        tokens = getenv('TWITTER_BEARER_TOKENS')
        if tokens is None:
            raise ValueError(
                'TWITTER_BEARER_TOKENS enviroment variable was not found.')
        return set(tokens.split(','))

    def is_not_empty(self, array: list) -> bool:
        return len(array) > 0

    def get_bearer_token(self) -> str:
        with self.lock:
            if self.is_not_empty(self.tokens):
                print(
                    f'Bearer token has been taken, {len(self.tokens) - 1} left.')
                return self.tokens.pop()
            else:
                raise ValueError('All bearer tokens have been taken!')

    def get_header_with_bearer_token(self) -> dict:
        bearer_token = self.get_bearer_token()
        return {"Authorization": "Bearer {}".format(bearer_token)}
