import requests
from tqdm import tqdm
import os
import datetime
import time


class Crawler():

    def __init__(self, caller, keys):
        self.caller = caller
        self.keys = keys

    base_url = 'https://api.twitter.com/2'
    expansions = 'author_id,referenced_tweets.id.author_id,referenced_tweets.id,in_reply_to_user_id'
    tweet_fields = 'id,text,created_at,author_id,public_metrics,referenced_tweets,lang'
    user_fields = 'id,username,description,public_metrics,verified,created_at'

    def append_parameter_if_exists(self, parameters, value, name):
        if value is not None:
            parameters[name] = value

    def full_search_tweets(self, query, start_time=None, end_time=None, max_results=None):
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
        header = self.keys.get_header_with_bearer_token(self.full_search_tweets)
        return self.caller.download(url, header, parameters)


class Caller():

    def __init__(self):
        self.started = datetime.datetime.now().isoformat()
        self.refresh_window = 15 * 60

    def get(self, url, header, parameters):
        response = requests.request("GET", url, headers=header, params=parameters)
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
            parameters['next_token'] = response.meta.next_token

    def can_paginate(self, response, page_count, max_pages):
        return hasattr(response.meta, 'next_token') and page_count != max_pages

    def limit_handler(self, url, header, parameters, pages):
        while True:
            try:
                yield next(self.pages(url, header, parameters, pages))
            except StopIteration:
                break
            except Exception:
                now = datetime.now().isoformat()
                sleep_time = self.refresh_window - (now - self.started).total_seconds()
                time.sleep(sleep_time)
                continue

    def download(self, url, header, parameters, pages=-1):
        description = 'Downloading'
        return tqdm(self.limit_handler(url, header, parameters, pages), desc=description)


class Singleton(type):
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class Keys(metaclass=Singleton):

    import threading
    lock = threading.Lock()

    def __init__(self):
        self.tokens = set(os.getenv('TWITTER_BEARER_TOKENS').split(','))
        self.functions = dict()

    def get_bearer_token(self, function) -> str:
        name = function.__name__
        with self.lock:
            if name not in self.functions:
                function[name] = self.tokens
                return self.tokens.pop()
            else:
                if len(function[name]) == 0:
                    raise ValueError('All bearer tokens have been taken!')
                else:
                    return function[name].pop()

    def get_header_with_bearer_token(self, function) -> dict:
        bearer_token = self.get_bearer_token(function)
        return {
            "Authorization": "Bearer {}".format(bearer_token)
        }
