import datetime
import logging
import os
import requests
import shutil
import time

def create_log_dir():
    dir = 'logs'
    if os.path.exists(dir):
        shutil.rmtree(dir)
    os.makedirs(dir)

def setup_logging():
    log_file = 'logs/{}-{}.{}'.format(datetime.datetime.now().isoformat(), 'log', 'log')
    logging.basicConfig(
            filename= log_file,
            filemode='w+',
            format='%(asctime)s %(levelname)-8s %(message)s',
            level=logging.INFO,
            datefmt='%Y-%m-%d %H:%M:%S')

create_log_dir()
setup_logging()

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

    def full_search_tweets(self, query, start_time=None, end_time=None, max_results=None, tweet_amount=None):
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
        return self.caller.download(url, header, parameters, tweet_amount if tweet_amount else -1)


class Caller():

    refresh_window = 15 * 60

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
                logging.info('Downloader has gotten %d pages successfully.', count)
                break
            except Exception as e:
                sleep_time = self.refresh_window - self.seconds_passed_since(started)
                logging.error('%s has occurred. Sleeping %s seconds.', str(e), sleep_time)
                time.sleep(sleep_time)
                started = self.now()
                continue

    def seconds_passed_since(self, started):
        return (self.now() - started).total_seconds()

    def now(self):
        return datetime.datetime.now().isoformat()

class Singleton(type):
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class Keys(metaclass=Singleton):

    import threading
    lock = threading.Lock()
    functions = dict()

    def __init__(self):
        self.tokens = self.read_tokens()

    def read_tokens(self):
        tokens = os.getenv('TWITTER_BEARER_TOKENS')
        if tokens is None:
            raise ValueError('TWITTER_BEARER_TOKENS enviroment variable was not found.')
        return set(tokens.split(','))

    def get_bearer_token(self, function) -> str:
        name = function.__name__
        with self.lock:
            if name not in self.functions:
                self.functions[name] = self.tokens
                return self.tokens.pop()
            elif not self.functions[name]:
                raise ValueError('All bearer tokens have been taken!')
            else:
                return self.functions[name].pop()

    def get_header_with_bearer_token(self, function) -> dict:
        bearer_token = self.get_bearer_token(function)
        return {
            "Authorization": "Bearer {}".format(bearer_token)
        }
