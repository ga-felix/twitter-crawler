from unittest import TestCase
from src.parser import *


class ParserTests(TestCase):

    payload = {
        'data': [
            {
                'id': '124',
                'text': 'RT @Nobody: Sample payload!',
                'created_at': '',
                'author_id': '123',
                'public_metrics': {
                    'retweet_count': 1,
                    'like_count': 2,
                    'quote_count': 0,
                    'reply_count': 1
                },
                'referenced_tweets': [
                    {
                        'id': '123',
                        'type': 'retweeted'
                    }
                ],
                'lang': 'pt'
            }
        ],
        'includes': {
            'users': [
                {
                    'id': '123',
                    'username': 'Nobody',
                    'description': 'I\'m nobody.',
                    'public_metrics': {
                        'followers_count': 1,
                        'following_count': 2,
                        'tweet_count': 0,
                        'listed_count': 1,
                    },
                    'verified': True,
                    'created_at': ''
                }
            ]
        }
    }

    def get_dict_keys(self, any_dict):
        return list(any_dict.keys())

    def test_should_parse_tweets_on_payload(self):
        tweet_parser = TweetParser()
        tweets = tweet_parser.parse(self.payload)
        expected_fields = ['id',
                           'text',
                           'created_at',
                           'author_id',
                           'retweet_count',
                           'like_count',
                           'quote_count',
                           'reply_count',
                           'referenced_tweets',
                           'lang']
        self.assertCountEqual(self.get_dict_keys(tweets[0]), expected_fields)

    def test_should_parse_users_on_payload(self):
        user_parser = UserParser()
        users = user_parser.parse(self.payload)
        expected_fields = ['id',
                           'username',
                           'description',
                           'followers_count',
                           'following_count',
                           'tweet_count',
                           'listed_count',
                           'verified',
                           'created_at']
        self.assertCountEqual(self.get_dict_keys(users[0]), expected_fields)
