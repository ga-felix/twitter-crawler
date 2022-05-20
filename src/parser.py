import pandas as pd


class Parser():

    def __init__(self, format):
        self.format = format

    def parse(self, payload: dict):
        return self.format.convert(payload)


class CsvFormat():

    def explode_public_metrics(self, payload: dict):
        for tweet in payload['data']:
            for key in tweet['public_metrics'].keys():
                tweet[key] = tweet['public_metrics'][key]
        return payload

    def explode_referenced_tweets(self, payload: dict):
        for tweet in payload['data']:
            for idx, ref in enumerate(tweet['referenced_tweets']):
                tweet = self.append_referenced_tweets(tweet, idx)
        return payload

    def append_referenced_tweets(self, tweet, index):
        for key in tweet['referenced_tweets'][index].keys():
            new_key = 'reference_' + key + '_' + str(index)
            tweet[new_key] = tweet['referenced_tweets'][index][key]
        return tweet

    def convert(self, payload: dict):
        payload = self.explode_referenced_tweets(payload)
        payload = self.explode_public_metrics(payload)
        return pd.DataFrame(payload['data'])