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
            del tweet['public_metrics']
        return payload

    def explode_referenced_tweets(self, payload: dict):
        for tweet in payload['data']:
            self.append_referenced_tweets(tweet)
        return payload

    def append_referenced_tweets(self, tweet):
        referenced_tweets = list()
        for referenced_tweet in tweet['referenced_tweets']:
            referenced_tweets.append(referenced_tweet['id'])
        tweet['referenced_tweets'] = referenced_tweets

    def convert(self, payload: dict) -> list:
        payload = self.explode_public_metrics(payload)
        return payload['data']
