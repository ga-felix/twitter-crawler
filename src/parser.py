class PublicMetricsParser:

    def parse(self, item) -> list:
        for key in item['public_metrics'].keys():
            item[key] = item['public_metrics'][key]
        del item['public_metrics']
        return item


class TweetParser:

    public_metric_parser = PublicMetricsParser()

    def explode_public_metrics(self, payload: dict):
        for tweet in payload['data']:
            tweet = self.public_metric_parser.parse(tweet)
        return payload

    def parse(self, payload: dict) -> list:
        payload = self.explode_public_metrics(payload)
        return payload['data']


class UserParser:

    public_metric_parser = PublicMetricsParser()

    def explode_public_metrics(self, payload: dict):
        for user in payload['includes']['users']:
            user = self.public_metric_parser.parse(user)
        return payload

    def parse(self, payload: dict) -> list:
        payload = self.explode_public_metrics(payload)
        return payload['includes']['users']


class ReferencedTweetsParser:

    public_metric_parser = PublicMetricsParser()

    def explode_public_metrics(self, payload: dict):
        for tweet in payload['includes']['tweets']:
            tweet = self.public_metric_parser.parse(tweet)
        return payload

    def parse(self, payload: dict) -> list:
        payload = self.explode_public_metrics(payload)
        return payload['includes']['tweets']


class ReferenceParser:

    def has_reference(self, tweet):
        return tweet['referenced_tweets'] is not None

    def find_full_referenced_tweet(self, payload, id):
        for tweet in payload['includes']['tweets']:
            if tweet['id'] == id:
                return tweet

    def find_references(self, tweet: dict, payload: dict) -> list:
        references = list()
        for referenced_tweet in tweet['referenced_tweets']:
            type = referenced_tweet['type']
            referenced_tweet = self.find_full_referenced_tweet(
                payload, referenced_tweet['id'])
            references.append({
                'id': tweet['id'] + referenced_tweet['id'] + type,
                'tweet_referrer': tweet['id'],
                'tweet_referenced': referenced_tweet['id'],
                'author_referrer': tweet['author_id'],
                'author_referenced': referenced_tweet['author_id'],
                'type': type
            })
        return references

    def parse(self, payload: dict) -> list:
        references_table = list()
        for tweet in payload['data']:
            if self.has_reference(tweet):
                references_table.append(self.find_references(tweet, payload))
        return references_table
