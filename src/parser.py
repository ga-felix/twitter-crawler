class TweetParser:

    def explode_public_metrics(self, payload: dict):
        for tweet in payload['data']:
            for key in tweet['public_metrics'].keys():
                tweet[key] = tweet['public_metrics'][key]
            del tweet['public_metrics']
        return payload

    def parse(self, payload: dict) -> list:
        payload = self.explode_public_metrics(payload)
        return payload['data']


class UserParser:

    def explode_public_metrics(self, payload: dict):
        for user in payload['includes']['users']:
            for key in user['public_metrics'].keys():
                user[key] = user['public_metrics'][key]
            del user['public_metrics']
        return payload

    def parse(self, payload: dict) -> list:
        payload = self.explode_public_metrics(payload)
        return payload['includes']['users']
