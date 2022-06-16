from src import *
from test import *
import unittest


def write_objects(writer, objects: list):
    for object in objects:
        writer.write(object)


def write_references(payload: dict):
    tweet_references = ReferenceParser().parse(payload)
    avro_writer = AvroWriter(
        '/home/gafelix/Documentos/Git/twitter-crawler/avro/reference/',
        '/home/gafelix/Documentos/Git/twitter-crawler/src/sch/reference.avsc')
    for tweet_reference in tweet_references:
        write_objects(avro_writer, tweet_reference)


def write_users(payload: dict):
    users = UserParser().parse(payload)
    avro_writer = AvroWriter(
        '/home/gafelix/Documentos/Git/twitter-crawler/avro/user/',
        '/home/gafelix/Documentos/Git/twitter-crawler/src/sch/user.avsc')
    write_objects(avro_writer, users)


def write_referenced_tweets(payload: dict, writer):
    referenced_tweets = ReferencedTweetsParser().parse(payload)
    write_objects(writer, referenced_tweets)


def write_tweets(payload: dict):
    tweets = TweetParser().parse(payload)
    avro_writer = AvroWriter(
        '/home/gafelix/Documentos/Git/twitter-crawler/avro/tweet/',
        '/home/gafelix/Documentos/Git/twitter-crawler/src/sch/tweet.avsc')
    write_referenced_tweets(payload, avro_writer)
    write_objects(avro_writer, tweets)


def main():
    for payload in Crawler(Caller(), Keys()).full_search_tweets('retweets_of:jairbolsonaro lang:pt', tweet_amount=10):
        google_cloud_uploader = GoogleCloudUploader(
            '/home/gafelix/Documentos/Git/twitter-crawler/src/cfg/key.json')
        write_tweets(payload)
        write_users(payload)
        write_references(payload)
        google_cloud_uploader.upload(
            '/home/gafelix/Documentos/Git/twitter-crawler/avro/tweet/',
            'gs://monitor_debate/twitter/tweet')
        google_cloud_uploader.upload(
            '/home/gafelix/Documentos/Git/twitter-crawler/avro/user/',
            'gs://monitor_debate/twitter/user')
        google_cloud_uploader.upload(
            '/home/gafelix/Documentos/Git/twitter-crawler/avro/reference/',
            'gs://monitor_debate/twitter/tweet_join')


if __name__ == '__main__':
    # unittest.main()
    main()
