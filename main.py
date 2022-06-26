from src import *
from test import *
import unittest
import csv


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


def get_data(payload: dict):
    write_tweets(payload)
    write_users(payload)
    write_references(payload)


def upload_data(uploader):
    uploader.upload(
        '/home/gafelix/Documentos/Git/twitter-crawler/avro/tweet/',
        'gs://monitor_debate/twitter/tweet')
    uploader.upload(
        '/home/gafelix/Documentos/Git/twitter-crawler/avro/user/',
        'gs://monitor_debate/twitter/user')
    uploader.upload(
        '/home/gafelix/Documentos/Git/twitter-crawler/avro/reference/',
        'gs://monitor_debate/twitter/tweet_join')


def open_csv_as_list(path: str, column: int, delimiter=',') -> list:
    with open(path) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=delimiter)
        return [row[column] for row in csv_reader]


def main():
    google_cloud_uploader = GoogleCloudUploader(
        '/home/gafelix/Documentos/Git/twitter-crawler/src/cfg/key.json')
    csv_path = '/home/gafelix/Documentos/LulaUsers.csv'
    for account in open_csv_as_list(csv_path, 0):
        query = f'\"http\" from:{account} lang:pt'
        for payload in Crawler(Caller(), Keys()).full_search_tweets(query, max_results=500, start_time='2022-06-01T00:00:00Z'):
            get_data(payload)
            upload_data(google_cloud_uploader)


if __name__ == '__main__':
    unittest.main()
    #main()
