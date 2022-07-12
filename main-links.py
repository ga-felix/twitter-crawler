from src import *
# from test import *
# import unittest
import csv


def write_objects(writer, objects: list):
    for object in objects:
        writer.write(object)


def write_references(payload: dict):
    tweet_references = ReferenceParser().parse(payload)
    avro_writer = AvroWriter(
        '/home/felix/twitter/twitter-crawler/avro/reference/',
        '/home/felix/twitter/twitter-crawler/src/sch/reference.avsc')
    for tweet_reference in tweet_references:
        write_objects(avro_writer, tweet_reference)


def write_users(payload: dict):
    users = UserParser().parse(payload)
    avro_writer = AvroWriter(
        '/home/felix/twitter/twitter-crawler/avro/user/',
        '/home/felix/twitter/twitter-crawler/src/sch/user.avsc')
    write_objects(avro_writer, users)


def write_referenced_tweets(payload: dict, writer):
    referenced_tweets = ReferencedTweetsParser().parse(payload)
    write_objects(writer, referenced_tweets)


def write_tweets(payload: dict):
    tweets = TweetParser().parse(payload)
    avro_writer = AvroWriter(
        '/home/felix/twitter/twitter-crawler/avro/tweet/',
        '/home/felix/twitter/twitter-crawler/src/sch/tweet.avsc')
    write_referenced_tweets(payload, avro_writer)
    write_objects(avro_writer, tweets)


def get_data(payload: dict):
    write_tweets(payload)
    write_users(payload)
    write_references(payload)


def upload_data(uploader):
    uploader.upload(
        '/home/felix/twitter/twitter-crawler/avro/tweet/',
        'gs://monitor_debate/twitter/tweet')
    uploader.upload(
        '/home/felix/twitter/twitter-crawler/avro/user/',
        'gs://monitor_debate/twitter/user')
    uploader.upload(
        '/home/felix/twitter/twitter-crawler/avro/reference/',
        'gs://monitor_debate/twitter/tweet_join')


def open_csv_as_list(path: str, column: int, delimiter=',') -> list:
    with open(path) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=delimiter)
        return [row[column] for row in csv_reader]

def is_payload_empty(payload: dict) -> bool:
    return payload is None or 'data' not in payload

def main():
    google_cloud_uploader = GoogleCloudUploader(
        '/home/felix/twitter/twitter-crawler/src/cfg/key.json')
    csv_path = ['/home/felix/twitter/twitter-crawler/LulaUsers.csv', '/home/felix/twitter/twitter-crawler/BolsonaroUsers.csv']
    crawler = Crawler(Caller(), Keys())
    for csv_item in csv_path:
        for account in open_csv_as_list(csv_item, 0):
            query = f'\"http\" from:{account} lang:pt'
            print(f'[INFO] Running {query}.')
            for payload in crawler.full_search_tweets(query, max_results=500, start_time='2022-06-01T00:00:00Z'):
                if is_payload_empty(payload):
                    print(f'[INFO] Empty payload found. Ignoring {account}...')
                    continue
                get_data(payload)
                upload_data(google_cloud_uploader)


if __name__ == '__main__':
    # unittest.main()
    main()
