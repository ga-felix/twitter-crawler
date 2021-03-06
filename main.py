from src import *
# from test import *
# import unittest
import csv
import datetime


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
    try:
        write_references(payload)
        write_tweets(payload)
        write_users(payload)
    except TypeError as err:
        message = 'TypeError has occurred likely to private referenced tweet'
        errors = len(payload['errors'])
        print(f'[ERROR] {message} {errors} payload errors found.')


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
    crawler = Crawler(Caller(), Keys())
    query = f'((Assassino OR #BolsonarismoMata OR Polarizacao OR Jorge OR Garanho OR Marcelo OR Arruda) is:retweet OR (from:jairbolsonaro)) lang:pt'
    now = datetime.datetime.now()
    print(f'[INFO] {now}: Running {query}.')
    payloads = 0
    for payload in crawler.full_search_tweets(query, max_results=500, pages=200, start_time='2022-07-10T10:00:00Z'):
        try:
            get_data(payload)
        except Exception as e:
            with open('log.txt', 'a+') as f:
                now = datetime.datetime.now()
                message = f'[ERROR] {now}: {e}'
                print(message)
                f.write(message)
            f.close()
        payloads += 1
        now = datetime.datetime.now()
        print(f'[INFO] {now}: {payloads} have been written.')
    upload_data(google_cloud_uploader)


if __name__ == '__main__':
    # unittest.main()
    main()
