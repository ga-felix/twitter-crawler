from downloader import Crawler, Caller, Keys
from parser import TweetParser, UserParser
from uploader import GoogleCloudUploader
from writer import AvroWriter


for payload in Crawler(Caller(), Keys()).full_search_tweets('retweets_of:jairbolsonaro lang:pt', tweet_amount=10):
    tweets = TweetParser().parse(payload)
    users = UserParser().parse(payload)
    google_cloud_exporter = GoogleCloudUploader(
        '/home/gafelix/Documentos/Git/twitter-crawler/src/cfg/key.json')
    avro_writer = AvroWriter(
        '/home/gafelix/Documentos/Git/twitter-crawler/avro/tweet/')
    for tweet in tweets:
        avro_writer.json_to_avro(
            tweet, '/home/gafelix/Documentos/Git/twitter-crawler/src/sch/tweet.avsc')
    avro_writer = AvroWriter(
        '/home/gafelix/Documentos/Git/twitter-crawler/avro/user/')
    for user in users:
        avro_writer.json_to_avro(
            user, '/home/gafelix/Documentos/Git/twitter-crawler/src/sch/user.avsc')
    google_cloud_exporter.upload(
        '/home/gafelix/Documentos/Git/twitter-crawler/avro/tweet/',
        'gs://monitor_debate/twitter/tweet')
    google_cloud_exporter.upload(
        '/home/gafelix/Documentos/Git/twitter-crawler/avro/user/',
        'gs://monitor_debate/twitter/user')
