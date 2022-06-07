import downloader as dw
import parser as pr
import exporter as ep



for payload in dw.Crawler(dw.Caller(), dw.Keys()).full_search_tweets('retweets_of:jairbolsonaro lang:pt', tweet_amount=10):
    tweets = pr.Parser(pr.CsvFormat()).parse(payload)
    google_cloud_exporter = ep.GoogleCloudExporter('C:/Users/GabrielFelix/Documents/GitHub/twitter-crawler/src/cfg/keys.json')
    for tweet in tweets:
        google_cloud_exporter.json_to_avro(tweet, 'C:/Users/GabrielFelix/Documents/GitHub/twitter-crawler/src/sch/tweet.avsc')