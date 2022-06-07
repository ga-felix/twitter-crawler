import os
import avro.schema
from avro.datafile import DataFileWriter
from avro.io import DatumWriter
import json


class Exporter():

    def __init__(self, exporter):
        self.exporter = exporter

    def upload(self, filename):
        self.exporter.upload(filename)


class GoogleCloudExporter():

    def __init__(self, keys_path: str):
        login_command = 'gcloud auth activate-service-account --key-file={}'.format(
            keys_path)
        os.system(login_command)

    def json_to_avro(self, tweet, schema_path):
        schema = avro.schema.parse(open(schema_path, 'rb').read())
        writer = DataFileWriter(open(tweet['id'] + '.avro', 'w+'), DatumWriter(), schema)
        writer.append(json.dumps(tweet, ensure_ascii=False))

    def upload(self, filename):
        upload_command_info = f'gsutil cp {filename} gs://monitor_debate/youtube/canal/info/'
