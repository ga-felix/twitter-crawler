import avro.schema
from avro.datafile import DataFileWriter
from avro.io import DatumWriter
from os import makedirs


class AvroWriter:

    def __init__(self, folder_path, schema_path):
        self.folder_path = folder_path
        self.schema_path = schema_path

    def create_export_dir(self):
        makedirs(self.folder_path, exist_ok=True)

    def get_schema(self, schema_path):
        return avro.schema.parse(open(schema_path, 'rb').read())

    def write_avro_file(self, dict_file, schema):
        writer = DataFileWriter(
            open(self.folder_path + dict_file['id'] + '.avro', 'wb'), DatumWriter(), schema)
        writer.append(dict_file)
        writer.close()

    def write(self, dict_file):
        self.create_export_dir()
        schema = self.get_schema(self.schema_path)
        self.write_avro_file(dict_file, schema)
