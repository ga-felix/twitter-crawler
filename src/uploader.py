from os import listdir, system
from os.path import isfile, join


class GoogleCloudUploader:

    def __init__(self, keys_path: str):
        login_command = f'gcloud auth activate-service-account --key-file={keys_path}'
        system(login_command)

    def list_files(self, folder_path):
        return [f for f in listdir(folder_path) if isfile(join(folder_path, f))]

    def upload(self, folder_path, target_path):
        for filename in self.list_files(folder_path):
            upload_command = f'gsutil -m cp {folder_path + filename} {target_path}'
            system(upload_command)
