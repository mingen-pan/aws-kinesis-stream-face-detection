import boto3

from util import yaml_handler

env = yaml_handler('./aws_env.yaml')


class S3Handler:
    def __init__(self, bucket):
        self.client = boto3.client(
            's3'
        )
        self.bucket_name = bucket

    def create(self):
        try:
            response = self.client.create_bucket(
                ACL='private',
                Bucket=self.bucket_name
                # CreateBucketConfiguration={
                #     'LocationConstraint': env['aws_default_region']
                # }
            )
            print("Bucket Created at: ", response["Location"])
        except Exception as e:
            print(e)

    def delete(self):
        try:
            self.client.delete_bucket(
                Bucket=self.bucket_name
            )
        except Exception as e:
            print(e)

    def upload(self, filename, key):
        self.client.upload_file(filename, self.bucket_name, key)
