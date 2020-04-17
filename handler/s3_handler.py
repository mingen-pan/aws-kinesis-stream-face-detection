import boto3

from util import yaml_handler

env = yaml_handler('./aws_env.yaml')


class S3Handler:
    def __init__(self, bucket):
        self.client = boto3.client(
            's3',
            region_name=env['aws_default_region']
        )
        self.bucket_name = bucket

    def create(self):
        print("Creating S3: ", self.bucket_name)
        try:
            response = self.client.create_bucket(
                ACL='private',
                Bucket=self.bucket_name
                # CreateBucketConfiguration={
                #     'LocationConstraint': env['aws_default_region']
                # }
            )
            print("Bucket Created at: ", response["Location"])
            print('Done...')
        except Exception as e:
            print(e)
        print("")

    def delete(self):
        print("Deleting S3: ", self.bucket_name)
        try:
            self.client.delete_bucket(
                Bucket=self.bucket_name
            )
            print('Done...')
        except Exception as e:
            print(e)
        print("")

    def empty(self):
        print("Emptying S3: ", self.bucket_name)
        try:
            s3 = boto3.resource(
                's3',
                region_name=env['aws_default_region']
            )
            bucket = s3.Bucket(self.bucket_name)
            # suggested by Jordon Philips
            bucket.objects.all().delete()
            print('Done...')
        except Exception as e:
            print(e)
        print("")

    def upload(self, filename, key):
        self.client.upload_file(filename, self.bucket_name, key)
