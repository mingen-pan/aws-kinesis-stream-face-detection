import boto3

from util import yaml_handler

env = yaml_handler('./aws_env.yaml')


# Kinesis Video Handler
class KVSHandler:
    def __init__(self, name):
        self.name = name
        self.client = boto3.client(
            'kinesisvideo',
            region_name=env['aws_default_region']
        )
        self.arn = None

    def create(self, media_type="video/h264", retention_hour=24):
        print("Creating Kinesis Video Stream: ", self.name)
        try:
            response = self.client.create_stream(
                StreamName=self.name,
                MediaType=media_type,
                DataRetentionInHours=retention_hour
            )
            self.arn = response["StreamARN"]
            print('Done...')
        except Exception as e:
            print(e)
        print("")

    def delete(self):
        print("Deleting Kinesis Video Stream: ", self.name)
        try:
            arn = self.get_arn()
            self.client.delete_stream(
                StreamARN=arn
            )
            print('Done...')
        except Exception as e:
            print(e)
        print("")

    def get_arn(self):
        if self.arn is not None:
            return self.arn
        return self.fetch_arn()

    def fetch_arn(self):
        response = self.client.describe_stream(
            StreamName=self.name,
        )
        self.arn = response["StreamInfo"]["StreamARN"]
        return self.arn


class KDSHandler:
    def __init__(self, name):
        self.name = name
        self.client = boto3.client(
            'kinesis',
            region_name=env['aws_default_region']
        )
        self.arn = None

    def create(self, shard=1):
        print("Deleting Kinesis Data Stream: ", self.name)
        try:
            self.client.create_stream(
                StreamName=self.name,
                ShardCount=shard
            )
            print('Done...')
        except Exception as e:
            print(e)
        print("")

    def delete(self):
        print("Deleting Kinesis Data Stream: ", self.name)
        try:
            self.client.delete_stream(
                StreamName=self.name,
                EnforceConsumerDeletion=True
            )
            print('Done...')
        except Exception as e:
            print(e)
        print("")

    def get_arn(self):
        if self.arn is not None:
            return self.arn
        return self.fetch_arn()

    def fetch_arn(self):
        response = self.client.describe_stream(
            StreamName=self.name,
            Limit=1,
        )
        self.arn = response["StreamDescription"]['StreamARN']
        return self.arn

    def query_status(self):
        response = self.client.describe_stream(
            StreamName=self.name,
            Limit=1
        )
        return response["StreamDescription"]["StreamStatus"]

    def create_kds_consumer(self, name):
        print("Creating Kinesis Data Stream Consumer: ", name)
        try:
            self.client.register_stream_consumer(
                StreamARN=self.get_arn(),
                ConsumerName=name
            )
            print('Done...')
        except Exception as e:
            print(e)
        print("")
