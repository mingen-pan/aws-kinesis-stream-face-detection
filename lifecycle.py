import boto3

from s3_handler import S3Handler
from util import yaml_handler

env = yaml_handler('./aws_env.yaml')


class LifeCycleController:
    def __init__(self):
        self.collection_id = 'Faces'
        self.stream_processor_name = 'FaceDetect'
        self.consumer_name = 'face_consumer'
        bucket = "visitor-images"

        self.rekoginition_client = boto3.client(
            'rekognition',
            region_name=env['aws_default_region'],
            aws_access_key_id=env['aws_access_key_id'],
            aws_secret_access_key=env['aws_secret_access_key'],
            aws_session_token=env['aws_session_token']
        )

        self.kinesis_client = boto3.client(
            'kinesis',
            region_name=env['aws_default_region'],
            aws_access_key_id=env['aws_access_key_id'],
            aws_secret_access_key=env['aws_secret_access_key'],
            aws_session_token=env['aws_session_token']
        )

        self.s3_handler = S3Handler(bucket)

    def create(self):
        self.s3_handler.create()

        try:
            create_collection(self.rekoginition_client, self.collection_id)
        except Exception as e:
            print(e)

        try:
            create_rekognition_stream_processor(self.rekoginition_client, self.stream_processor_name,
                                                self.collection_id)
        except Exception as e:
            print(e)

        try:
            create_kds_consumer(self.kinesis_client, self.consumer_name)
        except Exception as e:
            print(e)

    def start(self):
        self.rekoginition_client.start_stream_processor(Name=self.stream_processor_name)

    def query(self):
        print(
            self.rekoginition_client.describe_stream_processor(Name=self.stream_processor_name)
        )

    def delete(self):
        self.rekoginition_client.delete_stream_processor(Name=self.stream_processor_name)
        self.s3_handler.delete()


def create_collection(client, collection_id):
    # Create a collection
    print('Creating collection:' + collection_id)
    response = client.create_collection(CollectionId=collection_id)
    print('Collection ARN: ' + response['CollectionArn'])
    print('Status code: ' + str(response['StatusCode']))
    print('Done...')
    print("")


def create_rekognition_stream_processor(client, name, collection_id, match_threshold=80):
    response = client.create_stream_processor(
        Input={
            'KinesisVideoStream': {
                'Arn': env["arn_kvs"]
            }
        },
        Output={
            'KinesisDataStream': {
                'Arn': env["arn_kds"]
            }
        },
        Name=name,
        Settings={
            'FaceSearch': {
                'CollectionId': collection_id,
                'FaceMatchThreshold': match_threshold
            }
        },
        RoleArn=env["arn_recognition"]
    )
    print('StreamProcessor ARN: ' + response['StreamProcessorArn'])
    print('Done...')
    print("")


def create_kds_consumer(client, name):
    response = client.register_stream_consumer(
        StreamARN=env["arn_kds"],
        ConsumerName=name
    )
    print('Consumer ARN: ' + response['ConsumerARN'])
    print('Done...')
    print("")


if __name__ == "__main__":
    controller = LifeCycleController()
    controller.create()
