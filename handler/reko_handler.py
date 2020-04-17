import boto3

from util import yaml_handler

env = yaml_handler('./aws_env.yaml')


class RekoHanlder:
    def __init__(self, collection_id, stream_processor_name):
        self.collection_id = collection_id
        self.stream_processor_name = stream_processor_name
        self.client = boto3.client(
            'rekognition',
            region_name=env['aws_default_region']
        )

    def create_collection(self):
        try:
            print('Creating collection:' + self.collection_id)
            self.client.create_collection(CollectionId=self.collection_id)
            print('Done...')
        except Exception as e:
            print(e)
        print("")

    def create_rekognition_stream_processor(self, arn_kvs, arn_kds,
                                            match_threshold=80):
        print('Creating Rekognition Stream Processor: ' + self.stream_processor_name)
        try:
            self.client.create_stream_processor(
                Input={
                    'KinesisVideoStream': {
                        'Arn': arn_kvs
                    }
                },
                Output={
                    'KinesisDataStream': {
                        'Arn': arn_kds
                    }
                },
                Name=self.stream_processor_name,
                Settings={
                    'FaceSearch': {
                        'CollectionId': self.collection_id,
                        'FaceMatchThreshold': match_threshold
                    }
                },
                RoleArn=env["arn_recognition"]
            )
            print('Done...')
        except Exception as e:
            print(e)
        print("")

    def stop_rekognition_stream_processor(self):
        print('Stopping Rekognition Stream Processor: ', self.stream_processor_name)
        try:
            self.client.stop_stream_processor(
                Name=self.stream_processor_name
            )
            print('Done...')
        except Exception as e:
            print(e)
        print("")

    def delete_rekognition_stream_processor(self):
        print('Deleting Rekognition Stream Processor: ', self.stream_processor_name)
        try:
            self.client.delete_stream_processor(
                Name=self.stream_processor_name
            )
            print('Done...')
        except Exception as e:
            print(e)
        print("")

    def delete_collection(self):
        print("Delete Collection: ", self.collection_id)
        try:
            response = self.client.delete_collection(
                CollectionId=self.collection_id
            )
            print("Status Code: ", response["StatusCode"])
            print('Done...')
        except Exception as e:
            print(e)
        print("")

    def start_stream_processor(self):
        print("Starting Rekoginition Stream Processor:", self.stream_processor_name)
        try:
            self.client.start_stream_processor(Name=self.stream_processor_name)
            print('Done...')
        except Exception as e:
            print(e)
        print("")

    def index_faces(self, bucket, image):
        response = self.client.index_faces(CollectionId=self.collection_id,
                                           Image={'S3Object': {'Bucket': bucket, 'Name': image}},
                                           MaxFaces=10,
                                           QualityFilter="AUTO",
                                           DetectionAttributes=['ALL'])

        return [record["Face"] for record in response["FaceRecords"]]
