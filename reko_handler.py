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

    def index_face(self, bucket, image):
        response = self.client.index_faces(CollectionId=self.collection_id,
                                           Image={'S3Object': {'Bucket': bucket, 'Name': image}},
                                           MaxFaces=1,
                                           QualityFilter="AUTO",
                                           DetectionAttributes=['ALL'])

        for record in response["FaceRecords"]:
            return record["Face"]["FaceId"]
        return None
