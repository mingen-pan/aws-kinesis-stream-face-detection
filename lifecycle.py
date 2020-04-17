from time import sleep

import boto3

from handler.dynamo_handler import DynamoHandler
from handler.kinesis_handler import KVSHandler, KDSHandler
from handler.reko_handler import RekoHanlder
from handler.s3_handler import S3Handler
from util import yaml_handler

env = yaml_handler('./aws_env.yaml')

class LifeCycleController:
    def __init__(self):
        # name of Kinesis video stream
        kv_name = "macbook-camera"
        kds_name = "face-stream"
        collection_id = 'Faces'
        stream_processor_name = 'FaceDetect'
        self.consumer_name = 'face_consumer'
        bucket = "visitor-images"
        db_table = "visitors"

        self.kvs_handler = KVSHandler(kv_name)
        self.kds_handler = KDSHandler(kds_name)
        self.reko_handler = RekoHanlder(collection_id, stream_processor_name)
        self.s3_handler = S3Handler(bucket)
        self.dynamo_handler = DynamoHandler(db_table)

    def create(self):
        self.kvs_handler.create()
        self.kds_handler.create()
        self.reko_handler.create_collection()
        self.reko_handler.create_rekognition_stream_processor(self.kvs_handler.get_arn(), self.kds_handler.get_arn())
        # start the stream processor
        self.reko_handler.start_stream_processor()

        # waiting for KDS to start
        while True:
            status = self.kds_handler.query_status()
            if status == "ACTIVE" or status == "UPDATING":
                break
            sleep(1)

        self.kds_handler.create_kds_consumer(self.consumer_name)
        self.s3_handler.create()
        self.dynamo_handler.create()

    def delete(self, empty_s3=True):
        self.reko_handler.stop_rekognition_stream_processor()
        self.reko_handler.delete_rekognition_stream_processor()
        self.reko_handler.delete_collection()
        # Consumer will be deleted accordingly
        self.kvs_handler.delete()
        self.kds_handler.delete()
        self.dynamo_handler.delete()
        if empty_s3:
            self.s3_handler.empty()
        else:
            self.s3_handler.delete()


if __name__ == "__main__":
    controller = LifeCycleController()
    controller.create()
    # controller.delete()
