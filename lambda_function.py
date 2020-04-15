import json
from base64 import b64decode

import sys

sys.path.insert(0, '/opt/python')
import cv2

from dynamo_handler import DynamoHandler
from kvs_handler import KVSHandler, extract_face, TMP_DIR
from reko_handler import RekoHanlder
from s3_handler import S3Handler
from util import yaml_handler

env = yaml_handler('./aws_env.yaml')

collection_id = 'Faces'
stream_processor_name = 'FaceDetect'
bucket = "visitor-images"
db_name = "visitors"


def decode_base64_and_load_json(data):
    data_bytes = b64decode(data)
    data_str = data_bytes.decode('utf8')
    return json.loads(data_str)


def lambda_handler(event, context):
    print(event)
    for record in event["Records"]:
        face_recognition_record = decode_base64_and_load_json(record["kinesis"]["data"])
        print(face_recognition_record)
        # only process the record with faces
        if len(face_recognition_record["FaceSearchResponse"]) == 0:
            continue

        # initialize the handler
        arn_kvs = face_recognition_record["InputInformation"]["KinesisVideo"]["StreamArn"]
        kvs_handler = KVSHandler(arn_kvs)
        s3_handler = S3Handler(bucket)
        reko_handler = RekoHanlder(collection_id, stream_processor_name)
        dynamo_handler = DynamoHandler(db_name)

        # get the frame image
        timestamp = face_recognition_record["InputInformation"]["KinesisVideo"]["ProducerTimestamp"]
        image = kvs_handler.get_image_from_stream(timestamp)

        # upload the frame image to S3 for indexing faces
        # key = producer_timestamp + _ + frame
        image_key = str(int(timestamp)) + "_frame.jpg"
        cv2.imwrite(TMP_DIR + f"/{image_key}", image)
        s3_handler.upload(TMP_DIR + f"/{image_key}", image_key)
        faces = reko_handler.index_faces(bucket, image_key)

        for i, face in enumerate(faces):
            face_id = face["FaceId"]

            if face_id is None:
                continue
            print("face id: ", face_id)

            # key = producer_timestamp + _ + i
            face_image = extract_face(image, face["BoundingBox"], box_ratio=2)
            key = str(int(timestamp)) + '_' + str(i) + ".jpg"
            cv2.imwrite(TMP_DIR + f"/{key}", face_image)
            s3_handler.upload(TMP_DIR + f"/{key}", key)

            if dynamo_handler.exist(face_id):
                dynamo_handler.append_image(face_id, s3_handler.bucket_name, key)
            else:
                dynamo_handler.create_image_record(face_id, s3_handler.bucket_name, key)

        # only process one frame
        break

    return {
        'statusCode': 200
    }
