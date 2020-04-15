import json
from base64 import b64decode

import cv2

from dynamo_handler import DynamoHandler
from kvs_handler import KVSHandler, extract_face
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
    for i, record in enumerate(event["Records"]):
        face_recognition_record = decode_base64_and_load_json(record["kinesis"]["data"])

        # only process the record with faces
        if len(face_recognition_record["FaceSearchResponse"]) == 0:
            continue

        arn_kvs = face_recognition_record["InputInformation"]["StreamArn"]
        kvs_handler = KVSHandler(arn_kvs)
        s3_handler = S3Handler(bucket)
        reko_handler = RekoHanlder(collection_id, stream_processor_name)
        dynamo_handler = DynamoHandler(db_name)

        timestamp = face_recognition_record["InputInformation"]["ProducerTimestamp"]
        image = kvs_handler.get_image_from_stream(timestamp)

        for face_search_response in face_recognition_record["FaceSearchResponse"]:
            face_image = extract_face(image, face_search_response["DetectedFace"]["BoundingBox"], box_ratio=2)
            cv2.imwrite("./tmp/face.jpg", face_image)

            # key = producer_timestamp + _ + i
            key = str(int(timestamp)) + '_' + str(i) + ".jpg"
            s3_handler.upload("./tmp/face.jpg", key)
            face_id = reko_handler.index_face(bucket, key)
            if face_id is None:
                continue
            print("face id: ", face_id)

            if dynamo_handler.exist(face_id):
                dynamo_handler.append_image(face_id, s3_handler.bucket_name, key)
            else:
                dynamo_handler.create_image_record(face_id, s3_handler.bucket_name, key)

        # only process one frame
        break

    return {
        'statusCode': 200
    }
