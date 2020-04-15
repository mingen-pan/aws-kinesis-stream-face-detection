import json
import unittest
import cv2

from dynamo_handler import DynamoHandler
from kvs_handler import KVSHandler, extract_face
from lambda_function import decode_base64_and_load_json, lambda_handler
from reko_handler import RekoHanlder
from s3_handler import S3Handler
from util import yaml_handler

env = yaml_handler('./aws_env.yaml')

response_json = """{
  "Records": [
    {
      "kinesis": {
        "kinesisSchemaVersion": "1.0",
        "partitionKey": "8585fc21-052b-45b5-9a13-55844a87eaa9",
        "sequenceNumber": "49605974038509424079760648011085574233984981398739484674",
        "data": "eyJJbnB1dEluZm9ybWF0aW9uIjp7IktpbmVzaXNWaWRlbyI6eyJTdHJlYW1Bcm4iOiJhcm46YXdzOmtpbmVzaXN2aWRlbzp1cy1lYXN0LTE6MDkwOTE4NTU2MjY1OnN0cmVhbS9tYWNib29rLWNhbWVyYS8xNTg2NTU1MDU0OTg5IiwiRnJhZ21lbnROdW1iZXIiOiI5MTM0Mzg1MjMzMzE4MTU3NTk5MzcyNjYxOTczMjAwNTcxNDkyODE2NTQ2MTI0MyIsIlNlcnZlclRpbWVzdGFtcCI6MS41ODY5MTYzNjAyNjVFOSwiUHJvZHVjZXJUaW1lc3RhbXAiOjEuNTg2OTE2MzU5OTc5RTksIkZyYW1lT2Zmc2V0SW5TZWNvbmRzIjowLjB9fSwiU3RyZWFtUHJvY2Vzc29ySW5mb3JtYXRpb24iOnsiU3RhdHVzIjoiUlVOTklORyJ9LCJGYWNlU2VhcmNoUmVzcG9uc2UiOlt7IkRldGVjdGVkRmFjZSI6eyJCb3VuZGluZ0JveCI6eyJIZWlnaHQiOjAuNDcwODg4ODIsIldpZHRoIjowLjI2MjA3MTA0LCJMZWZ0IjowLjMyMzEwODUyLCJUb3AiOjAuNTE3NTY3OX0sIkNvbmZpZGVuY2UiOjk5Ljk5OTk0LCJMYW5kbWFya3MiOlt7IlgiOjAuMzgzOTU3NTQsIlkiOjAuNjkwNTI0MDQsIlR5cGUiOiJleWVMZWZ0In0seyJYIjowLjUwODIzNTM0LCJZIjowLjY3OTI4MDM0LCJUeXBlIjoiZXllUmlnaHQifSx7IlgiOjAuNDA3NTg2NCwiWSI6MC44NzcyNzcxLCJUeXBlIjoibW91dGhMZWZ0In0seyJYIjowLjUwOTkwMzIsIlkiOjAuODY4MTczODQsIlR5cGUiOiJtb3V0aFJpZ2h0In0seyJYIjowLjQ1MTc1MzgsIlkiOjAuNzc0MDUzOTMsIlR5cGUiOiJub3NlIn1dLCJQb3NlIjp7IlBpdGNoIjotMy4yOTU5ODksIlJvbGwiOi02Ljk1MTUzMTQsIllhdyI6LTI1LjQyNzc3fSwiUXVhbGl0eSI6eyJCcmlnaHRuZXNzIjo3Ny4xNDE4NCwiU2hhcnBuZXNzIjoyNi4xNzczNjh9fSwiTWF0Y2hlZEZhY2VzIjpbeyJTaW1pbGFyaXR5Ijo5OS44NTY0OSwiRmFjZSI6eyJCb3VuZGluZ0JveCI6eyJIZWlnaHQiOjAuNTM2ODc4LCJXaWR0aCI6MC40OTI1NzIsIkxlZnQiOjAuMDkxNDA3NywiVG9wIjowLjI4NTUzM30sIkZhY2VJZCI6ImFjZTg1YTcwLTlhMTAtNGM5MS1hZmM5LTNiM2Y4N2MwODg3ZSIsIkNvbmZpZGVuY2UiOjEwMC4wLCJJbWFnZUlkIjoiMDYyYTBiZjMtODdlZC0zMWUzLWE0ZWYtZTY4NzdmOGVmMjQ2In19LHsiU2ltaWxhcml0eSI6OTkuODIyNDY0LCJGYWNlIjp7IkJvdW5kaW5nQm94Ijp7IkhlaWdodCI6MC42ODUwODYsIldpZHRoIjowLjUxNjk0MiwiTGVmdCI6MC4xNjAwMzEsIlRvcCI6MC4yNzQ1MzV9LCJGYWNlSWQiOiJiZTAzMjM5My05NjJiLTQ4YmQtOGMxZS1jMmU5ZDdmOTk5ZjAiLCJDb25maWRlbmNlIjo5OS45OTk2LCJJbWFnZUlkIjoiNzQ4MjA0ODUtM2FlNy0zZGE3LWEyY2ItYjM1YjJkNWFlNGYwIn19XX1dfQ==",
        "approximateArrivalTimestamp": 1586916363.192
      },
      "eventSource": "aws:kinesis",
      "eventVersion": "1.0",
      "eventID": "shardId-000000000000:49605974038509424079760648011085574233984981398739484674",
      "eventName": "aws:kinesis:record",
      "invokeIdentityArn": "arn:aws:iam::090918556265:role/LambdaKinesis",
      "awsRegion": "us-east-1",
      "eventSourceARN": "arn:aws:kinesis:us-east-1:090918556265:stream/face-stream/consumer/face_consumer:1586654788"
    }
  ]
}
"""


class KVSTest(unittest.TestCase):

    def test_lambda(self):
        event = json.loads(response_json)
        lambda_handler(event, None)

    def test_print_image(self):
        event = json.loads(response_json)
        handler = KVSHandler(env["arn_kvs"])
        s3_handler = S3Handler("visitor-images")
        reko_handler = RekoHanlder("Faces", "FaceDetect")
        dynamo_handler = DynamoHandler("visitors")

        record = None
        for _, r in enumerate(event["Records"]):
            record = r
            break
        if record is None:
            return

        face_recognition_record = decode_base64_and_load_json(record["kinesis"]["data"])
        ProducerTimestamp = face_recognition_record["InputInformation"]["KinesisVideo"]["ProducerTimestamp"]
        image = handler.get_image_from_stream(ProducerTimestamp)
        # cv2.imwrite("image.jpg", image)
        print(image.shape)

        for face_search_response in face_recognition_record["FaceSearchResponse"]:
            nose = face_search_response["DetectedFace"]["Landmarks"][-1]
            print(nose)
            nose_xy = (int(nose["X"] * image.shape[1]), int(nose["Y"] * image.shape[0]))
            print(nose_xy)
            image = cv2.circle(image,
                               nose_xy,
                               10, (255, 0, 0), 2
                               )
            box = face_search_response["DetectedFace"]["BoundingBox"]
            print(box)
            face_image = extract_face(image, box, box_ratio=2)

            print("face: ", face_image.shape)
            cv2.imwrite("/tmp/image.jpg", image)
            cv2.imwrite("/tmp/face.jpg", face_image)
            s3_handler.upload("/tmp/face.jpg", "face.jpg")
            faces = reko_handler.index_faces(s3_handler.bucket_name, "face.jpg")
            if len(faces) == 0:
                return
            face_id = faces[0]["FaceId"]
            print(face_id)

            if dynamo_handler.exist(face_id):
                dynamo_handler.append_image(face_id, s3_handler.bucket_name, "face.jpg")
            else:
                dynamo_handler.create_image_record(face_id, s3_handler.bucket_name, "face.jpg")
            break
