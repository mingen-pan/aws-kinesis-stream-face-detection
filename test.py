import json
import unittest
import cv2
import numpy as np

from dynamo_handler import DynamoHandler
from kvs_handler import KVSHandler, extract_face
from reko_handler import RekoHanlder
from s3_handler import S3Handler
from util import yaml_handler

env = yaml_handler('./aws_env.yaml')

response_json = """{"InputInformation": {"KinesisVideo": {"StreamArn": 
"arn:aws:kinesisvideo:us-east-1:090918556265:stream/macbook-camera/1586555054989", "FragmentNumber": 
"91343852333181541341309040055593237511723545694", "ServerTimestamp": 1586738207.792, "ProducerTimestamp": 
1586738207.249, "FrameOffsetInSeconds": 0.0020000000949949026}}, "StreamProcessorInformation": {"Status": "RUNNING"}, 
"FaceSearchResponse": [{"DetectedFace": {"BoundingBox": {"Height": 0.4602379, "Width": 0.24975915, 
"Left": 0.38193148, "Top": 0.5065514}, "Confidence": 99.999985, "Landmarks": [{"X": 0.4419924, "Y": 0.6689897, 
"Type": "eyeLeft"}, {"X": 0.5569588, "Y": 0.6684084, "Type": "eyeRight"}, {"X": 0.45489523, "Y": 0.84728855, 
"Type": "mouthLeft"}, {"X": 0.5495061, "Y": 0.84689164, "Type": "mouthRight"}, {"X": 0.5027462, "Y": 0.7621452, 
"Type": "nose"}], "Pose": {"Pitch": -3.3248196, "Roll": -1.7610033, "Yaw": -10.993803}, "Quality": {"Brightness": 
84.072655, "Sharpness": 73.3221}}, "MatchedFaces": [{"Similarity": 99.89643, "Face": {"BoundingBox": {"Height": 
0.685086, "Width": 0.516942, "Left": 0.160031, "Top": 0.274535}, "FaceId": "be032393-962b-48bd-8c1e-c2e9d7f999f0", 
"Confidence": 99.9996, "ImageId": "74820485-3ae7-3da7-a2cb-b35b2d5ae4f0"}}]}]} """


class KVSTest(unittest.TestCase):

    def test_print_image(self):
        handler = KVSHandler(env["arn_kvs"])
        s3_handler = S3Handler("visitor-images")
        reko_handler = RekoHanlder("Faces", "FaceDetect")
        dynamo_handler = DynamoHandler("visitors")

        face_recognition_record = json.loads(response_json)
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
            cv2.imwrite("./tmp/image.jpg", image)
            cv2.imwrite("./tmp/face.jpg", face_image)
            s3_handler.upload("./tmp/face.jpg", "face.jpg")
            face_id = reko_handler.index_face(s3_handler.bucket_name, "face.jpg")
            print(face_id)

            if dynamo_handler.exist(face_id):
                dynamo_handler.append_image(face_id, s3_handler.bucket_name, "face.jpg")
            else:
                dynamo_handler.create_image_record(face_id, s3_handler.bucket_name, "face.jpg")
            break
