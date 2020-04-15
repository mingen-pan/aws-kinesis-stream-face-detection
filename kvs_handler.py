import boto3
import cv2

from util import yaml_handler
from datetime import datetime

env = yaml_handler('./aws_env.yaml')

TMP_DIR = "/tmp"

def get_endpoint(arn):
    kv_client = boto3.client(
        'kinesisvideo',
        region_name=env['aws_default_region']
    )
    response = kv_client.get_data_endpoint(
        StreamARN=arn,
        APIName='GET_MEDIA'
    )
    return response['DataEndpoint']


def extract_frame(payload):
    with open(f'{TMP_DIR}/stream.mkv', 'wb+') as f:
        streamBody = payload.read(1024 * 128)
        f.write(streamBody)
        # use openCV to get a frame
        cap = cv2.VideoCapture(f'{TMP_DIR}/stream.mkv')
        succeeded, frame = cap.read()
        if not succeeded:
            raise RuntimeError("cannot read a frame")
        return frame


def extract_face(image, box, box_ratio=1):
    top = int(max(0, box["Top"] - 0.5 * (box_ratio - 1) * box["Height"]) * image.shape[0])
    left = int(max(0, box["Left"] - 0.5 * (box_ratio - 1) * box["Width"]) * image.shape[1])
    height = int((box["Height"] * box_ratio) * image.shape[0])
    width = int((box["Width"] * box_ratio) * image.shape[1])
    return image[top:top + height, left: left + width]


class KVSHandler:
    def __init__(self, arn):
        self.client = boto3.client(
            'kinesis-video-media',
            region_name=env['aws_default_region'],
            endpoint_url=get_endpoint(arn)
        )
        self.stream_arn = arn

    def get_image_from_stream(self, timestamp, selector="PRODUCER_TIMESTAMP"):
        dt = datetime.fromtimestamp(timestamp)
        response = self.client.get_media(
            StreamARN=self.stream_arn,
            StartSelector={
                'StartSelectorType': selector,
                'StartTimestamp': dt
            }
        )
        print('ContentType: ', response["ContentType"])
        payload = response["Payload"]
        return extract_frame(payload)
