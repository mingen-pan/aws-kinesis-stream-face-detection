import boto3

from util import yaml_handler

env = yaml_handler('./aws_env.yaml')


class DynamoHandler:
    def __init__(self, table, index="faceId"):
        self.table_name = table
        self.index = index
        self.client = boto3.client(
            "dynamodb",
            region_name=env['aws_default_region'],
            aws_access_key_id=env['aws_access_key_id'],
            aws_secret_access_key=env['aws_secret_access_key'],
            aws_session_token=env['aws_session_token']
        )

    def create(self, index="faceId"):
        self.index = index
        self.client.create_table(
            TableName=self.table_name,
            KeySchema=[
                {
                    'AttributeName': index,
                    'KeyType': 'HASH'
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': index,
                    'AttributeType': 'S'
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 25,
                'WriteCapacityUnits': 25
            }
        )
        print(f"Table {self.table_name} created")

    def create_image_record(self, face_id, bucket, key):
        item = {
            "faceId": {
                "S": face_id
            },
            "photos": {
                "L": [
                    {
                        "M": {
                            "objectKey": {
                                "S": key
                            },
                            "bucket": {
                                "S": bucket
                            }
                        }
                    }
                ]
            }
        }
        self.client.put_item(
            Item=item,
            TableName=self.table_name,
        )

    def append_image(self, face_id, bucket, key):
        self.client.update_item(
            TableName=self.table_name,
            Key={
                self.index: {
                    'S': face_id
                }
            },
            UpdateExpression="SET photos = list_append(photos, :i)",
            ExpressionAttributeValues={
                ':i': {
                    "L": [
                        {
                            "M": {
                                "objectKey": {
                                    "S": key
                                },
                                "bucket": {
                                    "S": bucket
                                }
                            }
                        }
                    ]
                },
            }
        )

    def exist(self, face_id):
        try:
            self.client.get_item(
                TableName=self.table_name,
                Key={
                    self.index: {
                        'S': face_id
                    }
                },
                ProjectionExpression=f"{self.index}",
            )
            return True
        except Exception as e:
            if e.__class__.__name__ == "ResourceNotFoundException":
                return False
        return None
