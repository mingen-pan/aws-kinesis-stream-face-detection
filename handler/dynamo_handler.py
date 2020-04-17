import boto3

from util import yaml_handler

env = yaml_handler('./aws_env.yaml')


class DynamoHandler:
    def __init__(self, table, index="faceId"):
        self.table_name = table
        self.index = index
        self.client = boto3.client(
            "dynamodb",
            region_name=env['aws_default_region']
        )

    def create(self, index="faceId"):
        print("Creating DynamoDB: ", self.table_name)
        try:
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
            print('Done...')
        except Exception as e:
            print(e)
        print("")

    def delete(self):
        print("Deleting DynamoDB: ", self.table_name)
        try:
            self.client.delete_table(
                TableName=self.table_name
            )
            print('Done...')
        except Exception as e:
            print(e)
        print("")

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
            response = self.client.get_item(
                TableName=self.table_name,
                Key={
                    self.index: {
                        'S': face_id
                    }
                },
                ProjectionExpression=f"{self.index}",
            )
            return "Item" in response
        except Exception as e:
            print(e)
            if e.__class__.__name__ == "ResourceNotFoundException":
                return False
            else:
                raise e
