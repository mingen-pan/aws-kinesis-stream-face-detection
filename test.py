import json
import unittest
import cv2

from handler.dynamo_handler import DynamoHandler
from handler.kv_media_handler import KVMediaHandler, extract_face
from lambda_function import decode_base64_and_load_json, lambda_handler
from handler.reko_handler import RekoHanlder
from handler.s3_handler import S3Handler
from util import yaml_handler

env = yaml_handler('./aws_env.yaml')

response_json = """
{"Records": [{"kinesis": {"kinesisSchemaVersion": "1.0", "partitionKey": "aad81646-ccda-45c4-9ed0-c4130c4a4a10", "sequenceNumber": "49605974038509424079760656864102544007978456824330321922", "data": "eyJJbnB1dEluZm9ybWF0aW9uIjp7IktpbmVzaXNWaWRlbyI6eyJTdHJlYW1Bcm4iOiJhcm46YXdzOmtpbmVzaXN2aWRlbzp1cy1lYXN0LTE6MDkwOTE4NTU2MjY1OnN0cmVhbS9tYWNib29rLWNhbWVyYS8xNTg2NTU1MDU0OTg5IiwiRnJhZ21lbnROdW1iZXIiOiI5MTM0Mzg1MjMzMzE4MTY3MDA4MjEyMTM2NTU3ODA1MTk3Njg5ODQ0MDI4NTU4MCIsIlNlcnZlclRpbWVzdGFtcCI6MS41ODY5MzA2OTcxODNFOSwiUHJvZHVjZXJUaW1lc3RhbXAiOjEuNTg2OTMwNjk2MzM2RTksIkZyYW1lT2Zmc2V0SW5TZWNvbmRzIjozLjAwMDk5OTkyNzUyMDc1Mn19LCJTdHJlYW1Qcm9jZXNzb3JJbmZvcm1hdGlvbiI6eyJTdGF0dXMiOiJSVU5OSU5HIn0sIkZhY2VTZWFyY2hSZXNwb25zZSI6W3siRGV0ZWN0ZWRGYWNlIjp7IkJvdW5kaW5nQm94Ijp7IkhlaWdodCI6MC41MDUwNzEzNCwiV2lkdGgiOjAuMjk0MjMzMDIsIkxlZnQiOjAuMzc1MzYxNDQsIlRvcCI6MC40NjAwNjc3fSwiQ29uZmlkZW5jZSI6OTkuOTk5OTYsIkxhbmRtYXJrcyI6W3siWCI6MC40Mzg3MjU3LCJZIjowLjYxOTU0MTc2LCJUeXBlIjoiZXllTGVmdCJ9LHsiWCI6MC41NzYyNjU4LCJZIjowLjYxMzk0OTY2LCJUeXBlIjoiZXllUmlnaHQifSx7IlgiOjAuNDU4MjU2MjQsIlkiOjAuODMwMzUxNzcsIlR5cGUiOiJtb3V0aExlZnQifSx7IlgiOjAuNTcxOTE2OCwiWSI6MC44MjU1MzI1NiwiVHlwZSI6Im1vdXRoUmlnaHQifSx7IlgiOjAuNTE3ODE4OCwiWSI6MC43MzEzNjIxNiwiVHlwZSI6Im5vc2UifV0sIlBvc2UiOnsiUGl0Y2giOjQuNzA3MzQwNywiUm9sbCI6LTMuNDQ4Mjg5NiwiWWF3IjotMC42MTc1MjQ3fSwiUXVhbGl0eSI6eyJCcmlnaHRuZXNzIjo3OC4zNjgxNCwiU2hhcnBuZXNzIjo3OC42NDM1fX0sIk1hdGNoZWRGYWNlcyI6W3siU2ltaWxhcml0eSI6OTkuOTkyOTksIkZhY2UiOnsiQm91bmRpbmdCb3giOnsiSGVpZ2h0IjowLjUxMjA3OCwiV2lkdGgiOjAuMjc3NDYsIkxlZnQiOjAuMzcwMiwiVG9wIjowLjQzMzg3Mn0sIkZhY2VJZCI6Ijg5ZWNkZGJhLWZiZTctNDdjYy1hMjFhLTNlNzVmMDZmNGEyOCIsIkNvbmZpZGVuY2UiOjk5Ljk5OTksIkltYWdlSWQiOiIzM2MyNzMwOC1mNTc3LTNiZmEtYTg5My1hMjllMTRlMjJiNTUifX0seyJTaW1pbGFyaXR5Ijo5OS45ODk5MSwiRmFjZSI6eyJCb3VuZGluZ0JveCI6eyJIZWlnaHQiOjAuNDYzMzUyLCJXaWR0aCI6MC4yNTI5MTMsIkxlZnQiOjAuMzk4OTI4LCJUb3AiOjAuNTE0MTcyfSwiRmFjZUlkIjoiM2FmNGMxZjAtMWM1Ny00NTZmLTkyNjYtYzMwOGI5MzZjMWVmIiwiQ29uZmlkZW5jZSI6MTAwLjAsIkltYWdlSWQiOiJhODc5MjBhMC0wNWRhLTM4OWEtOGY4MC1mNzc5MmRjMDFkMDcifX0seyJTaW1pbGFyaXR5Ijo5OS45ODgxNiwiRmFjZSI6eyJCb3VuZGluZ0JveCI6eyJIZWlnaHQiOjAuNTE0MzYzLCJXaWR0aCI6MC4yODI3MzksIkxlZnQiOjAuNDQ0MjAzLCJUb3AiOjAuNDc4NDQxfSwiRmFjZUlkIjoiMjQzMDFlMmYtZTJlOC00N2VmLWE2MjktNDU2OWI2ZmUzM2MwIiwiQ29uZmlkZW5jZSI6MTAwLjAsIkltYWdlSWQiOiIzYTBiYjVkZC01OTFlLTNmYzEtYTM3MS1jNTljMTI2ZmIyODIifX0seyJTaW1pbGFyaXR5Ijo5OS45ODc5NywiRmFjZSI6eyJCb3VuZGluZ0JveCI6eyJIZWlnaHQiOjAuNDc4MzM3LCJXaWR0aCI6MC4yOTgxNzMsIkxlZnQiOjAuNDIxOTI3LCJUb3AiOjAuNDc1ODYzfSwiRmFjZUlkIjoiNjkzYmU1MDEtOTQzOC00ZmZjLThhNDItZjMyN2IwMjliYjljIiwiQ29uZmlkZW5jZSI6OTkuOTk5OSwiSW1hZ2VJZCI6IjYzMGI0NWQxLTNmYjEtMzAwNy1hM2Q3LWZhZmFmNjBkOWMzOCJ9fSx7IlNpbWlsYXJpdHkiOjk5Ljk4NzAyLCJGYWNlIjp7IkJvdW5kaW5nQm94Ijp7IkhlaWdodCI6MC40ODI0MDYsIldpZHRoIjowLjI5NjA4NCwiTGVmdCI6MC40MjUzMywiVG9wIjowLjQ3MzI3NH0sIkZhY2VJZCI6IjU0N2M3NDJmLWUzY2MtNDNkZS05Yjk2LWVlOWQ3N2YzOGUxZiIsIkNvbmZpZGVuY2UiOjk5Ljk5OTksIkltYWdlSWQiOiJjYzUyNjBjYy04NjNjLTM0NWItOWE2NS0yMzU5YmFjZjYwZGQifX0seyJTaW1pbGFyaXR5Ijo5OS45ODMzLCJGYWNlIjp7IkJvdW5kaW5nQm94Ijp7IkhlaWdodCI6MC40MzcwMjQsIldpZHRoIjowLjI0Njk4NSwiTGVmdCI6MC4zNjM1OTksIlRvcCI6MC41MTQ3NzV9LCJGYWNlSWQiOiJlZTk4ZjYyZC04ZWJjLTRiZTYtODYwYS0zODI1NjFmZDIwOWUiLCJDb25maWRlbmNlIjoxMDAuMCwiSW1hZ2VJZCI6Ijc5MDBlMGY4LWZjYTMtMzIyOS04NTgxLWIwOWRmY2E0NjRjYyJ9fSx7IlNpbWlsYXJpdHkiOjk5Ljk1NzQ3LCJGYWNlIjp7IkJvdW5kaW5nQm94Ijp7IkhlaWdodCI6MC4zOTc0OTIsIldpZHRoIjowLjIzNTQ5MiwiTGVmdCI6MC4zNzAzNzQsIlRvcCI6MC42MTg2MjF9LCJGYWNlSWQiOiIzMjA0OTg3NC1iNzQzLTRiMDAtOGExOS03OTNlOTRhZjQxOWIiLCJDb25maWRlbmNlIjoxMDAuMCwiSW1hZ2VJZCI6IjY5YmZiNzdlLWU1MzctMzI0MS1hMjdlLWZhMTcxOTdiZDQxNSJ9fSx7IlNpbWlsYXJpdHkiOjk5Ljk1NDU3NSwiRmFjZSI6eyJCb3VuZGluZ0JveCI6eyJIZWlnaHQiOjAuNjg1MDg2LCJXaWR0aCI6MC41MTY5NDIsIkxlZnQiOjAuMTYwMDMxLCJUb3AiOjAuMjc0NTM1fSwiRmFjZUlkIjoiYmUwMzIzOTMtOTYyYi00OGJkLThjMWUtYzJlOWQ3Zjk5OWYwIiwiQ29uZmlkZW5jZSI6OTkuOTk5NiwiSW1hZ2VJZCI6Ijc0ODIwNDg1LTNhZTctM2RhNy1hMmNiLWIzNWIyZDVhZTRmMCJ9fSx7IlNpbWlsYXJpdHkiOjk5Ljk1NDAyNSwiRmFjZSI6eyJCb3VuZGluZ0JveCI6eyJIZWlnaHQiOjAuMzc4MTg2LCJXaWR0aCI6MC4yMDY1MjEsIkxlZnQiOjAuMzA4NTQ0LCJUb3AiOjAuNTA4OTUzfSwiRmFjZUlkIjoiMWM5ZjFjYTgtNzZhYS00MjYxLTk0NTEtZTNkOWU2OTUyMzNlIiwiQ29uZmlkZW5jZSI6MTAwLjAsIkltYWdlSWQiOiJlNDEyN2YwNC1jNzVhLTMzMGItOTljNy1mYTkzYTE0ZjQyNmQifX0seyJTaW1pbGFyaXR5Ijo5OS45NTA5NCwiRmFjZSI6eyJCb3VuZGluZ0JveCI6eyJIZWlnaHQiOjAuNDY5NjMyLCJXaWR0aCI6MC4yNTU3NDcsIkxlZnQiOjAuMzY3NzY5LCJUb3AiOjAuNDMxOTV9LCJGYWNlSWQiOiI3ZmJiNjc5NS0zMjc1LTQwMWMtOTZlMi0wMzE0M2ZiOTgwMTUiLCJDb25maWRlbmNlIjoxMDAuMCwiSW1hZ2VJZCI6ImI4YjZkZDM3LTM3NTEtM2YyMi1hMDJiLWIzOTNlY2M5YjM3YyJ9fSx7IlNpbWlsYXJpdHkiOjk5Ljk0MTEyNCwiRmFjZSI6eyJCb3VuZGluZ0JveCI6eyJIZWlnaHQiOjAuNTc3OTkxLCJXaWR0aCI6MC41OTY2NDQsIkxlZnQiOjAuMzQwNDA2LCJUb3AiOjAuNDEwNTUyfSwiRmFjZUlkIjoiMTE1ZjhmZjktYmFmZC00YjQ0LThiMjgtMzM3ZTE0NWEzMWMxIiwiQ29uZmlkZW5jZSI6OTkuOTk5NywiSW1hZ2VJZCI6ImQwYzFlYjBjLTliMzQtMzViMi1iOWYwLWY2YjJmNTYwMzY4YyJ9fSx7IlNpbWlsYXJpdHkiOjk5LjkzMDUsIkZhY2UiOnsiQm91bmRpbmdCb3giOnsiSGVpZ2h0IjowLjM0NjYyNywiV2lkdGgiOjAuMTg5MzczLCJMZWZ0IjowLjM1Njc2MiwiVG9wIjowLjUyMTM3NH0sIkZhY2VJZCI6Ijc1NTUwOGZmLWNhMmQtNDM3YS04ZWRjLTE2MDk5NzI5NTM3OSIsIkNvbmZpZGVuY2UiOjEwMC4wLCJJbWFnZUlkIjoiZDE3ZjgzYjgtMTZkYy0zNDBmLTllMzYtZDAzN2ZmOTVjMTMxIn19LHsiU2ltaWxhcml0eSI6OTkuOTI0Njc1LCJGYWNlIjp7IkJvdW5kaW5nQm94Ijp7IkhlaWdodCI6MC40NzI3MjUsIldpZHRoIjowLjI1OTE1NSwiTGVmdCI6MC4zMjU0NzMsIlRvcCI6MC41MTc4NTl9LCJGYWNlSWQiOiIzYzBiNTI2ZS1hZWMzLTQxMWItYmNiNC0xNTNkYTMxMzVlZjAiLCJDb25maWRlbmNlIjo5OS45OTk5LCJJbWFnZUlkIjoiODM2N2MwYmUtMTM4OS0zOGEzLWEwNDAtNjFkYWUzNmY4ZGM1In19LHsiU2ltaWxhcml0eSI6OTkuOTIzMDMsIkZhY2UiOnsiQm91bmRpbmdCb3giOnsiSGVpZ2h0IjowLjY0MDI5MiwiV2lkdGgiOjAuNTA5NjY4LCJMZWZ0IjowLjIyNTc3NCwiVG9wIjowLjMyMDc0NH0sIkZhY2VJZCI6ImJmZTFmZWFkLTc1NTItNGZlMC05NTExLWM1MGZlYjViMTdmMSIsIkNvbmZpZGVuY2UiOjk5Ljk5OTMwNiwiSW1hZ2VJZCI6Ijk5MDZkNjI3LTUzYmYtMzQxMS04ZmVkLWE1Zjc0ZmQ0NGQ2NSJ9fSx7IlNpbWlsYXJpdHkiOjk5LjkyMTM2NCwiRmFjZSI6eyJCb3VuZGluZ0JveCI6eyJIZWlnaHQiOjAuMzg5MTEzLCJXaWR0aCI6MC4yMzY2OTQsIkxlZnQiOjAuNDA5ODYsIlRvcCI6MC42NzAyNX0sIkZhY2VJZCI6IjlmM2I1OTk2LTgwZDYtNDkzYS1hYjU0LTUwNDM4YjlmNTYzZiIsIkNvbmZpZGVuY2UiOjEwMC4wLCJJbWFnZUlkIjoiNjczYjhhOTMtODUyZS0zZTE1LTk5NTUtNDA2YWJmNmZjMDBmIn19LHsiU2ltaWxhcml0eSI6OTkuOTAwMTgsIkZhY2UiOnsiQm91bmRpbmdCb3giOnsiSGVpZ2h0IjowLjM3NTk4OCwiV2lkdGgiOjAuMjMzMzg3LCJMZWZ0IjowLjM3NDA1MywiVG9wIjowLjY4MTI5N30sIkZhY2VJZCI6ImNjN2FlZTFkLTRmZTQtNDViNS05ZTc3LWY3ZjRlMTY3NWE5NyIsIkNvbmZpZGVuY2UiOjEwMC4wLCJJbWFnZUlkIjoiZmM2MGI2ZjQtMjA2YS0zYzIwLTlmMDUtMDc1Mjc2NzA2OGUwIn19LHsiU2ltaWxhcml0eSI6OTkuODkyNTcsIkZhY2UiOnsiQm91bmRpbmdCb3giOnsiSGVpZ2h0IjowLjQ3MTc3OCwiV2lkdGgiOjAuMzU1NTU4LCJMZWZ0Ijo1LjU2MjY2RS01LCJUb3AiOjAuNTY0OTA4fSwiRmFjZUlkIjoiYmFhYmRmMDgtNWI2NC00NjhlLWFiMmQtNTQwYzc1NzM4YTQ2IiwiQ29uZmlkZW5jZSI6MTAwLjAsIkltYWdlSWQiOiIwY2RmNDgzZi02ZDQ4LTM3ZDAtYTNkNS1iNzJhODViNjZmMTIifX0seyJTaW1pbGFyaXR5Ijo5OS44ODU4NywiRmFjZSI6eyJCb3VuZGluZ0JveCI6eyJIZWlnaHQiOjAuOTc0MzIzLCJXaWR0aCI6MC43MDc3MDMsIkxlZnQiOjAuMDUyMDQwMSwiVG9wIjotMC4wMDQyOTY5NX0sIkZhY2VJZCI6IjYyOGYyYjA3LThhY2ItNGI0OS1hZjc0LWVjYTY3NDE2YjdhMyIsIkNvbmZpZGVuY2UiOjk5Ljk5OTUsIkltYWdlSWQiOiIxMDZlODg4YS05YjFiLTNjM2EtODFmOC1hNmM4ODI5N2NmNDEifX0seyJTaW1pbGFyaXR5Ijo5OS44ODQ0NywiRmFjZSI6eyJCb3VuZGluZ0JveCI6eyJIZWlnaHQiOjAuOTY2Mjk4LCJXaWR0aCI6MC43MDc1MjQsIkxlZnQiOjAuMDU0Mzc4MiwiVG9wIjotMC4wMDEyODQ0fSwiRmFjZUlkIjoiMzY1MjFlN2MtOWJjMS00MzIxLWExZjktM2Y1YmYyYjdjNWUxIiwiQ29uZmlkZW5jZSI6OTkuOTk5NSwiSW1hZ2VJZCI6ImNhYzJlZDhlLWNhYTItMzAxNy1iNTI1LTBmMGRkMThhMDE5ZCJ9fSx7IlNpbWlsYXJpdHkiOjk5Ljg4MzE1LCJGYWNlIjp7IkJvdW5kaW5nQm94Ijp7IkhlaWdodCI6MC45NTcyMDksIldpZHRoIjowLjcyMzkxNSwiTGVmdCI6MC4wNTI4NjgsIlRvcCI6MC4wMDI2ODg2M30sIkZhY2VJZCI6IjFjYTBhOWU3LWYzNDEtNDQ5Yy04OGQ4LWQ2OWVkYjU5Njk1NiIsIkNvbmZpZGVuY2UiOjk5Ljk5OTQsIkltYWdlSWQiOiI0NDQzMWJmYi1jYTc4LTMxZTEtOTQ1NS02N2Q1OWQzZGI0MDgifX1dfV19", "approximateArrivalTimestamp": 1586930703.157}, "eventSource": "aws:kinesis", "eventVersion": "1.0", "eventID": "shardId-000000000000:49605974038509424079760656864102544007978456824330321922", "eventName": "aws:kinesis:record", "invokeIdentityArn": "arn:aws:iam::090918556265:role/LambdaKinesis", "awsRegion": "us-east-1", "eventSourceARN": "arn:aws:kinesis:us-east-1:090918556265:stream/face-stream/consumer/face_consumer:1586654788"}, {"kinesis": {"kinesisSchemaVersion": "1.0", "partitionKey": "e4a38a83-fe6c-4e5e-9bd1-0c8e7a0f91d0", "sequenceNumber": "49605974038509424079760656864402357611242884997096407042", "data": "eyJJbnB1dEluZm9ybWF0aW9uIjp7IktpbmVzaXNWaWRlbyI6eyJTdHJlYW1Bcm4iOiJhcm46YXdzOmtpbmVzaXN2aWRlbzp1cy1lYXN0LTE6MDkwOTE4NTU2MjY1OnN0cmVhbS9tYWNib29rLWNhbWVyYS8xNTg2NTU1MDU0OTg5IiwiRnJhZ21lbnROdW1iZXIiOiI5MTM0Mzg1MjMzMzE4MTY3MDA4MjEyMTM2NTU3ODA1MTk3Njg5ODQ0MDI4NTU4MCIsIlNlcnZlclRpbWVzdGFtcCI6MS41ODY5MzA2OTcxODNFOSwiUHJvZHVjZXJUaW1lc3RhbXAiOjEuNTg2OTMwNjk2MzM2RTksIkZyYW1lT2Zmc2V0SW5TZWNvbmRzIjo0LjAwMDk5OTkyNzUyMDc1Mn19LCJTdHJlYW1Qcm9jZXNzb3JJbmZvcm1hdGlvbiI6eyJTdGF0dXMiOiJSVU5OSU5HIn0sIkZhY2VTZWFyY2hSZXNwb25zZSI6W3siRGV0ZWN0ZWRGYWNlIjp7IkJvdW5kaW5nQm94Ijp7IkhlaWdodCI6MC41MDk0MTgzLCJXaWR0aCI6MC4yOTUzMDk3LCJMZWZ0IjowLjM3NDY3ODEzLCJUb3AiOjAuNDYwNDQ2NjN9LCJDb25maWRlbmNlIjo5OS45OTk5NiwiTGFuZG1hcmtzIjpbeyJYIjowLjQzNzk4MzY2LCJZIjowLjYyMTU4ODA1LCJUeXBlIjoiZXllTGVmdCJ9LHsiWCI6MC41NzQ5NjMxLCJZIjowLjYxNTE1MzIsIlR5cGUiOiJleWVSaWdodCJ9LHsiWCI6MC40NTgzMjUwNiwiWSI6MC44MzIzNzYsIlR5cGUiOiJtb3V0aExlZnQifSx7IlgiOjAuNTcxNDg1MywiWSI6MC44MjY4MzIyLCJUeXBlIjoibW91dGhSaWdodCJ9LHsiWCI6MC41MTgzNTM3LCJZIjowLjczMzQ2NSwiVHlwZSI6Im5vc2UifV0sIlBvc2UiOnsiUGl0Y2giOi0wLjUxNjM0NjIsIlJvbGwiOi0yLjQ5Mzc0LCJZYXciOjEuNzU3MTA3M30sIlF1YWxpdHkiOnsiQnJpZ2h0bmVzcyI6NzcuOTc4NTE2LCJTaGFycG5lc3MiOjc4LjY0MzV9fSwiTWF0Y2hlZEZhY2VzIjpbeyJTaW1pbGFyaXR5Ijo5OS45OTMxMSwiRmFjZSI6eyJCb3VuZGluZ0JveCI6eyJIZWlnaHQiOjAuNTEyMDc4LCJXaWR0aCI6MC4yNzc0NiwiTGVmdCI6MC4zNzAyLCJUb3AiOjAuNDMzODcyfSwiRmFjZUlkIjoiODllY2RkYmEtZmJlNy00N2NjLWEyMWEtM2U3NWYwNmY0YTI4IiwiQ29uZmlkZW5jZSI6OTkuOTk5OSwiSW1hZ2VJZCI6IjMzYzI3MzA4LWY1NzctM2JmYS1hODkzLWEyOWUxNGUyMmI1NSJ9fSx7IlNpbWlsYXJpdHkiOjk5Ljk5MTI3LCJGYWNlIjp7IkJvdW5kaW5nQm94Ijp7IkhlaWdodCI6MC40NjMzNTIsIldpZHRoIjowLjI1MjkxMywiTGVmdCI6MC4zOTg5MjgsIlRvcCI6MC41MTQxNzJ9LCJGYWNlSWQiOiIzYWY0YzFmMC0xYzU3LTQ1NmYtOTI2Ni1jMzA4YjkzNmMxZWYiLCJDb25maWRlbmNlIjoxMDAuMCwiSW1hZ2VJZCI6ImE4NzkyMGEwLTA1ZGEtMzg5YS04ZjgwLWY3NzkyZGMwMWQwNyJ9fSx7IlNpbWlsYXJpdHkiOjk5Ljk5MDc1LCJGYWNlIjp7IkJvdW5kaW5nQm94Ijp7IkhlaWdodCI6MC40NzgzMzcsIldpZHRoIjowLjI5ODE3MywiTGVmdCI6MC40MjE5MjcsIlRvcCI6MC40NzU4NjN9LCJGYWNlSWQiOiI2OTNiZTUwMS05NDM4LTRmZmMtOGE0Mi1mMzI3YjAyOWJiOWMiLCJDb25maWRlbmNlIjo5OS45OTk5LCJJbWFnZUlkIjoiNjMwYjQ1ZDEtM2ZiMS0zMDA3LWEzZDctZmFmYWY2MGQ5YzM4In19LHsiU2ltaWxhcml0eSI6OTkuOTg5NTI1LCJGYWNlIjp7IkJvdW5kaW5nQm94Ijp7IkhlaWdodCI6MC41MTQzNjMsIldpZHRoIjowLjI4MjczOSwiTGVmdCI6MC40NDQyMDMsIlRvcCI6MC40Nzg0NDF9LCJGYWNlSWQiOiIyNDMwMWUyZi1lMmU4LTQ3ZWYtYTYyOS00NTY5YjZmZTMzYzAiLCJDb25maWRlbmNlIjoxMDAuMCwiSW1hZ2VJZCI6IjNhMGJiNWRkLTU5MWUtM2ZjMS1hMzcxLWM1OWMxMjZmYjI4MiJ9fSx7IlNpbWlsYXJpdHkiOjk5Ljk4ODQ0LCJGYWNlIjp7IkJvdW5kaW5nQm94Ijp7IkhlaWdodCI6MC40ODI0MDYsIldpZHRoIjowLjI5NjA4NCwiTGVmdCI6MC40MjUzMywiVG9wIjowLjQ3MzI3NH0sIkZhY2VJZCI6IjU0N2M3NDJmLWUzY2MtNDNkZS05Yjk2LWVlOWQ3N2YzOGUxZiIsIkNvbmZpZGVuY2UiOjk5Ljk5OTksIkltYWdlSWQiOiJjYzUyNjBjYy04NjNjLTM0NWItOWE2NS0yMzU5YmFjZjYwZGQifX0seyJTaW1pbGFyaXR5Ijo5OS45ODQ2NSwiRmFjZSI6eyJCb3VuZGluZ0JveCI6eyJIZWlnaHQiOjAuNDM3MDI0LCJXaWR0aCI6MC4yNDY5ODUsIkxlZnQiOjAuMzYzNTk5LCJUb3AiOjAuNTE0Nzc1fSwiRmFjZUlkIjoiZWU5OGY2MmQtOGViYy00YmU2LTg2MGEtMzgyNTYxZmQyMDllIiwiQ29uZmlkZW5jZSI6MTAwLjAsIkltYWdlSWQiOiI3OTAwZTBmOC1mY2EzLTMyMjktODU4MS1iMDlkZmNhNDY0Y2MifX0seyJTaW1pbGFyaXR5Ijo5OS45NjI2NCwiRmFjZSI6eyJCb3VuZGluZ0JveCI6eyJIZWlnaHQiOjAuMzk3NDkyLCJXaWR0aCI6MC4yMzU0OTIsIkxlZnQiOjAuMzcwMzc0LCJUb3AiOjAuNjE4NjIxfSwiRmFjZUlkIjoiMzIwNDk4NzQtYjc0My00YjAwLThhMTktNzkzZTk0YWY0MTliIiwiQ29uZmlkZW5jZSI6MTAwLjAsIkltYWdlSWQiOiI2OWJmYjc3ZS1lNTM3LTMyNDEtYTI3ZS1mYTE3MTk3YmQ0MTUifX0seyJTaW1pbGFyaXR5Ijo5OS45NTgwMSwiRmFjZSI6eyJCb3VuZGluZ0JveCI6eyJIZWlnaHQiOjAuNjg1MDg2LCJXaWR0aCI6MC41MTY5NDIsIkxlZnQiOjAuMTYwMDMxLCJUb3AiOjAuMjc0NTM1fSwiRmFjZUlkIjoiYmUwMzIzOTMtOTYyYi00OGJkLThjMWUtYzJlOWQ3Zjk5OWYwIiwiQ29uZmlkZW5jZSI6OTkuOTk5NiwiSW1hZ2VJZCI6Ijc0ODIwNDg1LTNhZTctM2RhNy1hMmNiLWIzNWIyZDVhZTRmMCJ9fSx7IlNpbWlsYXJpdHkiOjk5Ljk1NDc0LCJGYWNlIjp7IkJvdW5kaW5nQm94Ijp7IkhlaWdodCI6MC4zNzgxODYsIldpZHRoIjowLjIwNjUyMSwiTGVmdCI6MC4zMDg1NDQsIlRvcCI6MC41MDg5NTN9LCJGYWNlSWQiOiIxYzlmMWNhOC03NmFhLTQyNjEtOTQ1MS1lM2Q5ZTY5NTIzM2UiLCJDb25maWRlbmNlIjoxMDAuMCwiSW1hZ2VJZCI6ImU0MTI3ZjA0LWM3NWEtMzMwYi05OWM3LWZhOTNhMTRmNDI2ZCJ9fSx7IlNpbWlsYXJpdHkiOjk5Ljk1MjQ0LCJGYWNlIjp7IkJvdW5kaW5nQm94Ijp7IkhlaWdodCI6MC40Njk2MzIsIldpZHRoIjowLjI1NTc0NywiTGVmdCI6MC4zNjc3NjksIlRvcCI6MC40MzE5NX0sIkZhY2VJZCI6IjdmYmI2Nzk1LTMyNzUtNDAxYy05NmUyLTAzMTQzZmI5ODAxNSIsIkNvbmZpZGVuY2UiOjEwMC4wLCJJbWFnZUlkIjoiYjhiNmRkMzctMzc1MS0zZjIyLWEwMmItYjM5M2VjYzliMzdjIn19LHsiU2ltaWxhcml0eSI6OTkuOTQ0MzMsIkZhY2UiOnsiQm91bmRpbmdCb3giOnsiSGVpZ2h0IjowLjU3Nzk5MSwiV2lkdGgiOjAuNTk2NjQ0LCJMZWZ0IjowLjM0MDQwNiwiVG9wIjowLjQxMDU1Mn0sIkZhY2VJZCI6IjExNWY4ZmY5LWJhZmQtNGI0NC04YjI4LTMzN2UxNDVhMzFjMSIsIkNvbmZpZGVuY2UiOjk5Ljk5OTcsIkltYWdlSWQiOiJkMGMxZWIwYy05YjM0LTM1YjItYjlmMC1mNmIyZjU2MDM2OGMifX0seyJTaW1pbGFyaXR5Ijo5OS45MzY2NDYsIkZhY2UiOnsiQm91bmRpbmdCb3giOnsiSGVpZ2h0IjowLjM4OTExMywiV2lkdGgiOjAuMjM2Njk0LCJMZWZ0IjowLjQwOTg2LCJUb3AiOjAuNjcwMjV9LCJGYWNlSWQiOiI5ZjNiNTk5Ni04MGQ2LTQ5M2EtYWI1NC01MDQzOGI5ZjU2M2YiLCJDb25maWRlbmNlIjoxMDAuMCwiSW1hZ2VJZCI6IjY3M2I4YTkzLTg1MmUtM2UxNS05OTU1LTQwNmFiZjZmYzAwZiJ9fSx7IlNpbWlsYXJpdHkiOjk5LjkzNjYsIkZhY2UiOnsiQm91bmRpbmdCb3giOnsiSGVpZ2h0IjowLjM0NjYyNywiV2lkdGgiOjAuMTg5MzczLCJMZWZ0IjowLjM1Njc2MiwiVG9wIjowLjUyMTM3NH0sIkZhY2VJZCI6Ijc1NTUwOGZmLWNhMmQtNDM3YS04ZWRjLTE2MDk5NzI5NTM3OSIsIkNvbmZpZGVuY2UiOjEwMC4wLCJJbWFnZUlkIjoiZDE3ZjgzYjgtMTZkYy0zNDBmLTllMzYtZDAzN2ZmOTVjMTMxIn19LHsiU2ltaWxhcml0eSI6OTkuOTIxNzE1LCJGYWNlIjp7IkJvdW5kaW5nQm94Ijp7IkhlaWdodCI6MC40NzI3MjUsIldpZHRoIjowLjI1OTE1NSwiTGVmdCI6MC4zMjU0NzMsIlRvcCI6MC41MTc4NTl9LCJGYWNlSWQiOiIzYzBiNTI2ZS1hZWMzLTQxMWItYmNiNC0xNTNkYTMxMzVlZjAiLCJDb25maWRlbmNlIjo5OS45OTk5LCJJbWFnZUlkIjoiODM2N2MwYmUtMTM4OS0zOGEzLWEwNDAtNjFkYWUzNmY4ZGM1In19LHsiU2ltaWxhcml0eSI6OTkuOTE3ODcsIkZhY2UiOnsiQm91bmRpbmdCb3giOnsiSGVpZ2h0IjowLjY0MDI5MiwiV2lkdGgiOjAuNTA5NjY4LCJMZWZ0IjowLjIyNTc3NCwiVG9wIjowLjMyMDc0NH0sIkZhY2VJZCI6ImJmZTFmZWFkLTc1NTItNGZlMC05NTExLWM1MGZlYjViMTdmMSIsIkNvbmZpZGVuY2UiOjk5Ljk5OTMwNiwiSW1hZ2VJZCI6Ijk5MDZkNjI3LTUzYmYtMzQxMS04ZmVkLWE1Zjc0ZmQ0NGQ2NSJ9fSx7IlNpbWlsYXJpdHkiOjk5LjkxMzI3LCJGYWNlIjp7IkJvdW5kaW5nQm94Ijp7IkhlaWdodCI6MC4zNzU5ODgsIldpZHRoIjowLjIzMzM4NywiTGVmdCI6MC4zNzQwNTMsIlRvcCI6MC42ODEyOTd9LCJGYWNlSWQiOiJjYzdhZWUxZC00ZmU0LTQ1YjUtOWU3Ny1mN2Y0ZTE2NzVhOTciLCJDb25maWRlbmNlIjoxMDAuMCwiSW1hZ2VJZCI6ImZjNjBiNmY0LTIwNmEtM2MyMC05ZjA1LTA3NTI3NjcwNjhlMCJ9fSx7IlNpbWlsYXJpdHkiOjk5LjkxMjIsIkZhY2UiOnsiQm91bmRpbmdCb3giOnsiSGVpZ2h0IjowLjQ3MTc3OCwiV2lkdGgiOjAuMzU1NTU4LCJMZWZ0Ijo1LjU2MjY2RS01LCJUb3AiOjAuNTY0OTA4fSwiRmFjZUlkIjoiYmFhYmRmMDgtNWI2NC00NjhlLWFiMmQtNTQwYzc1NzM4YTQ2IiwiQ29uZmlkZW5jZSI6MTAwLjAsIkltYWdlSWQiOiIwY2RmNDgzZi02ZDQ4LTM3ZDAtYTNkNS1iNzJhODViNjZmMTIifX0seyJTaW1pbGFyaXR5Ijo5OS45MDE4MSwiRmFjZSI6eyJCb3VuZGluZ0JveCI6eyJIZWlnaHQiOjAuNjg3ODg3LCJXaWR0aCI6MC41NDA1NzQsIkxlZnQiOi0wLjAwOTI4Mjk0LCJUb3AiOjAuMzkwNjQ0fSwiRmFjZUlkIjoiNjkxMzM0NjgtN2Q2ZS00ZjY5LTljYzQtOWVkNjNmZTc1YmVkIiwiQ29uZmlkZW5jZSI6MTAwLjAsIkltYWdlSWQiOiJiZjY0MzRjOS05MjI1LTNiMDYtYWVlMi02MzZhMjExMTU5NmEifX0seyJTaW1pbGFyaXR5Ijo5OS45MDA1NCwiRmFjZSI6eyJCb3VuZGluZ0JveCI6eyJIZWlnaHQiOjAuNjEyNzA5LCJXaWR0aCI6MC40NjQwMjIsIkxlZnQiOjAuMDAzMTM4MDUsIlRvcCI6MC40MTk0Nzl9LCJGYWNlSWQiOiIxMDRkYjI2ZS04ZjVhLTRmZWEtYWRjOS1hMGE1NzM5YjRmMzAiLCJDb25maWRlbmNlIjoxMDAuMCwiSW1hZ2VJZCI6ImRhODllN2RmLWY3NTUtMzVlNy1hMWUxLTdlNGVkYWI2Y2EyNyJ9fSx7IlNpbWlsYXJpdHkiOjk5Ljg5OTkxLCJGYWNlIjp7IkJvdW5kaW5nQm94Ijp7IkhlaWdodCI6MC42MTE0NDEsIldpZHRoIjowLjUwODI2MiwiTGVmdCI6LTAuMDIyNzA5NCwiVG9wIjowLjM5NTA1MX0sIkZhY2VJZCI6ImUyNzViZmU3LTZjNzctNDNiYy1hZTY1LTFiZWVlNzk3NWFhZCIsIkNvbmZpZGVuY2UiOjk5Ljk5OTUsIkltYWdlSWQiOiI5ZTBjM2RiZC03YTJkLTMyZGMtYTQ3Mi05YTZiZDM2NmFkYTQifX1dfV19", "approximateArrivalTimestamp": 1586930704.229}, "eventSource": "aws:kinesis", "eventVersion": "1.0", "eventID": "shardId-000000000000:49605974038509424079760656864402357611242884997096407042", "eventName": "aws:kinesis:record", "invokeIdentityArn": "arn:aws:iam::090918556265:role/LambdaKinesis", "awsRegion": "us-east-1", "eventSourceARN": "arn:aws:kinesis:us-east-1:090918556265:stream/face-stream/consumer/face_consumer:1586654788"}]}
"""

class KVSTest(unittest.TestCase):

    def test_lambda(self):
        event = json.loads(response_json)
        lambda_handler(event, None)

    def test_print_image(self):
        event = json.loads(response_json)
        handler = KVMediaHandler(env["arn_kvs"])
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
        print(face_recognition_record)
        ProducerTimestamp = face_recognition_record["InputInformation"]["KinesisVideo"]["ProducerTimestamp"]
        offset = face_recognition_record["InputInformation"]["KinesisVideo"]["FrameOffsetInSeconds"]
        print(ProducerTimestamp, offset)
        image = handler.get_image_from_stream(ProducerTimestamp + offset)
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

