import os
import pytest

from courier.config import settings
from courier.main import main


context = {}
event = {
    "Records": [
        {
            "eventVersion": "2.1",
            "eventSource": "aws:s3",
            "awsRegion": "us-east-1",
            "eventTime": "2021-09-23T03:26:55.738Z",
            "eventName": "ObjectCreated:Put",
            "userIdentity": {"principalId": ""},
            "requestParameters": {"sourceIPAddress": ""},
            "responseElements": {"x-amz-request-id": "", "x-amz-id-2": ""},
            "s3": {
                "s3SchemaVersion": "1.0",
                "configurationId": "",
                "bucket": {
                    "name": "demo",
                    "ownerIdentity": {"principalId": ""},
                    "arn": "",
                },
                "object": {
                    "key": "input/demo-20210915.csv",
                    "size": 351,
                    "eTag": "",
                    "sequencer": "",
                },
            },
        }
    ]
}


def lambda_handler(event, context):
    print("running")
    main(event, context)


if __name__ == "__main__":
    lambda_handler(event, context)
