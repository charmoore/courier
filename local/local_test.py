#!/usr/bin/env python3
import boto3
import csv
import datetime
import mysql.connector
import os
import re
import s3fs
import sys
import time
import uuid
from botocore.exceptions import ClientError
import courier
from mysql.connector import errorcode

events = [{'Records': [{'eventVersion': '2.1', 'eventSource': 'aws:s3', 'awsRegion': 'us-east-1', 'eventTime': '2021-09-23T03:26:55.738Z', 'eventName': 'ObjectCreated:Put', 'userIdentity': {'principalId': ''}, 'requestParameters': {'sourceIPAddress': ''}, 'responseElements': {'x-amz-request-id': '', 'x-amz-id-2': ''}, 's3': {'s3SchemaVersion': '1.0', 'configurationId': '', 'bucket': {'name': '.', 'ownerIdentity': {'principalId': ''}, 'arn': ''}, 'object': {'key': 'input/sample_file.csv', 'size': 351, 'eTag': '', 'sequencer': ''}}}]}]

context = ''

for event in events:
	courier.process(event, context)
