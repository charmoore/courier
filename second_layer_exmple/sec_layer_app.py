"""
example lambda handler using courier library
"""

from courier import process


def handler(event, context):
    print("using courier library to process:")
    print(f"event: {event}, context: {context}")
    process(event, context)