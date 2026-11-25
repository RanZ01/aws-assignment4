"""Delete one largest object when the CloudWatch alarm enters ALARM."""
import json
import os

import boto3

s3 = boto3.client("s3")


def lambda_handler(event, context):
    bucket = os.environ["BUCKET_NAME"]
    largest = None
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket):
        for obj in page.get("Contents", []):
            if largest is None or (obj["Size"], obj["Key"]) > (largest["Size"], largest["Key"]):
                largest = obj
    if largest is None:
        print(json.dumps({"status": "empty", "bucket": bucket}))
        return {"status": "empty"}
    s3.delete_object(Bucket=bucket, Key=largest["Key"])
    result = {"status": "deleted", "object_name": largest["Key"],
              "size": largest["Size"]}
    print(json.dumps(result))
    return result
