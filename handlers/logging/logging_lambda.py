"""Log signed S3 object size changes from the logging SQS queue."""
import json
import os
from urllib.parse import unquote_plus

import boto3

logs = boto3.client("logs")


def _previous_size(object_name, event_time):
    """Find the newest creation log for this key before the deletion."""
    options = {
        "logGroupName": os.environ["LOG_GROUP_NAME"],
        "filterPattern": '{ $.object_name = ' + json.dumps(object_name) + ' }',
    }
    latest = None
    while True:
        response = logs.filter_log_events(**options)
        for event in response.get("events", []):
            try:
                row = json.loads(event["message"])
            except (KeyError, ValueError):
                continue
            if (row.get("object_name") == object_name
                    and row.get("event_type", "").startswith("ObjectCreated:")
                    and row.get("event_time", "") <= event_time):
                if latest is None or row["event_time"] > latest["event_time"]:
                    latest = row
        token = response.get("nextToken")
        if not token or token == options.get("nextToken"):
            break
        options["nextToken"] = token
    if latest is not None:
        return int(latest["size_delta"])
    raise RuntimeError(f"Creation log for {object_name!r} is not visible yet; retry SQS message")


def lambda_handler(event, context):
    logged = 0
    for message in event.get("Records", []):
        if message.get("eventSource") != "aws:sqs":
            continue
        body = json.loads(message["body"])
        if body.get("Type") == "Notification":
            body = json.loads(body["Message"])
        for record in body.get("Records", []):
            if record.get("eventSource") != "aws:s3":
                continue
            event_type = record.get("eventName", "")
            if not event_type.startswith(("ObjectCreated:", "ObjectRemoved:")):
                continue
            object_name = unquote_plus(record["s3"]["object"]["key"])
            if event_type.startswith("ObjectCreated:"):
                delta = int(record["s3"]["object"]["size"])
            else:
                delta = -_previous_size(object_name, record["eventTime"])
            row = {
                "object_name": object_name, "size_delta": delta,
                "event_type": event_type, "event_time": record["eventTime"],
                "bucket_name": record["s3"]["bucket"]["name"],
                "deployment_id": os.environ["LOG_GROUP_NAME"],
            }
            print(json.dumps(row, separators=(",", ":")))
            logged += 1
    return {"logged": logged}
