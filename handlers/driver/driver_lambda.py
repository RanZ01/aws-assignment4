"""Create three objects; wait for both alarm-driven deletions; then plot."""
import json
import os
import time
from urllib.error import HTTPError
from urllib.parse import urlencode, urlsplit
from urllib.request import urlopen

import boto3
from boto3.dynamodb.conditions import Key

s3 = boto3.client("s3")
cloudwatch = boto3.client("cloudwatch")
table = boto3.resource("dynamodb").Table(os.environ["TABLE_NAME"])


def call_api(url, params):
    target = url + ("&" if "?" in url else "?") + urlencode(params)
    try:
        with urlopen(target, timeout=28) as response:
            return json.loads(response.read().decode())
    except HTTPError as exc:
        raise RuntimeError(f"Plot API HTTP {exc.code}: {exc.read().decode()}") from exc


def recent_rows(bucket, since_us):
    options = {
        "KeyConditionExpression": Key("history_group").eq("ALL") & Key("record_key").between(
            f"{bucket}#{since_us:020d}#", f"{bucket}#~"),
        "ConsistentRead": True,
    }
    while True:
        page = table.query(**options)
        yield from page.get("Items", [])
        if not page.get("LastEvaluatedKey"):
            return
        options["ExclusiveStartKey"] = page["LastEvaluatedKey"]


def wait_for_row(bucket, since_us, predicate, description, timeout=45):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        for row in recent_rows(bucket, since_us):
            if predicate(row):
                return row
        time.sleep(1)
    raise TimeoutError(f"Tracker row missing: {description}; inspect SQS and Lambda logs")


def wait_for_deletion(bucket, key, since_us, expected_size, timeout=300):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        for row in recent_rows(bucket, since_us):
            if (row.get("object_key") == key
                    and row.get("event_name", "").startswith("ObjectRemoved:")
                    and int(row["total_size"]) == expected_size):
                return row
        time.sleep(2)
    raise TimeoutError(f"Cleaner did not delete {key}; inspect alarm history and logs")


def wait_for_alarm_ok(alarm_name, timeout=300):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        alarms = cloudwatch.describe_alarms(AlarmNames=[alarm_name])["MetricAlarms"]
        if alarms and alarms[0]["StateValue"] == "OK":
            return
        time.sleep(5)
    raise TimeoutError("Alarm did not return to OK after the first cleanup")


def lambda_handler(event, context):
    bucket = os.environ["BUCKET_NAME"]
    api_url = os.environ["PLOT_API_URL"]
    if urlsplit(api_url).scheme != "https":
        raise ValueError("PLOT_API_URL must use HTTPS")
    if s3.list_objects_v2(Bucket=bucket, MaxKeys=1).get("KeyCount", 0):
        raise RuntimeError("Bucket must be empty before this demo; destroy and redeploy the stacks")
    call_api(api_url, {"health": "1"})

    started_us = time.time_ns() // 1000
    rows, request_ids = [], []

    def put_and_wait(key, body, expected_size, expected_count):
        response = s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType="text/plain")
        request_id = response["ResponseMetadata"]["RequestId"]
        row = wait_for_row(
            bucket, started_us,
            lambda x: x.get("s3_request_id") == request_id
            and int(x["total_size"]) == expected_size
            and int(x["object_count"]) == expected_count,
            f"{key} put request {request_id}",
        )
        rows.append(row)
        request_ids.append(request_id)

    first = b"Empty Assignment 1"  # 18 bytes
    second = b"Empty Assignment 2222222222"  # 27 bytes as written in the prompt
    put_and_wait("assignment1.txt", first, len(first), 1)
    time.sleep(2)
    put_and_wait("assignment2.txt", second, len(first) + len(second), 2)
    rows.append(wait_for_deletion(bucket, "assignment2.txt", started_us, len(first)))
    wait_for_alarm_ok(os.environ["ALARM_NAME"])

    third = b"33"
    put_and_wait("assignment3.txt", third, len(first) + len(third), 2)
    rows.append(wait_for_deletion(bucket, "assignment1.txt", started_us, len(third)))

    plot = call_api(api_url, {"request_ids": ",".join(request_ids)})
    return {"status": "ok", "measured_sizes": [int(row["total_size"]) for row in rows],
            "plot": plot}
