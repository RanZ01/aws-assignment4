"""Exercise the SQS message contract and largest-object cleanup without AWS."""
import contextlib
import importlib.util
import io
import json
import os
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]


def load_handler(name, path, client, resource=None, environment=None):
    spec = importlib.util.spec_from_file_location(name, ROOT / path)
    module = importlib.util.module_from_spec(spec)
    fake_boto3 = SimpleNamespace(client=lambda service: client,
                                 resource=lambda service: resource)
    with patch.dict(os.environ, environment or {}), patch.dict(sys.modules, {"boto3": fake_boto3}):
        spec.loader.exec_module(module)
    return module


def sqs_event(event_name, key, size=None, event_time="2026-01-01T00:00:00.000Z"):
    obj = {"key": key}
    if size is not None:
        obj["size"] = size
    s3_record = {
        "eventSource": "aws:s3", "eventName": event_name, "eventTime": event_time,
        "s3": {"bucket": {"name": "test-bucket"}, "object": obj},
        "responseElements": {"x-amz-request-id": "test-request"},
    }
    return {"Records": [{"eventSource": "aws:sqs", "body": json.dumps({"Records": [s3_record]})}]}


class FakeLogs:
    def __init__(self, creation):
        self.creation = creation

    def filter_log_events(self, **kwargs):
        if "startFromHead" in kwargs:
            raise AssertionError("FilterLogEvents does not accept startFromHead")
        return {"events": [{"message": json.dumps(self.creation)}]}


class FakeS3:
    def __init__(self):
        self.deleted = []

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        return self

    def paginate(self, **kwargs):
        return iter([{"Contents": [{"Key": "small.txt", "Size": 2}]},
                     {"Contents": [{"Key": "large.txt", "Size": 27}]}])

    def delete_object(self, **kwargs):
        self.deleted.append(kwargs)


class FakeTable:
    def __init__(self):
        self.items = []

    def put_item(self, **kwargs):
        self.items.append(kwargs["Item"])


class FakeResource:
    def __init__(self, table):
        self.table = table

    def Table(self, name):
        return self.table


class EventHandlerTest(unittest.TestCase):
    def test_tracking_lambda_reads_sqs_body_and_writes_measurement(self):
        class TrackerS3:
            def get_paginator(self, name):
                return self

            def paginate(self, **kwargs):
                return iter([{"Contents": [{"Key": "assignment1.txt", "Size": 18}]}])

        table = FakeTable()
        tracker = load_handler(
            "tracking_test", "handlers/size_tracking/size_tracking_lambda.py",
            TrackerS3(), FakeResource(table), {"TABLE_NAME": "history"},
        )
        result = tracker.lambda_handler(sqs_event("ObjectCreated:Put", "assignment1.txt", 18), None)
        self.assertEqual(18, table.items[0]["total_size"])
        self.assertEqual(1, table.items[0]["object_count"])
        self.assertEqual(18, result["written"][0]["total_size"])

    def test_logging_lambda_logs_signed_delta_and_plot_event(self):
        previous = {"object_name": "assignment2.txt", "size_delta": 27,
                    "event_type": "ObjectCreated:Put", "event_time": "2026-01-01T00:00:00.000Z"}
        logger = load_handler(
            "logging_test", "handlers/logging/logging_lambda.py", FakeLogs(previous),
            environment={"LOG_GROUP_NAME": "event-log"},
        )
        output = io.StringIO()
        with patch.dict(os.environ, {"LOG_GROUP_NAME": "event-log"}), contextlib.redirect_stdout(output):
            logger.lambda_handler(sqs_event("ObjectRemoved:Delete", "assignment2.txt",
                                           event_time="2026-01-01T00:00:10.000Z"), None)
            logger.lambda_handler(sqs_event("ObjectCreated:Put", "plot", 1200), None)
        rows = [json.loads(line) for line in output.getvalue().splitlines()]
        self.assertEqual(-27, rows[0]["size_delta"])
        self.assertEqual(1200, rows[1]["size_delta"])
        self.assertEqual("plot", rows[1]["object_name"])
        self.assertEqual("event-log", rows[0]["deployment_id"])

    def test_cleaner_deletes_one_largest_object(self):
        s3 = FakeS3()
        cleaner = load_handler("cleaner_test", "handlers/cleaner/cleaner_lambda.py", s3,
                               environment={"BUCKET_NAME": "test-bucket"})
        with patch.dict(os.environ, {"BUCKET_NAME": "test-bucket"}), contextlib.redirect_stdout(io.StringIO()):
            result = cleaner.lambda_handler({}, None)
        self.assertEqual("large.txt", result["object_name"])
        self.assertEqual([{"Bucket": "test-bucket", "Key": "large.txt"}], s3.deleted)


if __name__ == "__main__":
    unittest.main()
