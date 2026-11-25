"""Two independent SQS consumers and the log-to-alarm cleanup path."""
from pathlib import Path

from aws_cdk import CfnOutput, Duration, RemovalPolicy, Stack
from aws_cdk import aws_cloudwatch as cloudwatch
from aws_cdk import aws_cloudwatch_actions as cloudwatch_actions
from aws_cdk import aws_dynamodb as dynamodb
from aws_cdk import aws_iam as iam
from aws_cdk import aws_lambda as lambda_
from aws_cdk import aws_lambda_event_sources as event_sources
from aws_cdk import aws_logs as logs
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_sns as sns
from aws_cdk import aws_sns_subscriptions as subscriptions
from aws_cdk import aws_sqs as sqs
from constructs import Construct

HANDLERS = Path(__file__).resolve().parents[1] / "handlers"


class ConsumerStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, *,
                 bucket: s3.IBucket, table: dynamodb.ITable,
                 topic: sns.ITopic, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        tracking_queue = sqs.Queue(
            self, "TrackingQueue", visibility_timeout=Duration.seconds(120),
            removal_policy=RemovalPolicy.DESTROY,
        )
        logging_queue = sqs.Queue(
            self, "LoggingQueue", visibility_timeout=Duration.seconds(120),
            removal_policy=RemovalPolicy.DESTROY,
        )
        # Raw delivery lets each SQS record body contain the S3 event JSON.
        topic.add_subscription(subscriptions.SqsSubscription(
            tracking_queue, raw_message_delivery=True,
        ))
        topic.add_subscription(subscriptions.SqsSubscription(
            logging_queue, raw_message_delivery=True,
        ))

        tracker = lambda_.Function(
            self, "SizeTracker", runtime=lambda_.Runtime.PYTHON_3_13,
            architecture=lambda_.Architecture.X86_64,
            code=lambda_.Code.from_asset(str(HANDLERS / "size_tracking")),
            handler="size_tracking_lambda.lambda_handler",
            memory_size=256, timeout=Duration.seconds(30),
            environment={"TABLE_NAME": table.table_name},
        )
        tracker.add_to_role_policy(iam.PolicyStatement(
            actions=["s3:ListBucket"], resources=[bucket.bucket_arn],
        ))
        tracker.add_to_role_policy(iam.PolicyStatement(
            actions=["dynamodb:PutItem"], resources=[table.table_arn],
        ))
        tracker.add_event_source(event_sources.SqsEventSource(tracking_queue, batch_size=1))

        logging_group = logs.LogGroup(
            self, "S3EventLogs", retention=logs.RetentionDays.ONE_WEEK,
            removal_policy=RemovalPolicy.DESTROY,
        )
        logger = lambda_.Function(
            self, "EventLogger", runtime=lambda_.Runtime.PYTHON_3_13,
            architecture=lambda_.Architecture.X86_64,
            code=lambda_.Code.from_asset(str(HANDLERS / "logging")),
            handler="logging_lambda.lambda_handler",
            memory_size=256, timeout=Duration.seconds(30),
            log_group=logging_group,
            environment={"LOG_GROUP_NAME": logging_group.log_group_name},
        )
        logger.add_to_role_policy(iam.PolicyStatement(
            actions=["logs:FilterLogEvents"], resources=[logging_group.log_group_arn],
        ))
        logger.add_event_source(event_sources.SqsEventSource(logging_queue, batch_size=1))

        metric_filter = logs.MetricFilter(
            self, "SizeDeltaMetricFilter", log_group=logging_group,
            filter_pattern=logs.FilterPattern.literal(
                '{ $.size_delta = * && $.object_name != "plot" }'
            ),
            metric_namespace="Assignment4App", metric_name="TotalObjectSize",
            metric_value="$.size_delta",
            dimensions={"Deployment": "$.deployment_id"},
        )
        delta_metric = cloudwatch.Metric(
            namespace="Assignment4App", metric_name="TotalObjectSize",
            dimensions_map={"Deployment": logging_group.log_group_name},
            statistic="sum", period=Duration.minutes(1),
        )
        metric = cloudwatch.MathExpression(
            expression="RUNNING_SUM(m1)",
            using_metrics={"m1": delta_metric},
            period=Duration.minutes(1),
        )
        cleaner = lambda_.Function(
            self, "Cleaner", runtime=lambda_.Runtime.PYTHON_3_13,
            architecture=lambda_.Architecture.X86_64,
            code=lambda_.Code.from_asset(str(HANDLERS / "cleaner")),
            handler="cleaner_lambda.lambda_handler",
            memory_size=256, timeout=Duration.seconds(30),
            environment={"BUCKET_NAME": bucket.bucket_name},
        )
        cleaner.add_to_role_policy(iam.PolicyStatement(
            actions=["s3:ListBucket"], resources=[bucket.bucket_arn],
        ))
        cleaner.add_to_role_policy(iam.PolicyStatement(
            actions=["s3:DeleteObject"], resources=[bucket.arn_for_objects("*")],
        ))
        self.alarm = cloudwatch.Alarm(
            self, "BucketSizeAlarm", metric=metric, threshold=20,
            comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
            evaluation_periods=1, datapoints_to_alarm=1,
            treat_missing_data=cloudwatch.TreatMissingData.NOT_BREACHING,
        )
        self.alarm.add_alarm_action(cloudwatch_actions.LambdaAction(cleaner))
        self.alarm.node.add_dependency(metric_filter)

        CfnOutput(self, "TrackingQueueUrl", value=tracking_queue.queue_url)
        CfnOutput(self, "LoggingQueueUrl", value=logging_queue.queue_url)
        CfnOutput(self, "LoggingLogGroup", value=logging_group.log_group_name)
        CfnOutput(self, "AlarmName", value=self.alarm.alarm_name)
        CfnOutput(self, "CleanerFunctionName", value=cleaner.function_name)
