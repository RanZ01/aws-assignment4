"""Storage and the S3-to-SNS event source."""

from aws_cdk import CfnOutput, Names, RemovalPolicy, Stack
from aws_cdk import aws_dynamodb as dynamodb
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_s3_notifications as s3n
from aws_cdk import aws_sns as sns
from constructs import Construct


class DataStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.bucket = s3.Bucket(
            self, "TestBucket",
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )
        self.table = dynamodb.Table(
            self, "SizeHistory",
            partition_key=dynamodb.Attribute(
                name="history_group", type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="record_key", type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,
        )
        # CDK derives a unique index name; the plotter receives it via its environment.
        self.size_index_name = f"{Names.unique_id(self.table)}-TotalSize"
        self.table.add_local_secondary_index(
            index_name=self.size_index_name,
            sort_key=dynamodb.Attribute(
                name="total_size", type=dynamodb.AttributeType.NUMBER
            ),
            projection_type=dynamodb.ProjectionType.ALL,
        )
        self.topic = sns.Topic(self, "BucketEvents")
        destination = s3n.SnsDestination(self.topic)
        self.bucket.add_event_notification(s3.EventType.OBJECT_CREATED, destination)
        self.bucket.add_event_notification(s3.EventType.OBJECT_REMOVED, destination)

        CfnOutput(self, "BucketName", value=self.bucket.bucket_name)
        CfnOutput(self, "TableName", value=self.table.table_name)
        CfnOutput(self, "SizeIndexName", value=self.size_index_name)
