from aws_cdk import (
    RemovalPolicy,
    Stack,
    custom_resources as _cr,
    aws_s3tables_alpha as _tables
)

from constructs import Construct

NAMESPACE = 'webdb'
TABLE_NAME = 'domains'


class WebdbTable(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        table = _tables.TableBucket(
            self, 'table',
            table_bucket_name = 'webdb',
            removal_policy = RemovalPolicy.RETAIN,
            unreferenced_file_removal = _tables.UnreferencedFileRemoval(
                status = _tables.UnreferencedFileRemovalStatus.ENABLED,
                noncurrent_days = 14,
                unreferenced_days = 7
            )
        )

    ### CREATE NAMESPACE ###

        namespace_resource = _cr.AwsCustomResource(
            self, 'namespace',
            on_create = _cr.AwsSdkCall(
                service = 's3tables',
                action = 'createNamespace',
                parameters = {
                    'tableBucketARN': table.table_bucket_arn,
                    'namespace': [NAMESPACE]
                },
                physical_resource_id = _cr.PhysicalResourceId.of(
                    f'{table.table_bucket_arn}/namespace/{NAMESPACE}'
                ),
                ignore_error_codes_matching = '409'
            ),
            on_delete = _cr.AwsSdkCall(
                service = 's3tables',
                action = 'deleteNamespace',
                parameters = {
                    'tableBucketARN': table.table_bucket_arn,
                    'namespace': NAMESPACE
                },
                ignore_error_codes_matching = '404'
            ),
            policy = _cr.AwsCustomResourcePolicy.from_sdk_calls(
                resources = _cr.AwsCustomResourcePolicy.ANY_RESOURCE
            ),
            removal_policy = RemovalPolicy.DESTROY
        )

    ### CREATE TABLE ###

        table_resource = _cr.AwsCustomResource(
            self, 'table-resource',
            on_create = _cr.AwsSdkCall(
                service = 's3tables',
                action = 'createTable',
                parameters = {
                    'tableBucketARN': table.table_bucket_arn,
                    'namespace': NAMESPACE,
                    'name': TABLE_NAME,
                    'format': 'ICEBERG'
                },
                physical_resource_id = _cr.PhysicalResourceId.of(
                    f'{table.table_bucket_arn}/namespace/{NAMESPACE}/table/{TABLE_NAME}'
                ),
                ignore_error_codes_matching = '409'
            ),
            on_delete = _cr.AwsSdkCall(
                service = 's3tables',
                action = 'deleteTable',
                parameters = {
                    'tableBucketARN': table.table_bucket_arn,
                    'namespace': NAMESPACE,
                    'name': TABLE_NAME
                },
                ignore_error_codes_matching = '404'
            ),
            policy = _cr.AwsCustomResourcePolicy.from_sdk_calls(
                resources = _cr.AwsCustomResourcePolicy.ANY_RESOURCE
            ),
            removal_policy = RemovalPolicy.DESTROY
        )

        table_resource.node.add_dependency(namespace_resource)
