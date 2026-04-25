from aws_cdk import (
    RemovalPolicy,
    Stack,
    aws_s3tables as _s3tables,
    custom_resources as _cr,
    aws_s3tables_alpha as _tables
)

from constructs import Construct

NAMESPACE = 'webdb'
TABLE_NAME = 'domains'
SCHEMA_FIELDS = [
    ('dns', 'string'),
    ('ns', 'string'),
    ('ip', 'string'),
    ('co', 'string'),
    ('web', 'string'),
    ('eml', 'string'),
    ('hold', 'string'),
    ('tel', 'long'),
    ('rank', 'long'),
    ('ts', 'string'),
    ('id', 'string'),
    ('sld', 'string'),
    ('tld', 'string'),
    ('asn', 'long'),
]


class WebdbTable(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        table = _tables.TableBucket(
            self, 'table',
            table_bucket_name = 'webdb',
            removal_policy = RemovalPolicy.DESTROY,
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

        table_resource = _s3tables.CfnTable(
            self, 'table-resource',
            namespace = NAMESPACE,
            open_table_format = 'ICEBERG',
            table_bucket_arn = table.table_bucket_arn,
            table_name = TABLE_NAME,
            iceberg_metadata = _s3tables.CfnTable.IcebergMetadataProperty(
                iceberg_schema = _s3tables.CfnTable.IcebergSchemaProperty(
                    schema_field_list = [
                        _s3tables.CfnTable.SchemaFieldProperty(
                            id = index,
                            name = name,
                            type = field_type
                        )
                        for index, (name, field_type) in enumerate(SCHEMA_FIELDS, start = 1)
                    ]
                )
            )
        )
        table_resource.apply_removal_policy(RemovalPolicy.DESTROY)
        table_resource.node.add_dependency(namespace_resource)
