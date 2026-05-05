from aws_cdk import (
    CfnOutput,
    Duration,
    RemovalPolicy,
    Stack,
    aws_athena as _athena,
    aws_glue as _glue,
    aws_iam as _iam,
    aws_s3 as _s3
)

from constructs import Construct

class WebdbStorage(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        region = Stack.of(self).region

        for namespace in ['database', 'enrich', 'insert', 'archive', 'temporary', 'output']:

            bucket = _s3.Bucket(
                self, namespace,
                bucket_name = f"webdb-{region}-{namespace}",
                encryption = _s3.BucketEncryption.S3_MANAGED,
                block_public_access = _s3.BlockPublicAccess.BLOCK_ALL,
                removal_policy = RemovalPolicy.RETAIN,
                auto_delete_objects = False,
                enforce_ssl = True,
                versioned = False
            )

            if namespace in ('temporary', 'output'):
                bucket.add_lifecycle_rule(
                    expiration = Duration.days(1),
                    noncurrent_version_expiration = Duration.days(1)
                )

        glue_database = _glue.CfnDatabase(
            self, 'webdb-glue-database',
            catalog_id = Stack.of(self).account,
            database_input = _glue.CfnDatabase.DatabaseInputProperty(
                name = 'webdb',
                description = 'Webdb domain catalog'
            )
        )

        domains_table = _glue.CfnTable(
            self, 'webdb-glue-domains-table',
            catalog_id = Stack.of(self).account,
            database_name = 'webdb',
            open_table_format_input = _glue.CfnTable.OpenTableFormatInputProperty(
                iceberg_input = _glue.CfnTable.IcebergInputProperty(
                    metadata_operation = 'CREATE',
                    version = '2'
                )
            ),
            table_input = _glue.CfnTable.TableInputProperty(
                name = 'domains',
                description = 'Domains Iceberg table for Athena',
                table_type = 'EXTERNAL_TABLE',
                parameters = {
                    'table_type': 'ICEBERG',
                    'format': 'parquet',
                    'write_compression': 'zstd'
                },
                storage_descriptor = _glue.CfnTable.StorageDescriptorProperty(
                    location = f's3://webdb-{region}-database/domains/',
                    input_format = 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
                    output_format = 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
                    compressed = False,
                    columns = [
                        _glue.CfnTable.ColumnProperty(name = 'dns', type = 'string'),
                        _glue.CfnTable.ColumnProperty(name = 'ns', type = 'array<string>'),
                        _glue.CfnTable.ColumnProperty(name = 'ip', type = 'string'),
                        _glue.CfnTable.ColumnProperty(name = 'co', type = 'string'),
                        _glue.CfnTable.ColumnProperty(name = 'web', type = 'string'),
                        _glue.CfnTable.ColumnProperty(name = 'eml', type = 'string'),
                        _glue.CfnTable.ColumnProperty(name = 'hold', type = 'string'),
                        _glue.CfnTable.ColumnProperty(name = 'tel', type = 'bigint'),
                        _glue.CfnTable.ColumnProperty(name = 'rank', type = 'bigint'),
                        _glue.CfnTable.ColumnProperty(name = 'ts', type = 'string'),
                        _glue.CfnTable.ColumnProperty(name = 'id', type = 'string'),
                        _glue.CfnTable.ColumnProperty(name = 'sld', type = 'string'),
                        _glue.CfnTable.ColumnProperty(name = 'tld', type = 'string'),
                        _glue.CfnTable.ColumnProperty(name = 'asn', type = 'bigint'),
                        _glue.CfnTable.ColumnProperty(name = 'year', type = 'int'),
                        _glue.CfnTable.ColumnProperty(name = 'month', type = 'int'),
                        _glue.CfnTable.ColumnProperty(name = 'day', type = 'int')
                    ],
                    serde_info = _glue.CfnTable.SerdeInfoProperty(
                        serialization_library = 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
                    )
                )
            )
        )

        domains_table.node.add_dependency(glue_database)

        optimizer_role = _iam.Role(
            self, 'webdb-iceberg-optimizer-role',
            assumed_by = _iam.ServicePrincipal('glue.amazonaws.com'),
            managed_policies = [
                _iam.ManagedPolicy.from_aws_managed_policy_name('service-role/AWSGlueServiceRole')
            ]
        )

        _s3.Bucket.from_bucket_name(
            self, 'database-bucket-for-optimizer',
            bucket_name = f'webdb-{region}-database'
        ).grant_read_write(optimizer_role)

        optimizer_role.add_to_policy(
            _iam.PolicyStatement(
                actions = [
                    'glue:GetCatalog',
                    'glue:GetDatabase',
                    'glue:GetDatabases',
                    'glue:GetTable',
                    'glue:GetTables',
                    'glue:GetTableVersion',
                    'glue:GetTableVersions',
                    'glue:UpdateTable'
                ],
                resources = [
                    f'arn:aws:glue:{region}:{Stack.of(self).account}:catalog',
                    f'arn:aws:glue:{region}:{Stack.of(self).account}:database/webdb',
                    f'arn:aws:glue:{region}:{Stack.of(self).account}:table/webdb/domains'
                ]
            )
        )

        optimizer_role.add_to_policy(
            _iam.PolicyStatement(
                actions = ['lakeformation:GetDataAccess'],
                resources = ['*']
            )
        )

        _ = CfnOutput(
            self, 'webdb-iceberg-optimizer-role-arn',
            description = 'IAM role ARN to select when enabling Glue Iceberg table optimization in console',
            value = optimizer_role.role_arn
        )

        _ = CfnOutput(
            self, 'webdb-iceberg-optimizer-role-name',
            description = 'IAM role name for Glue Iceberg table optimization in console',
            value = optimizer_role.role_name
        )

        _ = _athena.CfnWorkGroup(
            self, 'webdb-athena-workgroup',
            name = 'webdb',
            description = 'Athena workgroup for webdb queries',
            state = 'ENABLED',
            work_group_configuration = _athena.CfnWorkGroup.WorkGroupConfigurationProperty(
                enforce_work_group_configuration = True,
                publish_cloud_watch_metrics_enabled = True,
                result_configuration = _athena.CfnWorkGroup.ResultConfigurationProperty(
                    output_location = f's3://webdb-{region}-temporary/athena-results/'
                )
            )
        )
