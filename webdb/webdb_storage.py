from aws_cdk import (
    Duration,
    RemovalPolicy,
    Stack,
    aws_athena as _athena,
    aws_glue as _glue,
    aws_s3 as _s3
)

from constructs import Construct

class WebdbStorage(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        region = Stack.of(self).region

        for namespace in ['database', 'enrich', 'insert', 'archive', 'temporary']:

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

            if namespace == 'temporary':
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
            table_input = _glue.CfnTable.TableInputProperty(
                name = 'domains',
                description = 'Domains parquet table for Athena',
                table_type = 'EXTERNAL_TABLE',
                parameters = {
                    'classification': 'parquet',
                    'typeOfData': 'file',
                    'projection.enabled': 'true',
                    'projection.year.type': 'integer',
                    'projection.year.range': '2020,2100',
                    'projection.month.type': 'integer',
                    'projection.month.range': '1,12',
                    'projection.month.digits': '2',
                    'projection.day.type': 'integer',
                    'projection.day.range': '1,31',
                    'projection.day.digits': '2',
                    'storage.location.template': f's3://webdb-{region}-database/year=${{year}}/month=${{month}}/day=${{day}}/'
                },
                storage_descriptor = _glue.CfnTable.StorageDescriptorProperty(
                    location = f's3://webdb-{region}-database/',
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
                        _glue.CfnTable.ColumnProperty(name = 'asn', type = 'bigint')
                    ],
                    serde_info = _glue.CfnTable.SerdeInfoProperty(
                        serialization_library = 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
                    )
                )
            )
        )

        domains_table.add_property_override(
            'TableInput.PartitionKeys',
            [
                {'Name': 'year', 'Type': 'int'},
                {'Name': 'month', 'Type': 'int'},
                {'Name': 'day', 'Type': 'int'}
            ]
        )

        domains_table.node.add_dependency(glue_database)

        workgroup = _athena.CfnWorkGroup(
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

        sample_query = _athena.CfnNamedQuery(
            self, 'webdb-athena-domains-sample-query',
            database = 'webdb',
            name = 'domains_latest_100',
            description = 'Partition-pruned sample query for latest 100 rows from today',
            query_string = (
                'SELECT dns, ip, rank, ts, asn '\
                'FROM webdb.domains '\
                "WHERE year = CAST(date_format(current_date, '%Y') AS integer) "\
                "AND month = CAST(date_format(current_date, '%m') AS integer) "\
                "AND day = CAST(date_format(current_date, '%d') AS integer) "\
                'ORDER BY ts DESC '\
                'LIMIT 100'
            ),
            work_group = workgroup.name
        )

        partition_query = _athena.CfnNamedQuery(
            self, 'webdb-athena-domains-partition-query',
            database = 'webdb',
            name = 'domains_today_100',
            description = 'Partition-pruned sample query for today',
            query_string = (
                'SELECT dns, ip, rank, ts, asn '
                'FROM webdb.domains '
                "WHERE year = CAST(date_format(current_date, '%Y') AS integer) "
                "AND month = CAST(date_format(current_date, '%m') AS integer) "
                "AND day = CAST(date_format(current_date, '%d') AS integer) "
                'ORDER BY ts DESC '
                'LIMIT 100'
            ),
            work_group = workgroup.name
        )

        sample_query.node.add_dependency(workgroup)
        sample_query.node.add_dependency(domains_table)
        partition_query.node.add_dependency(workgroup)
        partition_query.node.add_dependency(domains_table)
