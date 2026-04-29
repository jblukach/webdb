from aws_cdk import (
    Duration,
    RemovalPolicy,
    Size,
    Stack,
    aws_iam as _iam,
    aws_lambda as _lambda,
    aws_logs as _logs,
    aws_lambda_event_sources as _event_sources,
    aws_s3 as _s3,
    aws_s3_notifications as _notifications,
    aws_sqs as _sqs
)

from constructs import Construct


class WebdbInsert(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        account = Stack.of(self).account

    ### IMPORT EXISTING INSERT BUCKET ###

        insert_bucket = _s3.Bucket.from_bucket_name(
            self, 'insert-bucket',
            bucket_name = f'webdb-{Stack.of(self).region}-insert'
        )

        temporary_bucket = _s3.Bucket.from_bucket_name(
            self, 'temporary-bucket',
            bucket_name = f'webdb-{Stack.of(self).region}-temporary'
        )

        archive_bucket = _s3.Bucket.from_bucket_name(
            self, 'archive-bucket',
            bucket_name = f'webdb-{Stack.of(self).region}-archive'
        )

    ### QUEUE FOR S3 CREATE EVENTS ###

        insert_queue_dlq = _sqs.Queue(
            self, 'insert-queue-dlq',
            queue_name = f'webdb-{Stack.of(self).region}-insert-events-dlq',
            retention_period = Duration.days(14)
        )

        insert_queue = _sqs.Queue(
            self, 'insert-queue',
            queue_name = f'webdb-{Stack.of(self).region}-insert-events',
            visibility_timeout = Duration.seconds(5400),
            retention_period = Duration.days(4),
            dead_letter_queue = _sqs.DeadLetterQueue(
                queue = insert_queue_dlq,
                max_receive_count = 5
            )
        )

        insert_queue.add_to_resource_policy(
            _iam.PolicyStatement(
                sid = 'AllowS3SendMessage',
                principals = [_iam.ServicePrincipal('s3.amazonaws.com')],
                actions = ['sqs:SendMessage'],
                resources = [insert_queue.queue_arn],
                conditions = {
                    'ArnEquals': {
                        'aws:SourceArn': insert_bucket.bucket_arn
                    },
                    'StringEquals': {
                        'aws:SourceAccount': account
                    }
                }
            )
        )

        insert_bucket.add_event_notification(
            _s3.EventType.OBJECT_CREATED,
            _notifications.SqsDestination(insert_queue)
        )

    ### IAM ROLE ###

        role = _iam.Role(
            self, 'role',
            assumed_by = _iam.ServicePrincipal(
                'lambda.amazonaws.com'
            )
        )

        role.add_managed_policy(
            _iam.ManagedPolicy.from_aws_managed_policy_name(
                'service-role/AWSLambdaBasicExecutionRole'
            )
        )

        role.add_to_policy(
            _iam.PolicyStatement(
                actions = [
                    's3:GetObject',
                    's3:PutObject',
                    's3:DeleteObject',
                ],
                resources = [
                    f'arn:aws:s3:::{insert_bucket.bucket_name}/*',
                    f'arn:aws:s3:::{temporary_bucket.bucket_name}/*',
                    f'arn:aws:s3:::{archive_bucket.bucket_name}/*'
                ]
            )
        )

        role.add_to_policy(
            _iam.PolicyStatement(
                actions = ['s3:ListBucket', 's3:GetBucketLocation'],
                resources = [
                    f'arn:aws:s3:::{insert_bucket.bucket_name}',
                    f'arn:aws:s3:::{temporary_bucket.bucket_name}',
                    f'arn:aws:s3:::{archive_bucket.bucket_name}'
                ]
            )
        )

        role.add_to_policy(
            _iam.PolicyStatement(
                actions = [
                    'athena:StartQueryExecution',
                    'athena:GetQueryExecution',
                    'athena:GetQueryResults',
                    'athena:StopQueryExecution',
                    'athena:GetTableMetadata',
                    'athena:GetDataCatalog',
                    'athena:GetWorkGroup',
                ],
                resources = ['*']
            )
        )

        role.add_to_policy(
            _iam.PolicyStatement(
                actions = [
                    'glue:GetDatabase',
                    'glue:CreateDatabase',
                    'glue:GetDatabases',
                    'glue:GetTable',
                    'glue:GetTables',
                    'glue:CreateTable',
                    'glue:DeleteTable',
                ],
                resources = ['*']
            )
        )

        role.add_to_policy(
            _iam.PolicyStatement(
                actions = [
                    's3tables:ListTableBuckets',
                    's3tables:GetTableBucket',
                    's3tables:ListNamespaces',
                    's3tables:ListTables',
                    's3tables:GetNamespace',
                    's3tables:GetTable',
                    's3tables:GetTableMetadataLocation',
                    's3tables:GetTableData',
                    's3tables:PutTableData',
                ],
                resources = ['*']
            )
        )

    ### LAMBDA FUNCTION (DOCKER) ###

        insert = _lambda.DockerImageFunction(
            self, 'insert',
            code = _lambda.DockerImageCode.from_image_asset('insert'),
            timeout = Duration.seconds(900),
            ephemeral_storage_size = Size.gibibytes(1),
            memory_size = 2048,
            role = role,
            environment = {
                'ATHENA_TARGET_CATALOG': 's3tablescatalog/webdb',
                'ATHENA_TARGET_DATABASE': 'webdb',
                'ATHENA_STAGING_DATABASE': 'webdb_staging',
                'ATHENA_WORKGROUP': 'primary',
                'ATHENA_RESULTS_PREFIX': '_athena_results',
                'ATHENA_STAGING_PREFIX': '_athena_staging',
                'ATHENA_TIMEOUT_SECONDS': '540',
                'ATHENA_POLL_SECONDS': '2',
            }
        )

        _logs.LogGroup(
            self, 'logs',
            log_group_name = '/aws/lambda/'+insert.function_name,
            retention = _logs.RetentionDays.ONE_WEEK,
            removal_policy = RemovalPolicy.DESTROY
        )

        insert.add_event_source(
            _event_sources.SqsEventSource(
                insert_queue,
                batch_size = 10
            )
        )
