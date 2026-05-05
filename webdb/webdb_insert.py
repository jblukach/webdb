from aws_cdk import (
    Duration,
    RemovalPolicy,
    Size,
    Stack,
    aws_dynamodb as _dynamodb,
    aws_glue as _glue,
    aws_iam as _iam,
    aws_lambda as _lambda,
    aws_lambda_event_sources as _event_sources,
    aws_logs as _logs,
    aws_s3 as _s3,
    aws_s3_assets as _assets,
    aws_s3_notifications as _notifications,
    aws_sqs as _sqs
)

from constructs import Construct


class WebdbInsert(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        account = Stack.of(self).account
        region = Stack.of(self).region

        insert_bucket = _s3.Bucket.from_bucket_name(
            self, 'insert-bucket',
            bucket_name = f'webdb-{region}-insert'
        )

        database_bucket = _s3.Bucket.from_bucket_name(
            self, 'database-bucket',
            bucket_name = f'webdb-{region}-database'
        )

        archive_bucket = _s3.Bucket.from_bucket_name(
            self, 'archive-bucket',
            bucket_name = f'webdb-{region}-archive'
        )

        insert_queue_dlq = _sqs.Queue(
            self, 'insert-queue-dlq',
            queue_name = f'webdb-{region}-insert-events-dlq',
            retention_period = Duration.days(14)
        )

        insert_queue = _sqs.Queue(
            self, 'insert-queue',
            queue_name = f'webdb-{region}-insert-events',
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
            _notifications.SqsDestination(insert_queue),
            _s3.NotificationKeyFilter(suffix = '.jsonl')
        )

        glue_script_asset = _assets.Asset(
            self, 'insert-glue-job-script',
            path = 'insert/glue_insert_job.py'
        )

        glue_job_role = _iam.Role(
            self, 'insert-glue-job-role',
            assumed_by = _iam.ServicePrincipal('glue.amazonaws.com'),
            managed_policies = [
                _iam.ManagedPolicy.from_aws_managed_policy_name('service-role/AWSGlueServiceRole')
            ]
        )

        insert_bucket.grant_read(glue_job_role)
        database_bucket.grant_read_write(glue_job_role)
        glue_script_asset.grant_read(glue_job_role)

        glue_job = _glue.CfnJob(
            self, 'insert-glue-job',
            name = f'webdb-{region}-insert-iceberg',
            role = glue_job_role.role_arn,
            glue_version = '5.0',
            worker_type = 'G.1X',
            number_of_workers = 2,
            execution_property = _glue.CfnJob.ExecutionPropertyProperty(
                max_concurrent_runs = 4
            ),
            command = _glue.CfnJob.JobCommandProperty(
                name = 'glueetl',
                python_version = '3',
                script_location = glue_script_asset.s3_object_url
            ),
            default_arguments = {
                '--job-language': 'python',
                '--datalake-formats': 'iceberg',
                '--warehouse_path': f's3://{database_bucket.bucket_name}/',
                '--enable-glue-datacatalog': 'true',
                '--enable-continuous-cloudwatch-log': 'true',
                '--enable-metrics': 'true'
            },
            timeout = 60,
            max_retries = 0
        )

        processed_objects_table = _dynamodb.Table(
            self, 'processed-objects',
            table_name = f'webdb-{region}-processed-objects',
            billing_mode = _dynamodb.BillingMode.PAY_PER_REQUEST,
            partition_key = _dynamodb.Attribute(
                name = 'pk',
                type = _dynamodb.AttributeType.STRING
            ),
            point_in_time_recovery = False,
            removal_policy = RemovalPolicy.DESTROY,
            time_to_live_attribute = 'ttl'
        )

        insert = _lambda.Function(
            self, 'insert',
            runtime = _lambda.Runtime.PYTHON_3_12,
            handler = 'insert.handler',
            code = _lambda.Code.from_asset('insert'),
            architecture = _lambda.Architecture.X86_64,
            timeout = Duration.seconds(900),
            memory_size = 4096,
            ephemeral_storage_size = Size.gibibytes(2),
            environment = {
                'ARCHIVE_BUCKET': archive_bucket.bucket_name,
                'GLUE_JOB_NAME': glue_job.name,
                'GLUE_TIMEOUT_SECONDS': '840',
                'GLUE_POLL_SECONDS': '10',
                'PROCESSED_OBJECTS_TABLE': processed_objects_table.table_name
            }
        )

        _logs.LogGroup(
            self, 'logs',
            log_group_name = '/aws/lambda/' + insert.function_name,
            retention = _logs.RetentionDays.ONE_WEEK,
            removal_policy = RemovalPolicy.DESTROY
        )

        insert_bucket.grant_read(insert)
        insert_bucket.grant_delete(insert)
        archive_bucket.grant_put(insert)
        processed_objects_table.grant_read_write_data(insert)
        insert.add_to_role_policy(
            _iam.PolicyStatement(
                actions = [
                    'glue:StartJobRun',
                    'glue:GetJobRun',
                    'glue:BatchStopJobRun'
                ],
                resources = [
                    Stack.of(self).format_arn(
                        service = 'glue',
                        resource = 'job',
                        resource_name = glue_job.name
                    )
                ]
            )
        )

        insert.node.add_dependency(glue_job)

        insert.add_event_source(
            _event_sources.SqsEventSource(
                insert_queue,
                batch_size = 10
            )
        )
