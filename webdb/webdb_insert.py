from aws_cdk import (
    Duration,
    RemovalPolicy,
    Size,
    Stack,
    aws_iam as _iam,
    aws_lambda as _lambda,
    aws_lambda_event_sources as _event_sources,
    aws_logs as _logs,
    aws_s3 as _s3,
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

        insert = _lambda.DockerImageFunction(
            self, 'insert',
            code = _lambda.DockerImageCode.from_image_asset('insert'),
            architecture = _lambda.Architecture.X86_64,
            timeout = Duration.seconds(900),
            memory_size = 2048,
            ephemeral_storage_size = Size.gibibytes(1),
            environment = {
                'DATABASE_BUCKET': database_bucket.bucket_name,
                'ARCHIVE_BUCKET': archive_bucket.bucket_name
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
        database_bucket.grant_put(insert)
        archive_bucket.grant_put(insert)

        insert.add_event_source(
            _event_sources.SqsEventSource(
                insert_queue,
                batch_size = 10
            )
        )
