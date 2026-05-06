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


class WebdbUnzip(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        account = Stack.of(self).account
        region = Stack.of(self).region

        unzip_bucket = _s3.Bucket.from_bucket_name(
            self, 'unzip-bucket',
            bucket_name = f'webdb-{region}-unzip'
        )

        insert_bucket = _s3.Bucket.from_bucket_name(
            self, 'insert-bucket',
            bucket_name = f'webdb-{region}-insert'
        )

        unzip_queue_dlq = _sqs.Queue(
            self, 'unzip-queue-dlq',
            queue_name = f'webdb-{region}-unzip-events-dlq',
            retention_period = Duration.days(14)
        )

        unzip_queue = _sqs.Queue(
            self, 'unzip-queue',
            queue_name = f'webdb-{region}-unzip-events',
            visibility_timeout = Duration.seconds(900),
            retention_period = Duration.days(4),
            dead_letter_queue = _sqs.DeadLetterQueue(
                queue = unzip_queue_dlq,
                max_receive_count = 5
            )
        )

        unzip_queue.add_to_resource_policy(
            _iam.PolicyStatement(
                sid = 'AllowS3SendMessage',
                principals = [_iam.ServicePrincipal('s3.amazonaws.com')],
                actions = ['sqs:SendMessage'],
                resources = [unzip_queue.queue_arn],
                conditions = {
                    'ArnEquals': {
                        'aws:SourceArn': unzip_bucket.bucket_arn
                    },
                    'StringEquals': {
                        'aws:SourceAccount': account
                    }
                }
            )
        )

        unzip_bucket.add_event_notification(
            _s3.EventType.OBJECT_CREATED,
            _notifications.SqsDestination(unzip_queue),
            _s3.NotificationKeyFilter(suffix = '.gz')
        )

        unzip = _lambda.Function(
            self, 'unzip',
            function_name = 'unzip',
            runtime = _lambda.Runtime.PYTHON_3_13,
            architecture = _lambda.Architecture.ARM_64,
            code = _lambda.Code.from_asset('unzip'),
            handler = 'unzip.handler',
            timeout = Duration.seconds(900),
            memory_size = 2048,
            ephemeral_storage_size = Size.gibibytes(1),
            environment = {
                'INSERT_BUCKET': insert_bucket.bucket_name
            }
        )

        _logs.LogGroup(
            self, 'logs',
            log_group_name = '/aws/lambda/' + unzip.function_name,
            retention = _logs.RetentionDays.ONE_WEEK,
            removal_policy = RemovalPolicy.DESTROY
        )

        unzip_bucket.grant_read(unzip)
        unzip_bucket.grant_delete(unzip)
        insert_bucket.grant_put(unzip)
        unzip.add_event_source(
            _event_sources.SqsEventSource(
                unzip_queue,
                batch_size = 10
            )
        )
