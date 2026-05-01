from aws_cdk import (
	Duration,
	RemovalPolicy,
	Size,
	Stack,
	aws_dynamodb as _dynamodb,
	aws_iam as _iam,
	aws_lambda as _lambda,
	aws_lambda_event_sources as _event_sources,
	aws_logs as _logs,
	aws_s3 as _s3,
	aws_s3_notifications as _notifications,
	aws_sqs as _sqs
)

from constructs import Construct


class WebdbOutput(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        account = Stack.of(self).account
        region = Stack.of(self).region

        output_bucket = _s3.Bucket.from_bucket_name(
            self, 'output-bucket',
            bucket_name = f'webdb-{region}-output'
        )

        output_queue_dlq = _sqs.Queue(
            self, 'output-queue-dlq',
            queue_name = f'webdb-{region}-output-events-dlq',
            retention_period = Duration.days(14)
        )

        output_queue = _sqs.Queue(
            self, 'output-queue',
            queue_name = f'webdb-{region}-output-events',
            visibility_timeout = Duration.seconds(900),
            retention_period = Duration.days(4),
            dead_letter_queue = _sqs.DeadLetterQueue(
                queue = output_queue_dlq,
                max_receive_count = 5
            )
        )

        output_queue.add_to_resource_policy(
            _iam.PolicyStatement(
                sid = 'AllowS3SendMessage',
                principals = [_iam.ServicePrincipal('s3.amazonaws.com')],
                actions = ['sqs:SendMessage'],
                resources = [output_queue.queue_arn],
                conditions = {
                    'ArnEquals': {
                        'aws:SourceArn': output_bucket.bucket_arn
                    },
                    'StringEquals': {
                        'aws:SourceAccount': account
                    }
                }
            )
        )

        output_bucket.add_event_notification(
            _s3.EventType.OBJECT_CREATED,
            _notifications.SqsDestination(output_queue),
            _s3.NotificationKeyFilter(suffix = '.gz')
        )

        possibilities = _dynamodb.TableV2.from_table_name(
            self, 'possibilities',
            table_name = 'possibilities'
        )

        output = _lambda.Function(
            self, 'output',
            function_name = 'output',
            runtime = _lambda.Runtime.PYTHON_3_13,
            architecture = _lambda.Architecture.ARM_64,
            code = _lambda.Code.from_asset('output'),
            handler = 'output.handler',
            timeout = Duration.seconds(900),
            memory_size = 2048,
            ephemeral_storage_size = Size.gibibytes(1)
        )

        _logs.LogGroup(
            self, 'logs',
            log_group_name = '/aws/lambda/' + output.function_name,
            retention = _logs.RetentionDays.ONE_WEEK,
            removal_policy = RemovalPolicy.DESTROY
        )

        output_bucket.grant_read(output)
        output.add_to_role_policy(
            _iam.PolicyStatement(
                sid = 'AllowPossibilitiesQueryAndBatchWrite',
                actions = ['dynamodb:Query', 'dynamodb:BatchWriteItem'],
                resources = [possibilities.table_arn],
                conditions = {
                    'ForAllValues:StringEquals': {
                        'dynamodb:LeadingKeys': ['LUNKER#']
                    }
                }
            )
        )

        output.add_event_source(
            _event_sources.SqsEventSource(
                output_queue,
                batch_size = 10
            )
        )
