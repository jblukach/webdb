import datetime

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

class WebdbEnrich(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        account = Stack.of(self).account

        year = datetime.datetime.now().strftime('%Y')
        month = datetime.datetime.now().strftime('%m')
        day = datetime.datetime.now().strftime('%d')

    ### S3 BUCKET ###

        bucket = _s3.Bucket.from_bucket_name(
            self, 'bucket',
            bucket_name = 'packages-use2-lukach-io'
        )

    ### LAMBDA LAYERS ###

        geoip2 = _lambda.LayerVersion(
            self, 'geoip2',
            layer_version_name = 'geoip2',
            description = str(year)+'-'+str(month)+'-'+str(day)+' deployment',
            code = _lambda.Code.from_bucket(
                bucket = bucket,
                key = 'geoip2.zip'
            ),
            compatible_architectures = [
                _lambda.Architecture.ARM_64
            ],
            compatible_runtimes = [
                _lambda.Runtime.PYTHON_3_13
            ],
            removal_policy = RemovalPolicy.DESTROY
        )

        maxminddb = _lambda.LayerVersion(
            self, 'maxminddb',
            layer_version_name = 'maxminddb',
            description = str(year)+'-'+str(month)+'-'+str(day)+' deployment',
            code = _lambda.Code.from_bucket(
                bucket = bucket,
                key = 'maxminddb.zip'
            ),
            compatible_architectures = [
                _lambda.Architecture.ARM_64
            ],
            compatible_runtimes = [
                _lambda.Runtime.PYTHON_3_13
            ],
            removal_policy = RemovalPolicy.DESTROY
        )

    ### IMPORT EXISTING ENRICH BUCKET ###

        enrich_bucket = _s3.Bucket.from_bucket_name(
            self, 'enrich-bucket',
            bucket_name = f'webdb-{Stack.of(self).region}-enrich'
        )

    ### QUEUE FOR S3 CREATE EVENTS ###

        enrich_queue_dlq = _sqs.Queue(
            self, 'enrich-queue-dlq',
            queue_name = f'webdb-{Stack.of(self).region}-enrich-events-dlq',
            retention_period = Duration.days(14)
        )

        enrich_queue = _sqs.Queue(
            self, 'enrich-queue',
            queue_name = f'webdb-{Stack.of(self).region}-enrich-events',
            visibility_timeout = Duration.seconds(5400),
            retention_period = Duration.days(4),
            dead_letter_queue = _sqs.DeadLetterQueue(
                queue = enrich_queue_dlq,
                max_receive_count = 5
            )
        )

        enrich_queue.add_to_resource_policy(
            _iam.PolicyStatement(
                sid = 'AllowS3SendMessage',
                principals = [_iam.ServicePrincipal('s3.amazonaws.com')],
                actions = ['sqs:SendMessage'],
                resources = [enrich_queue.queue_arn],
                conditions = {
                    'ArnEquals': {
                        'aws:SourceArn': enrich_bucket.bucket_arn
                    },
                    'StringEquals': {
                        'aws:SourceAccount': account
                    }
                }
            )
        )

        enrich_bucket.add_event_notification(
            _s3.EventType.OBJECT_CREATED,
            _notifications.SqsDestination(enrich_queue)
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
                    's3:DeleteObject',
                    's3:GetObject',
                    's3:PutObject'
                ],
                resources = [
                    '*'
                ]
            )
        )

    ### LAMBDA FUNCTION ###

        enrich = _lambda.Function(
            self, 'enrich',
            runtime = _lambda.Runtime.PYTHON_3_13,
            architecture = _lambda.Architecture.ARM_64,
            code = _lambda.Code.from_asset('enrich'),
            handler = 'enrich.handler',
            timeout = Duration.seconds(900),
            ephemeral_storage_size = Size.gibibytes(1),
            memory_size = 2048,
            role = role,
            layers = [
                geoip2,
                maxminddb
            ]
        )

        _logs.LogGroup(
            self, 'logs',
            log_group_name = '/aws/lambda/'+enrich.function_name,
            retention = _logs.RetentionDays.ONE_WEEK,
            removal_policy = RemovalPolicy.DESTROY
        )

        enrich.add_event_source(
            _event_sources.SqsEventSource(
                enrich_queue,
                batch_size = 10
            )
        )
