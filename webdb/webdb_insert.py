from aws_cdk import (
    Duration,
    RemovalPolicy,
    Stack,
    aws_glue as _glue,
    aws_iam as _iam,
    aws_lambda as _lambda,
    aws_lambda_event_sources as _event_sources,
    aws_logs as _logs,
    aws_s3 as _s3,
    aws_s3_assets as _assets,
    aws_s3_notifications as _notifications,
    aws_sqs as _sqs,
)

from constructs import Construct


class WebdbInsert(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        account = Stack.of(self).account

    ### IMPORT EXISTING INSERT BUCKET ###

        insert_bucket = _s3.Bucket.from_bucket_name(
            self,
            'insert-bucket',
            bucket_name=f'webdb-{Stack.of(self).region}-insert',
        )

        temporary_bucket = _s3.Bucket.from_bucket_name(
            self,
            'temporary-bucket',
            bucket_name=f'webdb-{Stack.of(self).region}-temporary',
        )

        archive_bucket = _s3.Bucket.from_bucket_name(
            self,
            'archive-bucket',
            bucket_name=f'webdb-{Stack.of(self).region}-archive',
        )

    ### QUEUE FOR S3 CREATE EVENTS ###

        insert_queue_dlq = _sqs.Queue(
            self,
            'insert-queue-dlq',
            queue_name=f'webdb-{Stack.of(self).region}-insert-events-dlq',
            retention_period=Duration.days(14),
        )

        insert_queue = _sqs.Queue(
            self,
            'insert-queue',
            queue_name=f'webdb-{Stack.of(self).region}-insert-events',
            visibility_timeout=Duration.seconds(5400),
            retention_period=Duration.days(4),
            dead_letter_queue=_sqs.DeadLetterQueue(
                queue=insert_queue_dlq,
                max_receive_count=5,
            ),
        )

        insert_queue.add_to_resource_policy(
            _iam.PolicyStatement(
                sid='AllowS3SendMessage',
                principals=[_iam.ServicePrincipal('s3.amazonaws.com')],
                actions=['sqs:SendMessage'],
                resources=[insert_queue.queue_arn],
                conditions={
                    'ArnEquals': {'aws:SourceArn': insert_bucket.bucket_arn},
                    'StringEquals': {'aws:SourceAccount': account},
                },
            )
        )

        insert_bucket.add_event_notification(
            _s3.EventType.OBJECT_CREATED,
            _notifications.SqsDestination(insert_queue),
        )

    ### GLUE JOB ###

        glue_job_role = _iam.Role(
            self,
            'insert-glue-role',
            assumed_by=_iam.ServicePrincipal('glue.amazonaws.com'),
        )

        glue_job_role.add_managed_policy(
            _iam.ManagedPolicy.from_aws_managed_policy_name('service-role/AWSGlueServiceRole')
        )

        glue_job_role.add_to_policy(
            _iam.PolicyStatement(
                actions=['s3:GetObject', 's3:ListBucket'],
                resources=[
                    f'arn:aws:s3:::{insert_bucket.bucket_name}',
                    f'arn:aws:s3:::{insert_bucket.bucket_name}/*',
                ],
            )
        )

        glue_job_role.add_to_policy(
            _iam.PolicyStatement(
                actions=[
                    'glue:GetDatabase',
                    'glue:GetDatabases',
                    'glue:GetTable',
                    'glue:GetTables',
                    'glue:GetPartitions',
                ],
                resources=['*'],
            )
        )

        glue_job_role.add_to_policy(
            _iam.PolicyStatement(
                actions=[
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
                resources=['*'],
            )
        )

        glue_script_asset = _assets.Asset(
            self,
            'insert-glue-script-asset',
            path='insert/glue_insert_job.py',
        )
        glue_script_asset.grant_read(glue_job_role)

        glue_job = _glue.CfnJob(
            self,
            'insert-glue-job',
            name='webdb-insert-job',
            role=glue_job_role.role_arn,
            command=_glue.CfnJob.JobCommandProperty(
                name='glueetl',
                script_location=glue_script_asset.s3_object_url,
                python_version='3',
            ),
            glue_version='4.0',
            execution_property=_glue.CfnJob.ExecutionPropertyProperty(max_concurrent_runs=4),
            max_retries=0,
            timeout=60,
            number_of_workers=2,
            worker_type='G.1X',
            default_arguments={
                '--job-language': 'python',
                '--enable-continuous-cloudwatch-log': 'true',
                '--enable-glue-datacatalog': 'true',
                '--enable-metrics': 'true',
                '--datalake-formats': 'iceberg',
            },
        )

    ### LAMBDA FUNCTION ###

        role = _iam.Role(
            self,
            'role',
            assumed_by=_iam.ServicePrincipal('lambda.amazonaws.com'),
        )

        role.add_managed_policy(
            _iam.ManagedPolicy.from_aws_managed_policy_name('service-role/AWSLambdaBasicExecutionRole')
        )

        role.add_to_policy(
            _iam.PolicyStatement(
                actions=['s3:GetObject', 's3:PutObject', 's3:DeleteObject'],
                resources=[
                    f'arn:aws:s3:::{insert_bucket.bucket_name}/*',
                    f'arn:aws:s3:::{temporary_bucket.bucket_name}/*',
                    f'arn:aws:s3:::{archive_bucket.bucket_name}/*',
                ],
            )
        )

        role.add_to_policy(
            _iam.PolicyStatement(
                actions=['s3:ListBucket', 's3:GetBucketLocation'],
                resources=[
                    f'arn:aws:s3:::{insert_bucket.bucket_name}',
                    f'arn:aws:s3:::{temporary_bucket.bucket_name}',
                    f'arn:aws:s3:::{archive_bucket.bucket_name}',
                ],
            )
        )

        role.add_to_policy(
            _iam.PolicyStatement(
                actions=['glue:StartJobRun', 'glue:GetJobRun', 'glue:BatchStopJobRun'],
                resources=['*'],
            )
        )

        insert = _lambda.Function(
            self,
            'insert',
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler='insert.handler',
            code=_lambda.Code.from_asset(
                'insert',
                exclude=['glue_insert_job.py'],
            ),
            timeout=Duration.seconds(900),
            memory_size=2048,
            role=role,
            environment={
                'TARGET_CATALOG': 's3tablescatalog',
                'ATHENA_TARGET_DATABASE': 'webdb',
                'TABLE_NAME': 'domains',
                'GLUE_JOB_NAME': glue_job.name,
                'GLUE_TIMEOUT_SECONDS': '3600',
                'GLUE_POLL_SECONDS': '10',
            },
        )

        _logs.LogGroup(
            self,
            'logs',
            log_group_name='/aws/lambda/' + insert.function_name,
            retention=_logs.RetentionDays.ONE_WEEK,
            removal_policy=RemovalPolicy.DESTROY,
        )

        insert.add_event_source(
            _event_sources.SqsEventSource(
                insert_queue,
                batch_size=10,
            )
        )
