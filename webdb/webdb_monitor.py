from aws_cdk import (
    Duration,
    RemovalPolicy,
    Size,
    Stack,
    aws_dynamodb as _dynamodb,
    aws_events as _events,
    aws_events_targets as _targets,
    aws_lambda as _lambda,
    aws_logs as _logs,
    aws_s3 as _s3,
    aws_sns as _sns,
    aws_sns_subscriptions as _subs,
)

from constructs import Construct


class WebdbMonitor(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        region = Stack.of(self).region

        execution_table = _dynamodb.TableV2(
            self, 'executions',
            table_name=f'webdb-{region}-executions',
            partition_key={
                'name': 'pk',
                'type': _dynamodb.AttributeType.STRING,
            },
            sort_key={
                'name': 'sk',
                'type': _dynamodb.AttributeType.STRING,
            },
            billing=_dynamodb.Billing.on_demand(),
            removal_policy=RemovalPolicy.DESTROY,
            point_in_time_recovery_specification=_dynamodb.PointInTimeRecoverySpecification(
                point_in_time_recovery_enabled=True
            ),
            deletion_protection=True,
            time_to_live_attribute='ttl',
        )

        alerts_topic = _sns.Topic(
            self, 'pipeline-alerts-topic',
            topic_name=f'webdb-{region}-pipeline-alerts',
            display_name='webdb pipeline alerts',
        )

        alerts_topic.add_subscription(
            _subs.EmailSubscription('hello@lukach.io')
        )

        monitor = _lambda.Function(
            self, 'monitor',
            function_name='monitor',
            runtime=_lambda.Runtime.PYTHON_3_13,
            architecture=_lambda.Architecture.ARM_64,
            code=_lambda.Code.from_asset('monitor'),
            handler='monitor.handler',
            timeout=Duration.seconds(900),
            memory_size=1024,
            ephemeral_storage_size=Size.gibibytes(1),
            environment={
                'EXECUTION_TABLE': execution_table.table_name,
                'ARCHIVE_BUCKET': f'webdb-{region}-archive',
                'ALERT_TOPIC_ARN': alerts_topic.topic_arn,
            },
        )

        _logs.LogGroup(
            self, 'logs',
            log_group_name='/aws/lambda/' + monitor.function_name,
            retention=_logs.RetentionDays.ONE_WEEK,
            removal_policy=RemovalPolicy.DESTROY,
        )

        run_table = _dynamodb.TableV2.from_table_name(
            self, 'run-table',
            table_name='run',
        )

        processed_table = _dynamodb.TableV2.from_table_name(
            self, 'processed-objects',
            table_name=f'webdb-{region}-processed-objects',
        )

        output_bucket = _s3.Bucket.from_bucket_name(
            self, 'output-bucket',
            bucket_name=f'webdb-{region}-output',
        )

        insert_bucket = _s3.Bucket.from_bucket_name(
            self, 'insert-bucket',
            bucket_name=f'webdb-{region}-insert',
        )

        archive_bucket = _s3.Bucket.from_bucket_name(
            self, 'archive-bucket',
            bucket_name=f'webdb-{region}-archive',
        )

        execution_table.grant_read_write_data(monitor)
        run_table.grant_write_data(monitor)
        processed_table.grant_write_data(monitor)
        output_bucket.grant_read_write(monitor)
        insert_bucket.grant_read_write(monitor)
        archive_bucket.grant_put(monitor)
        alerts_topic.grant_publish(monitor)

        _events.Rule(
            self, 'athena-query-state-change',
            description='Monitor Athena query state changes for webdb',
            event_pattern=_events.EventPattern(
                source=['aws.athena'],
                detail_type=['Athena Query State Change'],
                detail={
                    'workgroupName': ['webdb'],
                    'currentState': ['SUCCEEDED', 'FAILED', 'CANCELLED'],
                },
            ),
            targets=[_targets.LambdaFunction(monitor)],
        )

        _events.Rule(
            self, 'glue-job-state-change',
            description='Monitor Glue job state changes for webdb insert job',
            event_pattern=_events.EventPattern(
                source=['aws.glue'],
                detail_type=['Glue Job State Change'],
                detail={
                    'jobName': [f'webdb-{region}-insert-iceberg'],
                    'state': ['SUCCEEDED', 'FAILED', 'STOPPED', 'TIMEOUT', 'ERROR', 'EXPIRED'],
                },
            ),
            targets=[_targets.LambdaFunction(monitor)],
        )
