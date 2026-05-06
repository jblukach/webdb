from aws_cdk import (
    Duration,
    RemovalPolicy,
    Size,
    Stack,
    aws_events as _events,
    aws_events_targets as _targets,
    aws_iam as _iam,
    aws_lambda as _lambda,
    aws_logs as _logs,
    aws_ssm as _ssm
)

from constructs import Construct


class WebdbSchedule(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        region = Stack.of(self).region

        lunker = _ssm.StringParameter.from_string_parameter_attributes(
            self, 'lunker',
            parameter_name = '/account/lunker'
        )

        role = _iam.Role(
            self, 'role',
            assumed_by = _iam.ServicePrincipal('lambda.amazonaws.com')
        )

        role.add_managed_policy(
            _iam.ManagedPolicy.from_aws_managed_policy_name(
                'service-role/AWSLambdaBasicExecutionRole'
            )
        )

        role.add_to_policy(
            _iam.PolicyStatement(
                actions = [
                    'dynamodb:DescribeTable',
                    'dynamodb:Query',
                    'dynamodb:BatchGetItem',
                    'dynamodb:BatchWriteItem'
                ],
                resources = [
                    'arn:aws:dynamodb:' + region + ':' + lunker.string_value + ':table/permutation',
                    'arn:aws:dynamodb:' + region + ':' + Stack.of(self).account + ':table/state',
                    'arn:aws:dynamodb:' + region + ':' + Stack.of(self).account + ':table/run'
                ]
            )
        )

        schedule = _lambda.Function(
            self, 'schedule',
            function_name = 'schedule',
            runtime = _lambda.Runtime.PYTHON_3_13,
            architecture = _lambda.Architecture.ARM_64,
            code = _lambda.Code.from_asset('schedule'),
            handler = 'schedule.handler',
            environment = dict(
                DYNAMODB_TABLE = 'arn:aws:dynamodb:' + region + ':' + lunker.string_value + ':table/permutation',
                STATE_DYNAMODB_TABLE = 'state',
                RUN_DYNAMODB_TABLE = 'run',
                STATE_DYNAMODB_REGION = 'us-east-2',
                TTL_DAYS = '365'
            ),
            timeout = Duration.seconds(900),
            memory_size = 1024,
            ephemeral_storage_size = Size.gibibytes(1),
            role = role
        )

        _logs.LogGroup(
            self, 'logs',
            log_group_name = '/aws/lambda/' + schedule.function_name,
            retention = _logs.RetentionDays.ONE_WEEK,
            removal_policy = RemovalPolicy.DESTROY
        )

        event = _events.Rule(
            self, 'event',
            schedule = _events.Schedule.cron(
                minute = '*/5',
                hour = '*',
                month = '*',
                week_day = '*',
                year = '*'
            )
        )

        event.add_target(
            _targets.LambdaFunction(schedule)
        )
