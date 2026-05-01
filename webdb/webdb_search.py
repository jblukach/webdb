from aws_cdk import (
    Duration,
    RemovalPolicy,
    Size,
    Stack,
    aws_iam as _iam,
    aws_lambda as _lambda,
    aws_logs as _logs,
    aws_ssm as _ssm
)

from constructs import Construct


class WebdbSearch(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        region = Stack.of(self).region

        lunker = _ssm.StringParameter.from_string_parameter_attributes(
            self, 'lunker',
            parameter_name = '/account/lunker'
        )

        webmonitor = _ssm.StringParameter.from_string_parameter_attributes(
            self, 'webmonitor',
            parameter_name = '/account/webmonitor'
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
                    'athena:StartQueryExecution',
                    'athena:GetQueryExecution',
                    'athena:GetQueryResults',
                    'athena:StopQueryExecution',
                    'glue:GetDatabase',
                    'glue:GetDatabases',
                    'glue:GetTable',
                    'glue:GetTables',
                    'glue:GetPartition',
                    'glue:GetPartitions',
                    'glue:BatchGetPartition',
                    'dynamodb:GetItem',
                    'dynamodb:DescribeTable',
                    's3:GetBucketLocation',
                    's3:GetObject',
                    's3:ListBucket',
                    's3:ListBucketMultipartUploads',
                    's3:ListMultipartUploadParts',
                    's3:AbortMultipartUpload',
                    's3:PutObject'
                ],
                resources = ['*']
            )
        )

        search = _lambda.Function(
            self, 'search',
            function_name = 'search',
            runtime = _lambda.Runtime.PYTHON_3_13,
            architecture = _lambda.Architecture.ARM_64,
            code = _lambda.Code.from_asset('search'),
            handler = 'search.handler',
            environment = dict(
                DYNAMODB_TABLE = 'arn:aws:dynamodb:'+region+':'+lunker.string_value+':table/permutation',
                ATHENA_DATABASE = 'webdb',
                ATHENA_TABLE = 'domains',
                ATHENA_WORKGROUP = 'webdb',
                OUTPUT_BUCKET = f'webdb-{region}-output',
                TEMP_BUCKET = f'webdb-{region}-temporary'
            ),
            timeout = Duration.seconds(900),
            memory_size = 512,
            role = role
        )

        search.add_permission(
            'allow-webmonitor-account-invoke',
            principal = _iam.AccountPrincipal(webmonitor.string_value),
            action = 'lambda:InvokeFunction'
        )

        _logs.LogGroup(
            self, 'logs',
            log_group_name = '/aws/lambda/'+search.function_name,
            retention = _logs.RetentionDays.ONE_WEEK,
            removal_policy = RemovalPolicy.DESTROY
        )
