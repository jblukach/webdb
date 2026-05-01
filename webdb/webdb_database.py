from aws_cdk import (
    RemovalPolicy,
    Stack,
    aws_dynamodb as _dynamodb,
    aws_iam as _iam,
    aws_ssm as _ssm,
)

from constructs import Construct


class WebdbDatabase(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        lunker = _ssm.StringParameter.from_string_parameter_attributes(
            self, 'lunker',
            parameter_name = '/account/lunker'
        )

        table = _dynamodb.TableV2(
            self, 'possibilities',
            table_name = 'possibilities',
            partition_key = {
                'name': 'pk',
                'type': _dynamodb.AttributeType.STRING
            },
            sort_key = {
                'name': 'sk',
                'type': _dynamodb.AttributeType.STRING
            },
            billing = _dynamodb.Billing.on_demand(),
            removal_policy = RemovalPolicy.DESTROY,
            point_in_time_recovery_specification = _dynamodb.PointInTimeRecoverySpecification(
                point_in_time_recovery_enabled = True
            ),
            deletion_protection = True,
            time_to_live_attribute = 'ttl',
            replicas = [
                _dynamodb.ReplicaTableProps(region = 'us-east-1'),
                _dynamodb.ReplicaTableProps(region = 'us-west-2'),
            ],
        )

        table.add_to_resource_policy(
            _iam.PolicyStatement(
                sid = 'AllowLunkerAccountAccess',
                effect = _iam.Effect.ALLOW,
                principals = [
                    _iam.AccountPrincipal(lunker.string_value)
                ],
                actions = [
                    'dynamodb:DescribeTable',
                    'dynamodb:GetItem',
                    'dynamodb:Query'
                ],
                resources = [
                    self.format_arn(
                        service = 'dynamodb',
                        resource = 'table',
                        resource_name = 'possibilities'
                    )
                ]
            )
        )
