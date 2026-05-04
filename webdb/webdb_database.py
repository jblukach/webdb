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

        primary_region = 'us-east-2'

        lunker = _ssm.StringParameter.from_string_parameter_attributes(
            self, 'lunker',
            parameter_name = '/account/lunker'
        )

        def replica_resource_policy(region: str) -> _iam.PolicyDocument:
            return _iam.PolicyDocument(
                statements = [
                    _iam.PolicyStatement(
                        sid = 'AllowLunkerAccountQueryAccess',
                        effect = _iam.Effect.ALLOW,
                        principals = [
                            _iam.ArnPrincipal(
                                f'arn:aws:iam::{lunker.string_value}:root'
                            )
                        ],
                        actions = [
                            'dynamodb:DescribeTable',
                            'dynamodb:Query',
                        ],
                        resources = [
                            self.format_arn(
                                service = 'dynamodb',
                                region = region,
                                resource = 'table',
                                resource_name = 'possibilities',
                            ),
                            self.format_arn(
                                service = 'dynamodb',
                                region = region,
                                resource = 'table',
                                resource_name = 'possibilities/index/*',
                            ),
                        ],
                    )
                ]
            )

        _dynamodb.TableV2(
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
            resource_policy = replica_resource_policy(primary_region),
            time_to_live_attribute = 'ttl',
            replicas = [
                _dynamodb.ReplicaTableProps(
                    region = 'us-east-1',
                    resource_policy = replica_resource_policy('us-east-1'),
                ),
                _dynamodb.ReplicaTableProps(
                    region = 'us-west-2',
                    resource_policy = replica_resource_policy('us-west-2'),
                ),
            ],
        )

        _dynamodb.TableV2(
            self, 'state',
            table_name = 'state',
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
        )
