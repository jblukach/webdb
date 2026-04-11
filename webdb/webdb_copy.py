from aws_cdk import (
    RemovalPolicy,
    Stack,
    aws_s3 as _s3
)

from constructs import Construct

class WebdbCopy(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        region = Stack.of(self).region
