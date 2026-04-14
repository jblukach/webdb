from aws_cdk import (
    RemovalPolicy,
    Stack,
    aws_s3 as _s3
)

from constructs import Construct

class WebdbStorage(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        region = Stack.of(self).region

        for namespace in ['enrich', 'insert', 'archive']:

            _s3.Bucket(
                self, namespace,
                bucket_name = f"webdb-{region}-{namespace}",
                encryption = _s3.BucketEncryption.S3_MANAGED,
                block_public_access = _s3.BlockPublicAccess.BLOCK_ALL,
                removal_policy = RemovalPolicy.RETAIN,
                auto_delete_objects = False,
                enforce_ssl = True,
                versioned = False
            )
