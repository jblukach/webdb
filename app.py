#!/usr/bin/env python3
import os

import aws_cdk as cdk

from webdb.webdb_github import WebdbGithub
from webdb.webdb_table import WebdbTable

app = cdk.App()

WebdbGithub(
    app, 'WebdbGithub',
    env = cdk.Environment(
        account = os.getenv('CDK_DEFAULT_ACCOUNT'),
        region = 'us-east-2'
    ),
    synthesizer = cdk.DefaultStackSynthesizer(
        qualifier = 'lukach'
    )
)

WebdbTable(
    app, 'WebdbTable',
    env = cdk.Environment(
        account = os.getenv('CDK_DEFAULT_ACCOUNT'),
        region = 'us-east-2'
    ),
    synthesizer = cdk.DefaultStackSynthesizer(
        qualifier = 'lukach'
    )
)

cdk.Tags.of(app).add('Alias','webdb')
cdk.Tags.of(app).add('GitHub','https://github.com/jblukach/webdb')
cdk.Tags.of(app).add('Org','lukach.io')

app.synth()