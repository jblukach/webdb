#!/usr/bin/env python3
import os

import aws_cdk as cdk

from webdb.webdb_enrich import WebdbEnrich
from webdb.webdb_github import WebdbGithub
from webdb.webdb_insert import WebdbInsert
from webdb.webdb_search import WebdbSearch
from webdb.webdb_storage import WebdbStorage
from webdb.webdb_transfer import WebdbTransfer

app = cdk.App()

WebdbEnrich(
    app, 'WebdbEnrich',
    env = cdk.Environment(
        account = os.getenv('CDK_DEFAULT_ACCOUNT'),
        region = 'us-east-2'
    ),
    synthesizer = cdk.DefaultStackSynthesizer(
        qualifier = 'lukach'
    )
)

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

WebdbStorage(
    app, 'WebdbStorage',
    env = cdk.Environment(
        account = os.getenv('CDK_DEFAULT_ACCOUNT'),
        region = 'us-east-2'
    ),
    synthesizer = cdk.DefaultStackSynthesizer(
        qualifier = 'lukach'
    )
)

WebdbTransfer(
    app, 'WebdbTransfer',
    env = cdk.Environment(
        account = os.getenv('CDK_DEFAULT_ACCOUNT'),
        region = 'us-east-2'
    ),
    synthesizer = cdk.DefaultStackSynthesizer(
        qualifier = 'lukach'
    )
)

WebdbInsert(
    app, 'WebdbInsert',
    env = cdk.Environment(
        account = os.getenv('CDK_DEFAULT_ACCOUNT'),
        region = 'us-east-2'
    ),
    synthesizer = cdk.DefaultStackSynthesizer(
        qualifier = 'lukach'
    )
)

WebdbSearch(
    app, 'WebdbSearch',
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