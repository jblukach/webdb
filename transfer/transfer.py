import boto3
import datetime
import json
import os

def handler(event, context):

    year = datetime.datetime.now().strftime('%Y')
    month = datetime.datetime.now().strftime('%m')
    day = datetime.datetime.now().strftime('%d')

    fname = f'{year}-{month}-{day}-detailed-update.csv'

    s3_client = boto3.client('s3')

    s3_client.download_file(
        os.environ['GET_BUCKET'],
        fname,
        f'/tmp/{fname}'
    )

    s3_resource = boto3.resource('s3')

    s3_resource.meta.client.upload_file(
        f'/tmp/{fname}',
        os.environ['PUT_BUCKET'],
        fname,
        ExtraArgs = {
            'ContentType': "text/csv"
        }
    )

    os.system('ls -lh /tmp')

    return {
        'statusCode': 200,
        'body': json.dumps('Copied!')
    }