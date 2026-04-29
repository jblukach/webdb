import boto3
import datetime
import json
import os

def handler(event, context):
    print(
        json.dumps(
            {
                'message': 'transfer_handler_start',
                'request_id': getattr(context, 'aws_request_id', 'unknown'),
                'record_count': len(event.get('Records', [])) if isinstance(event, dict) else 0,
            },
            separators=(',', ':'),
        )
    )

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
            'ContentType': 'text/csv'
        }
    )

    return {
        'statusCode': 200,
        'body': json.dumps('Copied!')
    }