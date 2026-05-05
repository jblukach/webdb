import boto3
import datetime
import json
import os

from botocore.exceptions import ClientError


def _build_filename(date_value):
    return date_value.strftime('%Y-%m-%d') + '-detailed-update.csv'


def _resolve_source_key(s3_client, source_bucket):
    today = datetime.datetime.utcnow().date()
    candidates = [
        _build_filename(today),
        _build_filename(today - datetime.timedelta(days = 1)),
    ]

    for key in candidates:
        try:
            s3_client.head_object(Bucket = source_bucket, Key = key)
            return key
        except ClientError as exc:
            code = exc.response.get('Error', {}).get('Code', '')
            if code in ('404', 'NotFound', 'NoSuchKey'):
                continue
            raise

    raise FileNotFoundError('No transfer source file found for today or yesterday')

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

    source_bucket = os.environ['GET_BUCKET']
    destination_bucket = os.environ['PUT_BUCKET']

    s3_client = boto3.client('s3')
    fname = _resolve_source_key(s3_client, source_bucket)
    print(json.dumps({'message': 'transfer_source_selected', 'key': fname}, separators = (',', ':')))

    s3_client.download_file(
        source_bucket,
        fname,
        f'/tmp/{fname}'
    )

    s3_resource = boto3.resource('s3')

    s3_resource.meta.client.upload_file(
        f'/tmp/{fname}',
        destination_bucket,
        fname,
        ExtraArgs = {
            'ContentType': 'text/csv'
        }
    )

    return {
        'statusCode': 200,
        'body': json.dumps('Copied!')
    }