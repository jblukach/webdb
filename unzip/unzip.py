import gzip
import json
import os
from urllib.parse import unquote_plus

import boto3


INSERT_BUCKET = os.environ['INSERT_BUCKET']


def _insert_key_from_source(source_key):
    if source_key.endswith('.gz'):
        return source_key[:-3]
    return source_key + '.out'


def _process_object(s3_client, source_bucket, source_key):
    source_obj = s3_client.get_object(Bucket = source_bucket, Key = source_key)
    zipped_bytes = source_obj['Body'].read()

    try:
        unzipped_bytes = gzip.decompress(zipped_bytes)
    except OSError as exc:
        raise ValueError(f'Failed to gunzip s3://{source_bucket}/{source_key}: {exc}') from exc

    destination_key = _insert_key_from_source(source_key)

    s3_client.put_object(
        Bucket = INSERT_BUCKET,
        Key = destination_key,
        Body = unzipped_bytes,
        ContentType = 'application/x-ndjson'
    )

    print(f'Uploaded s3://{INSERT_BUCKET}/{destination_key}')

    s3_client.delete_object(Bucket = source_bucket, Key = source_key)
    print(f'Deleted s3://{source_bucket}/{source_key}')


def handler(event, _context):
    s3_client = boto3.client('s3')

    for sqs_record in event.get('Records', []):
        body = sqs_record.get('body', '{}')

        try:
            s3_event = json.loads(body)
        except json.JSONDecodeError:
            print(f'Invalid SQS message body: {body}')
            continue

        for s3_record in s3_event.get('Records', []):
            source_bucket = s3_record.get('s3', {}).get('bucket', {}).get('name', '')
            source_key = s3_record.get('s3', {}).get('object', {}).get('key', '')
            source_key = unquote_plus(source_key)

            if not source_bucket or not source_key:
                print('S3 record missing bucket/key')
                continue

            if not source_key.endswith('.gz'):
                print(f'Skipping non-gzip key {source_key}')
                continue

            _process_object(s3_client, source_bucket, source_key)

    return {'statusCode': 200}
