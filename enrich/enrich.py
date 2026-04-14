import json
from urllib.parse import unquote_plus


def handler(event, context):
    for record in event.get('Records', []):
        body = record.get('body', '{}')

        try:
            s3_event = json.loads(body)
        except json.JSONDecodeError:
            print(f'Invalid SQS message body: {body}')
            continue

        for s3_record in s3_event.get('Records', []):
            bucket = s3_record.get('s3', {}).get('bucket', {}).get('name', '')
            key = s3_record.get('s3', {}).get('object', {}).get('key', '')
            key = unquote_plus(key)

            if bucket and key:
                print(f's3://{bucket}/{key}')
            elif bucket:
                print(f's3://{bucket}')
            else:
                print('S3 record missing bucket/key')

    return {'statusCode': 200}
