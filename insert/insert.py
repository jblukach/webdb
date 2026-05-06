import datetime
import json
import os
import re
import time
from urllib.parse import unquote_plus

import boto3
from botocore.exceptions import ClientError


GLUE_JOB_NAME = os.environ['GLUE_JOB_NAME']
GLUE_START_TIMEOUT_SECONDS = int(os.environ.get('GLUE_START_TIMEOUT_SECONDS', '30'))
GLUE_START_BACKOFF_SECONDS = int(os.environ.get('GLUE_START_BACKOFF_SECONDS', '15'))
PROCESSED_OBJECTS_TABLE = os.environ.get('PROCESSED_OBJECTS_TABLE', '')
EXECUTION_TABLE = os.environ['EXECUTION_TABLE']


def _make_processed_pk(source_bucket, source_key):
    return f'{source_bucket}#{source_key}'


def _is_already_processed(dynamodb_client, source_bucket, source_key):
    if not PROCESSED_OBJECTS_TABLE:
        return False

    pk = _make_processed_pk(source_bucket, source_key)

    try:
        response = dynamodb_client.get_item(
            TableName = PROCESSED_OBJECTS_TABLE,
            Key = {'pk': {'S': pk}}
        )
        return 'Item' in response
    except Exception as exc:  # pylint: disable=broad-except
        print(f'Failed to check processed status: {str(exc)}')
        return False


def _mark_as_processed(dynamodb_client, source_bucket, source_key):
    if not PROCESSED_OBJECTS_TABLE:
        return

    pk = _make_processed_pk(source_bucket, source_key)
    ttl_epoch = int(time.time()) + (86400 * 30)  # 30 days

    try:
        dynamodb_client.put_item(
            TableName = PROCESSED_OBJECTS_TABLE,
            Item = {
                'pk': {'S': pk},
                'processed_at': {'N': str(int(time.time()))},
                'ttl': {'N': str(ttl_epoch)}
            }
        )
    except Exception as exc:  # pylint: disable=broad-except
        print(f'Failed to mark as processed: {str(exc)}')


def _partition_date(source_key, source_bytes):
    source_filename = os.path.basename(source_key)
    filename_match = re.match(r'^(\d{4})[-_]?(\d{2})[-_]?(\d{2})', source_filename)

    if filename_match:
        year, month, day = filename_match.groups()
        try:
            return datetime.datetime(int(year), int(month), int(day))
        except ValueError:
            pass

    for raw_line in source_bytes.decode('utf-8').splitlines():
        if not raw_line.strip():
            continue

        try:
            first_record = json.loads(raw_line)
        except json.JSONDecodeError:
            break

        ts_value = str(first_record.get('ts', '')).strip()
        if len(ts_value) < 10:
            break

        try:
            return datetime.datetime.strptime(ts_value[:10], '%Y-%m-%d')
        except ValueError:
            break

    return datetime.datetime.utcnow()


def _start_glue_job_run(glue_client, partition_dt, source_bucket, source_key):
    deadline = time.time() + GLUE_START_TIMEOUT_SECONDS
    arguments = {
        '--source_bucket': source_bucket,
        '--source_key': source_key,
        '--database': 'webdb',
        '--table': 'domains',
        '--year': str(partition_dt.year),
        '--month': str(partition_dt.month),
        '--day': str(partition_dt.day),
    }

    while True:
        retry_exception = None

        try:
            return glue_client.start_job_run(
                JobName = GLUE_JOB_NAME,
                Arguments = arguments
            )
        except glue_client.exceptions.ConcurrentRunsExceededException as exc:
            retry_exception = exc
        except ClientError as exc:
            error_code = exc.response.get('Error', {}).get('Code')
            if error_code != 'ConcurrentRunsExceededException':
                raise
            retry_exception = exc

        remaining_seconds = int(deadline - time.time())
        if remaining_seconds <= 0:
            raise TimeoutError(
                'Timed out waiting for Glue concurrency slot after '
                + str(GLUE_START_TIMEOUT_SECONDS)
                + ' seconds'
            ) from retry_exception

        backoff_seconds = min(GLUE_START_BACKOFF_SECONDS, remaining_seconds)
        print(
            'Glue concurrent run limit reached for '
            + GLUE_JOB_NAME
            + '; retrying in '
            + str(backoff_seconds)
            + ' seconds for s3://'
            + source_bucket
            + '/'
            + source_key
        )
        time.sleep(backoff_seconds)


def _put_execution_record(dynamodb_client, job_run_id, source_bucket, source_key, partition_dt):
    now = datetime.datetime.now(datetime.timezone.utc)
    ttl_epoch = int(now.timestamp()) + (86400 * 7)

    dynamodb_client.put_item(
        TableName = EXECUTION_TABLE,
        Item = {
            'pk': {'S': 'EXEC#' + job_run_id},
            'sk': {'S': 'GLUE'},
            'execution_type': {'S': 'GLUE'},
            'status': {'S': 'PENDING'},
            'created_at': {'S': now.isoformat()},
            'ttl': {'N': str(ttl_epoch)},
            'job_name': {'S': GLUE_JOB_NAME},
            'source_bucket': {'S': source_bucket},
            'source_key': {'S': source_key},
            'partition_year': {'N': str(partition_dt.year)},
            'partition_month': {'N': str(partition_dt.month)},
            'partition_day': {'N': str(partition_dt.day)},
            'processed_objects_table': {'S': PROCESSED_OBJECTS_TABLE},
        }
    )

def _convert_object(s3_client, glue_client, dynamodb_client, source_bucket, source_key):
    if _is_already_processed(dynamodb_client, source_bucket, source_key):
        print(f'Skipping already-processed file s3://{source_bucket}/{source_key}')
        return

    source_obj = s3_client.get_object(Bucket = source_bucket, Key = source_key)
    source_bytes = source_obj['Body'].read()

    if not source_bytes.strip():
        print(f'Skipping empty file s3://{source_bucket}/{source_key}')
        _mark_as_processed(dynamodb_client, source_bucket, source_key)
        return

    partition_dt = _partition_date(source_key, source_bytes)

    job_response = _start_glue_job_run(glue_client, partition_dt, source_bucket, source_key)
    job_run_id = job_response['JobRunId']
    print('Started Glue job run ' + job_run_id + ' for s3://' + source_bucket + '/' + source_key)

    _put_execution_record(
        dynamodb_client=dynamodb_client,
        job_run_id=job_run_id,
        source_bucket=source_bucket,
        source_key=source_key,
        partition_dt=partition_dt,
    )

    print('Tracked Glue execution for async completion handling: ' + job_run_id)


def handler(event, _context):
    s3_client = boto3.client('s3')
    glue_client = boto3.client('glue')
    dynamodb_client = boto3.client('dynamodb')

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

            if not source_key.endswith('.jsonl'):
                print(f'Skipping non-jsonl key {source_key}')
                continue

            _convert_object(s3_client, glue_client, dynamodb_client, source_bucket, source_key)

    return {'statusCode': 200}