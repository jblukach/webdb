import datetime
import gzip
import json
import os
import re
import time
from urllib.parse import unquote_plus

import boto3
from botocore.exceptions import ClientError


ARCHIVE_BUCKET = os.environ['ARCHIVE_BUCKET']
GLUE_JOB_NAME = os.environ['GLUE_JOB_NAME']
GLUE_TIMEOUT_SECONDS = int(os.environ.get('GLUE_TIMEOUT_SECONDS', '840'))
GLUE_POLL_SECONDS = int(os.environ.get('GLUE_POLL_SECONDS', '10'))
GLUE_START_TIMEOUT_SECONDS = int(os.environ.get('GLUE_START_TIMEOUT_SECONDS', '30'))
GLUE_START_BACKOFF_SECONDS = int(os.environ.get('GLUE_START_BACKOFF_SECONDS', '15'))
PROCESSED_OBJECTS_TABLE = os.environ.get('PROCESSED_OBJECTS_TABLE', '')


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


def _wait_for_glue(glue_client, job_run_id):
    started_at = time.time()

    while True:
        response = glue_client.get_job_run(
            JobName = GLUE_JOB_NAME,
            RunId = job_run_id,
            PredecessorsIncluded = False
        )
        state = response.get('JobRun', {}).get('JobRunState', 'UNKNOWN')

        if state == 'SUCCEEDED':
            return

        if state in ('FAILED', 'STOPPED', 'TIMEOUT', 'ERROR', 'EXPIRED'):
            raise RuntimeError('Glue job failed in state ' + state)

        elapsed = time.time() - started_at
        if elapsed >= GLUE_TIMEOUT_SECONDS:
            try:
                glue_client.batch_stop_job_run(
                    JobName = GLUE_JOB_NAME,
                    JobRunIds = [job_run_id]
                )
            except Exception as exc:  # pylint: disable=broad-except
                print('Failed to stop timed out Glue job ' + job_run_id + ': ' + str(exc))

            raise TimeoutError('Glue job timed out after ' + str(GLUE_TIMEOUT_SECONDS) + ' seconds')

        time.sleep(GLUE_POLL_SECONDS)


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
        try:
            return glue_client.start_job_run(
                JobName = GLUE_JOB_NAME,
                Arguments = arguments
            )
        except Exception as exc:  # pylint: disable=broad-except
            # Retry only when Glue reports job concurrency saturation.
            is_concurrent_runs_error = isinstance(exc, glue_client.exceptions.ConcurrentRunsExceededException)
            if not is_concurrent_runs_error and isinstance(exc, ClientError):
                is_concurrent_runs_error = (
                    exc.response.get('Error', {}).get('Code') == 'ConcurrentRunsExceededException'
                )

            if not is_concurrent_runs_error:
                raise

            remaining_seconds = int(deadline - time.time())
            if remaining_seconds <= 0:
                raise TimeoutError(
                    'Timed out waiting for Glue concurrency slot after '
                    + str(GLUE_START_TIMEOUT_SECONDS)
                    + ' seconds'
                ) from exc

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


def _archive_and_delete(s3_client, source_bucket, source_key, source_bytes, partition_dt):
    year = partition_dt.strftime('%Y')
    month = partition_dt.strftime('%m')
    day = partition_dt.strftime('%d')
    source_filename = os.path.basename(source_key)
    archive_key = f'year={year}/month={month}/day={day}/{source_filename}.gz'

    archive_body = gzip.compress(source_bytes)
    s3_client.put_object(
        Bucket = ARCHIVE_BUCKET,
        Key = archive_key,
        Body = archive_body,
        ContentType = 'application/x-ndjson',
        ContentEncoding = 'gzip'
    )
    print(f'Archived s3://{ARCHIVE_BUCKET}/{archive_key}')

    s3_client.delete_object(Bucket = source_bucket, Key = source_key)
    print(f'Deleted s3://{source_bucket}/{source_key}')


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

    _wait_for_glue(glue_client, job_run_id)
    _archive_and_delete(s3_client, source_bucket, source_key, source_bytes, partition_dt)
    _mark_as_processed(dynamodb_client, source_bucket, source_key)


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