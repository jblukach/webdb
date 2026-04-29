import gzip
import json
import os
import shutil
import time
import urllib.parse

import boto3
from botocore.exceptions import ClientError


REGION = os.environ.get('AWS_REGION', 'us-east-2')
GLUE_JOB_NAME = os.environ['GLUE_JOB_NAME']
GLUE_TIMEOUT_SECONDS = int(os.environ.get('GLUE_TIMEOUT_SECONDS', '3600'))
GLUE_POLL_SECONDS = int(os.environ.get('GLUE_POLL_SECONDS', '10'))
TARGET_CATALOG = os.environ.get('TARGET_CATALOG', 's3tablescatalog')
TARGET_DATABASE = os.environ.get('ATHENA_TARGET_DATABASE', 'webdb')
TARGET_TABLE = os.environ.get('TABLE_NAME', 'domains')


def _archive_and_delete(s3_client, bucket, key, local_path):
    """Gzip, archive, and delete the source JSONL file."""
    archive_bucket = f'webdb-{REGION}-archive'
    basename = os.path.basename(key)
    stem = os.path.splitext(basename)[0]

    date_prefix = ''
    if (
        len(stem) >= 10
        and stem[4] == '-'
        and stem[7] == '-'
        and stem[:4].isdigit()
        and stem[5:7].isdigit()
        and stem[8:10].isdigit()
    ):
        year, month, day = stem[:4], stem[5:7], stem[8:10]
        date_prefix = f'{year}/{month}/{day}/'

    gz_local = local_path + '.gz'
    try:
        with open(local_path, 'rb') as f_in, gzip.open(gz_local, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

        archive_key = f'{date_prefix}{basename}.gz'
        s3_client.upload_file(gz_local, archive_bucket, archive_key)
        print(f'Archived s3://{bucket}/{key} to s3://{archive_bucket}/{archive_key}')

        s3_client.delete_object(Bucket=bucket, Key=key)
        print(f'Deleted s3://{bucket}/{key}')
    finally:
        if os.path.exists(gz_local):
            os.remove(gz_local)


def _wait_for_job(glue_client, job_name, run_id):
    started = time.monotonic()
    while True:
        response = glue_client.get_job_run(JobName=job_name, RunId=run_id, PredecessorsIncluded=False)
        run = response['JobRun']
        state = run['JobRunState']

        if state == 'SUCCEEDED':
            return run

        if state in ('FAILED', 'STOPPED', 'TIMEOUT', 'ERROR', 'EXPIRED'):
            detail = run.get('ErrorMessage', 'No error message')
            raise RuntimeError(f'Glue job {job_name}/{run_id} {state}: {detail}')

        if time.monotonic() - started > GLUE_TIMEOUT_SECONDS:
            glue_client.batch_stop_job_run(JobName=job_name, JobRunIds=[run_id])
            raise TimeoutError(
                f'Glue job {job_name}/{run_id} timed out after {GLUE_TIMEOUT_SECONDS}s'
            )

        time.sleep(GLUE_POLL_SECONDS)


def _run_glue_insert(glue_client, bucket, key):
    args = {
        '--SOURCE_BUCKET': bucket,
        '--SOURCE_KEY': key,
        '--TARGET_CATALOG': TARGET_CATALOG,
        '--TARGET_DATABASE': TARGET_DATABASE,
        '--TARGET_TABLE': TARGET_TABLE,
    }
    response = glue_client.start_job_run(JobName=GLUE_JOB_NAME, Arguments=args)
    run_id = response['JobRunId']
    print(f'Started Glue job {GLUE_JOB_NAME} run {run_id} for s3://{bucket}/{key}')
    _wait_for_job(glue_client, GLUE_JOB_NAME, run_id)


def _process_file(s3_client, glue_client, bucket, key, request_id):
    local_path = f'/tmp/{os.path.basename(key)}'

    try:
        try:
            s3_client.head_object(Bucket=bucket, Key=key)
        except ClientError as exc:
            if exc.response.get('Error', {}).get('Code') in ('404', 'NoSuchKey', 'NotFound'):
                print(f'Skipping {key}: object no longer exists in {bucket}')
                return
            raise

        s3_client.download_file(bucket, key, local_path)
        _run_glue_insert(glue_client, bucket, key)
        _archive_and_delete(s3_client, bucket, key, local_path)
        print(f'Committed staged records from {key} into {TARGET_DATABASE}.{TARGET_TABLE}')
    except Exception as exc:
        log = {
            'message': 'insert_file_failed',
            'request_id': request_id,
            'bucket': bucket,
            'key': key,
            'error_type': type(exc).__name__,
            'error': str(exc),
        }
        print(json.dumps(log, separators=(',', ':')))
        raise
    finally:
        if os.path.exists(local_path):
            os.remove(local_path)


def handler(event, context):
    s3_client = boto3.client('s3')
    glue_client = boto3.client('glue')
    request_id = getattr(context, 'aws_request_id', 'unknown')

    print(
        json.dumps(
            {
                'message': 'insert_handler_start',
                'glue_job_name': GLUE_JOB_NAME,
                'target_database': TARGET_DATABASE,
                'target_table': TARGET_TABLE,
                'target_catalog': TARGET_CATALOG,
                'region': REGION,
                'request_id': request_id,
            },
            separators=(',', ':'),
        )
    )

    for record in event.get('Records', []):
        body = record.get('body', '{}')

        try:
            s3_event = json.loads(body)
        except json.JSONDecodeError:
            print('Invalid JSON body in SQS record')
            continue

        for s3_record in s3_event.get('Records', []):
            if not s3_record.get('eventName', '').startswith('ObjectCreated'):
                continue

            bucket = s3_record.get('s3', {}).get('bucket', {}).get('name')
            key = s3_record.get('s3', {}).get('object', {}).get('key')

            if key:
                key = urllib.parse.unquote_plus(key)

            print(f'bucket={bucket} key={key}')

            if bucket and key:
                _process_file(s3_client, glue_client, bucket, key, request_id)

    return {'statusCode': 200}
