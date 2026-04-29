import gzip
import json
import os
import shutil
import time
import urllib.parse
import uuid

import boto3
import pandas as pd
from botocore.exceptions import BotoCoreError, ClientError


REGION = os.environ.get('AWS_REGION', 'us-east-2')
NAMESPACE = 'webdb'
TABLE_NAME = 'domains'
ATHENA_TARGET_CATALOG = os.environ.get('ATHENA_TARGET_CATALOG', 's3tablescatalog/webdb')
ATHENA_TARGET_DATABASE = os.environ.get('ATHENA_TARGET_DATABASE', 'webdb')
ATHENA_STAGING_DATABASE = os.environ.get('ATHENA_STAGING_DATABASE', 'webdb_staging')
ATHENA_WORKGROUP = os.environ.get('ATHENA_WORKGROUP', 'primary')
ATHENA_RESULTS_PREFIX = os.environ.get('ATHENA_RESULTS_PREFIX', '_athena_results')
ATHENA_STAGING_PREFIX = os.environ.get('ATHENA_STAGING_PREFIX', '_athena_staging')
ATHENA_TIMEOUT_SECONDS = int(os.environ.get('ATHENA_TIMEOUT_SECONDS', '540'))
ATHENA_POLL_SECONDS = int(os.environ.get('ATHENA_POLL_SECONDS', '2'))

TARGET_COLUMNS = [
    ('dns', 'string'),
    ('ns', 'string'),
    ('ip', 'string'),
    ('co', 'string'),
    ('web', 'string'),
    ('eml', 'string'),
    ('hold', 'string'),
    ('tel', 'bigint'),
    ('rank', 'bigint'),
    ('ts', 'string'),
    ('id', 'string'),
    ('sld', 'string'),
    ('tld', 'string'),
    ('asn', 'bigint'),
]


def _build_dataframe(path):
    """Read JSONL and convert to DataFrame with appropriate types."""
    df = pd.read_json(path, lines=True)
    if df.empty:
        return None

    # Convert '-' strings to numeric nulls where appropriate
    for col in df.columns:
        if df[col].dtype == object and df[col].eq('-').any():
            converted = pd.to_numeric(df[col], errors='coerce')
            if converted.notna().any() or df[col].eq('-').all():
                df[col] = converted.astype('Int64')

    # Serialize list/dict columns to JSON strings
    for col in df.columns:
        if df[col].dtype == object:
            first_valid = df[col].dropna().iloc[0] if not df[col].dropna().empty else None
            if isinstance(first_valid, (list, dict)):
                df[col] = df[col].apply(
                    lambda v: json.dumps(v, separators=(',', ':')) if v is not None else None
                )

    return df


def _quote_ident(value):
    return '"' + value.replace('"', '""') + '"'


def _qualified_name(catalog, database, table):
    return f'{_quote_ident(catalog)}.{_quote_ident(database)}.{_quote_ident(table)}'


def _run_athena_query(athena_client, query, output_location, database=None):
    execution_context_database = database or ATHENA_STAGING_DATABASE
    params = {
        'QueryString': query,
        'WorkGroup': ATHENA_WORKGROUP,
        'ResultConfiguration': {'OutputLocation': output_location},
        'QueryExecutionContext': {
            'Catalog': 'AwsDataCatalog',
            'Database': execution_context_database,
        },
    }
    response = athena_client.start_query_execution(**params)
    query_id = response['QueryExecutionId']
    started = time.monotonic()

    while True:
        execution = athena_client.get_query_execution(QueryExecutionId=query_id)
        state = execution['QueryExecution']['Status']['State']
        if state == 'SUCCEEDED':
            return query_id
        if state in ('FAILED', 'CANCELLED'):
            reason = execution['QueryExecution']['Status'].get('StateChangeReason', 'No reason provided')
            raise RuntimeError(f'Athena query {query_id} {state}: {reason}')
        if time.monotonic() - started > ATHENA_TIMEOUT_SECONDS:
            athena_client.stop_query_execution(QueryExecutionId=query_id)
            raise TimeoutError(f'Athena query {query_id} timed out after {ATHENA_TIMEOUT_SECONDS}s')
        time.sleep(ATHENA_POLL_SECONDS)


def _create_staging_database(athena_client, output_location):
    sql = f'CREATE DATABASE IF NOT EXISTS {ATHENA_STAGING_DATABASE}'
    _run_athena_query(athena_client, sql, output_location)


def _create_staging_table(athena_client, output_location, temp_table, staging_location):
    columns = ', '.join(f'{_quote_ident(name)} {col_type}' for name, col_type in TARGET_COLUMNS)
    ddl = f'''
CREATE EXTERNAL TABLE {ATHENA_STAGING_DATABASE}.{temp_table} (
    {columns}
)
STORED AS PARQUET
LOCATION '{staging_location}'
'''.strip()
    _run_athena_query(athena_client, ddl, output_location)


def _insert_from_staging(athena_client, output_location, temp_table):
    target_name = _qualified_name(ATHENA_TARGET_CATALOG, ATHENA_TARGET_DATABASE, TABLE_NAME)
    staging_name = _qualified_name('AwsDataCatalog', ATHENA_STAGING_DATABASE, temp_table)
    col_list = ', '.join(_quote_ident(name) for name, _ in TARGET_COLUMNS)
    insert_sql = f'''
INSERT INTO {target_name} ({col_list})
SELECT {col_list}
FROM {staging_name}
'''.strip()
    _run_athena_query(athena_client, insert_sql, output_location, database=ATHENA_STAGING_DATABASE)


def _drop_staging_table(athena_client, output_location, temp_table):
    _run_athena_query(
        athena_client,
        f'DROP TABLE IF EXISTS {ATHENA_STAGING_DATABASE}.{temp_table}',
        output_location,
    )


def _upload_parquet_to_staging(s3_client, parquet_path, staging_bucket, staging_key):
    s3_client.upload_file(parquet_path, staging_bucket, staging_key)


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


def _insert_file(bucket, key, s3_client, athena_client, request_id):
    """Process one JSONL file: convert to Parquet, stage in S3, and commit via Athena."""
    local_path = f'/tmp/{os.path.basename(key)}'
    parquet_path = f'/tmp/{uuid.uuid4().hex}.parquet'
    temporary_bucket = f'webdb-{REGION}-temporary'
    output_location = f's3://{temporary_bucket}/{ATHENA_RESULTS_PREFIX}/'
    temp_table = f'tmp_{uuid.uuid4().hex[:12]}'
    staging_prefix = f'{ATHENA_STAGING_PREFIX}/{temp_table}'
    staging_key = f'{staging_prefix}/data.parquet'
    staging_location = f's3://{temporary_bucket}/{staging_prefix}/'

    try:
        try:
            s3_client.head_object(Bucket=bucket, Key=key)
        except ClientError as exc:
            if exc.response.get('Error', {}).get('Code') in ('404', 'NoSuchKey', 'NotFound'):
                print(f'Skipping {key}: object no longer exists in {bucket}')
                return
            raise

        s3_client.download_file(bucket, key, local_path)

        dataframe = _build_dataframe(local_path)
        if dataframe is None:
            print(f'No records in {key}')
            return

        dataframe.to_parquet(parquet_path, index=False)

        _upload_parquet_to_staging(s3_client, parquet_path, temporary_bucket, staging_key)
        _create_staging_database(athena_client, output_location)
        _create_staging_table(athena_client, output_location, temp_table, staging_location)
        _insert_from_staging(athena_client, output_location, temp_table)

        _archive_and_delete(s3_client, bucket, key, local_path)

        print(f'Committed {len(dataframe)} staged records from {key} into {NAMESPACE}.{TABLE_NAME}')
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
        try:
            _drop_staging_table(athena_client, output_location, temp_table)
        except (RuntimeError, TimeoutError, BotoCoreError, ClientError) as exc:
            print(f'Failed to drop staging table {temp_table}: {exc}')
        try:
            s3_client.delete_object(Bucket=temporary_bucket, Key=staging_key)
        except (BotoCoreError, ClientError) as exc:
            print(f'Failed to delete staged parquet s3://{temporary_bucket}/{staging_key}: {exc}')
        if os.path.exists(local_path):
            os.remove(local_path)
        if os.path.exists(parquet_path):
            os.remove(parquet_path)


def handler(event, context):
    s3_client = boto3.client('s3')
    athena_client = boto3.client('athena')
    request_id = getattr(context, 'aws_request_id', 'unknown')

    print(
        json.dumps(
            {
                'message': 'insert_handler_start',
                'athena_target_catalog': ATHENA_TARGET_CATALOG,
                'athena_target_database': ATHENA_TARGET_DATABASE,
                'namespace': NAMESPACE,
                'table': TABLE_NAME,
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
                _insert_file(bucket, key, s3_client, athena_client, request_id)

    return {'statusCode': 200}
