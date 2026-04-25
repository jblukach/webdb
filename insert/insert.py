import json
import os
import re
import time
import urllib.parse
import uuid

import boto3
import pandas as pd
from botocore.exceptions import BotoCoreError, ClientError


REGION = os.environ.get('AWS_REGION', 'us-east-2')
NAMESPACE = 'webdb'
TABLE_NAME = 'domains'
ATHENA_CATALOG = os.environ.get('ATHENA_TARGET_CATALOG', 's3tablescatalog/webdb')
ATHENA_STAGING_DATABASE = os.environ.get('ATHENA_STAGING_DATABASE', 'webdb_staging')
ATHENA_WORKGROUP = os.environ.get('ATHENA_WORKGROUP', 'primary')
ATHENA_RESULTS_PREFIX = os.environ.get('ATHENA_RESULTS_PREFIX', '_athena_results')
ATHENA_STAGING_PREFIX = os.environ.get('ATHENA_STAGING_PREFIX', '_athena_staging')
ATHENA_OUTPUT_LOCATION = os.environ.get('ATHENA_OUTPUT_LOCATION', '').strip()
ATHENA_POLL_SECONDS = int(os.environ.get('ATHENA_POLL_SECONDS', '2'))
ATHENA_TIMEOUT_SECONDS = int(os.environ.get('ATHENA_TIMEOUT_SECONDS', '540'))


def _resolve_target_catalog_and_database(value):
    if '/' in value:
        catalog, database = value.split('/', 1)
        catalog = catalog.strip()
        database = database.strip()
        if catalog and database:
            return catalog, database
    catalog = value.strip()
    return (catalog or 'AwsDataCatalog'), NAMESPACE


TARGET_CATALOG_NAME, TARGET_DATABASE_NAME = _resolve_target_catalog_and_database(ATHENA_CATALOG)


def _quote_ident(value):
    return '"' + value.replace('"', '""') + '"'


def _quote_literal(value):
    return "'" + value.replace("'", "''") + "'"


def _safe_temp_table_name(key):
    base = os.path.basename(key)
    normalized = re.sub(r'[^a-zA-Z0-9_]', '_', base).lower()
    normalized = re.sub(r'_+', '_', normalized).strip('_')
    if not normalized:
        normalized = 'insert'
    return f'tmp_{normalized}_{uuid.uuid4().hex[:8]}'


def _pandas_type_to_athena(dtype):
    kind = dtype.kind
    if kind in ('i', 'u'):
        return 'bigint'
    if kind == 'f':
        return 'double'
    if kind == 'b':
        return 'boolean'
    if kind == 'M':
        return 'timestamp'
    return 'string'


def _run_athena_query(athena_client, query, output_location, database=None, catalog='AwsDataCatalog'):
    params = {
        'QueryString': query,
        'WorkGroup': ATHENA_WORKGROUP,
        'ResultConfiguration': {'OutputLocation': output_location},
        'QueryExecutionContext': {
            'Catalog': catalog,
            'Database': database or ATHENA_STAGING_DATABASE,
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


def _query_single_value(athena_client, query, output_location, database=None):
    query_id = _run_athena_query(athena_client, query, output_location, database=database)
    result = athena_client.get_query_results(QueryExecutionId=query_id)
    rows = result.get('ResultSet', {}).get('Rows', [])

    # First row is the header for SELECT queries.
    if len(rows) < 2:
        return None

    data = rows[1].get('Data', [])
    if not data:
        return None

    return data[0].get('VarCharValue')


def _query_update_count(athena_client, query_id):
    execution = athena_client.get_query_execution(QueryExecutionId=query_id)
    stats = execution.get('QueryExecution', {}).get('Statistics', {})
    return int(stats.get('DataManifestLocation') and stats.get('EngineExecutionTimeInMillis') and
               execution['QueryExecution'].get('Status', {}).get('State') == 'SUCCEEDED' or 0)


def _query_rows_inserted(athena_client, query_id):
    """Return inserted row count from Athena DML result set (first data cell)."""
    try:
        result = athena_client.get_query_results(QueryExecutionId=query_id)
        rows = result.get('ResultSet', {}).get('Rows', [])
        # For DML Athena returns a single row with the affected-row count.
        for row in rows:
            for cell in row.get('Data', []):
                val = cell.get('VarCharValue', '')
                if val.isdigit():
                    return int(val)
    except (BotoCoreError, ClientError):
        pass
    return None


def _build_dataframe(path):
    df = pd.read_json(path, lines=True)
    if df.empty:
        return None
    for col in df.columns:
        if df[col].dtype == object and df[col].eq('-').any():
            converted = pd.to_numeric(df[col], errors='coerce')
            if converted.notna().any() or df[col].eq('-').all():
                df[col] = converted.astype('Int64')
    # Serialize any remaining list/dict columns to JSON strings so parquet
    # schema matches the string type declared in the Athena staging table DDL.
    for col in df.columns:
        if df[col].dtype == object:
            first_valid = df[col].dropna().iloc[0] if not df[col].dropna().empty else None
            if isinstance(first_valid, (list, dict)):
                df[col] = df[col].apply(
                    lambda v: json.dumps(v, separators=(',', ':')) if v is not None else None
                )
    return df


def _target_columns(athena_client):
    try:
        metadata = athena_client.get_table_metadata(
            CatalogName=TARGET_CATALOG_NAME,
            DatabaseName=TARGET_DATABASE_NAME,
            TableName=TABLE_NAME,
        )
        return metadata['TableMetadata']['Columns']
    except ClientError as exc:
        error_code = exc.response.get('Error', {}).get('Code')
        message = exc.response.get('Error', {}).get('Message', '')
        if error_code in ('MetadataException', 'EntityNotFoundException'):
            raise RuntimeError(
                f'Target table {TARGET_CATALOG_NAME}.{TARGET_DATABASE_NAME}.{TABLE_NAME} was not found. '
                'Verify the table stack is deployed, the Athena target catalog is configured correctly, '
                'and the table bucket is integrated with AWS analytics services in this Region.'
            ) from exc
        if error_code == 'InvalidRequestException' and 'Cannot find catalog' in message:
            raise RuntimeError(
                f'Athena catalog {TARGET_CATALOG_NAME} was not found. '
                'Set ATHENA_TARGET_CATALOG to the catalog that exposes the domains table in this account '
                '(for S3 Tables this is typically s3tablescatalog/<table-bucket-name>) and ensure the '
                'table bucket is integrated with AWS analytics services in this Region.'
            ) from exc
        raise


def _create_staging_table(athena_client, output_location, staging_location, temp_table, dataframe):
    columns = []
    for col in dataframe.columns:
        athena_type = _pandas_type_to_athena(dataframe[col].dtype)
        columns.append(f'{_quote_ident(col)} {athena_type}')

    create_sql = f'CREATE DATABASE IF NOT EXISTS {ATHENA_STAGING_DATABASE}'
    _run_athena_query(athena_client, create_sql, output_location)

    ddl = f'''
CREATE EXTERNAL TABLE {ATHENA_STAGING_DATABASE}.{temp_table} (
    {', '.join(columns)}
)
STORED AS PARQUET
LOCATION {_quote_literal(staging_location)}
'''.strip()
    _run_athena_query(athena_client, ddl, output_location)


def _insert_from_staging(athena_client, output_location, temp_table, target_columns, source_columns):
    source_names = set(source_columns)
    select_exprs = []
    insert_columns = []

    for target in target_columns:
        name = target['Name']
        target_type = target['Type']
        insert_columns.append(_quote_ident(name))
        if name in source_names:
            select_exprs.append(
                f'CAST({_quote_ident(name)} AS {target_type}) AS {_quote_ident(name)}'
            )
        else:
            select_exprs.append(
                f'CAST(NULL AS {target_type}) AS {_quote_ident(name)}'
            )

    insert_sql = f'''
INSERT INTO {TARGET_CATALOG_NAME}.{TARGET_DATABASE_NAME}.{TABLE_NAME}
({', '.join(insert_columns)})
SELECT {', '.join(select_exprs)}
FROM AwsDataCatalog.{ATHENA_STAGING_DATABASE}.{temp_table}
'''.strip()
    query_id = _run_athena_query(
        athena_client,
        insert_sql,
        output_location,
        database=TARGET_DATABASE_NAME,
        catalog=TARGET_CATALOG_NAME,
    )
    rows = _query_rows_inserted(athena_client, query_id)
    return rows


def _drop_staging_table(athena_client, output_location, temp_table):
    drop_sql = f'DROP TABLE IF EXISTS {ATHENA_STAGING_DATABASE}.{temp_table}'
    _run_athena_query(athena_client, drop_sql, output_location)


def _insert_file(bucket, key, s3_client, athena_client):
    local_path = f'/tmp/{os.path.basename(key)}'
    parquet_path = f'/tmp/{uuid.uuid4().hex}.parquet'
    temp_table = _safe_temp_table_name(key)
    temporary_bucket = f'webdb-{REGION}-temporary'
    output_location = ATHENA_OUTPUT_LOCATION or f's3://{temporary_bucket}/{ATHENA_RESULTS_PREFIX}/'
    staging_location = f's3://{temporary_bucket}/{ATHENA_STAGING_PREFIX}/{temp_table}/'
    parquet_key = f'{ATHENA_STAGING_PREFIX}/{temp_table}/data.parquet'

    try:
        try:
            s3_client.download_file(bucket, key, local_path)
        except ClientError as exc:
            if exc.response.get('Error', {}).get('Code') == 'NoSuchKey':
                print(f'Skipping {key}: object no longer exists in {bucket}')
                return
            raise
        dataframe = _build_dataframe(local_path)
        if dataframe is None:
            print(f'No records in {key}')
            return

        dataframe.to_parquet(parquet_path, index=False)
        s3_client.upload_file(parquet_path, temporary_bucket, parquet_key)

        target_columns = _target_columns(athena_client)
        if not target_columns:
            raise RuntimeError(
                f'Target table {TARGET_CATALOG_NAME}.{TARGET_DATABASE_NAME}.{TABLE_NAME} has no schema. '
                'Provision the table schema before ingesting data.'
            )

        _create_staging_table(
            athena_client=athena_client,
            output_location=output_location,
            staging_location=staging_location,
            temp_table=temp_table,
            dataframe=dataframe,
        )

        staging_count_value = _query_single_value(
            athena_client,
            (
                f'SELECT COUNT(*) '
                f'FROM AwsDataCatalog.{ATHENA_STAGING_DATABASE}.{temp_table}'
            ),
            output_location,
            database=ATHENA_STAGING_DATABASE,
        )
        staging_count = int(staging_count_value or 0)

        inserted_count = _insert_from_staging(
            athena_client=athena_client,
            output_location=output_location,
            temp_table=temp_table,
            target_columns=target_columns,
            source_columns=dataframe.columns,
        )

        if staging_count > 0 and inserted_count is not None and inserted_count == 0:
            raise RuntimeError(
                f'Athena insert reported 0 inserted rows for {key} '
                f'with {staging_count} staging rows in {temp_table}'
            )

        print(
            f'Inserted {inserted_count if inserted_count is not None else "?"} records from {key} via Athena '
            f'(staging rows: {staging_count})'
        )
    finally:
        try:
            _drop_staging_table(athena_client, output_location, temp_table)
        except (RuntimeError, TimeoutError, BotoCoreError, ClientError) as exc:
            print(f'Failed to drop staging table {temp_table}: {exc}')

        try:
            s3_client.delete_object(Bucket=temporary_bucket, Key=parquet_key)
        except (BotoCoreError, ClientError) as exc:
            print(f'Failed to delete staging object {parquet_key}: {exc}')

        if os.path.exists(local_path):
            os.remove(local_path)
        if os.path.exists(parquet_path):
            os.remove(parquet_path)


def handler(event, _context):
    s3_client = boto3.client('s3')
    athena_client = boto3.client('athena')

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
                _insert_file(bucket, key, s3_client, athena_client)

    return {'statusCode': 200}
