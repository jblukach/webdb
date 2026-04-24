import json
import os
import urllib.parse

import boto3
import pandas as pd
import pyarrow as pa
import pyiceberg
from pyiceberg.catalog import load_catalog


REGION = os.environ.get('AWS_REGION', 'us-east-2')
NAMESPACE = 'webdb'
TABLE_NAME = 'domains'


def _catalog_properties():
    sts = boto3.client('sts')
    account = sts.get_caller_identity()['Account']
    warehouse = f'arn:aws:s3tables:{REGION}:{account}:bucket/webdb'
    return {
        'warehouse': warehouse,
        'region_name': REGION,
    }


def _get_catalog():
    properties = _catalog_properties()

    # Newer pyiceberg versions support the built-in s3tables catalog type.
    try:
        return load_catalog('webdb', **({'type': 's3tables'} | properties))
    except ValueError as exc:
        if 'not a valid CatalogType' not in str(exc):
            raise

    # Older or variant builds may still expose S3TablesCatalog via class path.
    catalog_impl_candidates = (
        'pyiceberg.catalog.s3tables.S3TablesCatalog',
        'pyiceberg.catalog.s3_tables.S3TablesCatalog',
        'pyiceberg.catalog.s3tables_catalog.S3TablesCatalog',
    )

    last_error = None
    for catalog_impl in catalog_impl_candidates:
        try:
            return load_catalog('webdb', **({'py-catalog-impl': catalog_impl} | properties))
        except (ImportError, ModuleNotFoundError, AttributeError, TypeError, ValueError) as exc:
            last_error = exc

    print(f'pyiceberg version: {pyiceberg.__version__}')
    raise ValueError('Could not initialize an S3 Tables catalog in this runtime') from last_error


def _build_arrow_table(path):
    df = pd.read_json(path, lines=True)
    if df.empty:
        return None
    for col in df.columns:
        if df[col].dtype == object and df[col].eq('-').any():
            converted = pd.to_numeric(df[col], errors='coerce')
            if converted.notna().any() or df[col].eq('-').all():
                df[col] = converted.astype('Int64')
    return pa.Table.from_pandas(df, preserve_index=False)


def _insert_file(bucket, key, s3_client):
    local_path = f'/tmp/{os.path.basename(key)}'
    try:
        s3_client.download_file(bucket, key, local_path)
        arrow_table = _build_arrow_table(local_path)
    finally:
        if os.path.exists(local_path):
            os.remove(local_path)

    if arrow_table is None:
        print(f'No records in {key}')
        return

    catalog = _get_catalog()
    table = catalog.load_table(f'{NAMESPACE}.{TABLE_NAME}')
    table.append(arrow_table)
    print(f'Inserted {arrow_table.num_rows} records from {key}')


def handler(event, _context):
    s3_client = boto3.client('s3')

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
                _insert_file(bucket, key, s3_client)

    return {'statusCode': 200}
