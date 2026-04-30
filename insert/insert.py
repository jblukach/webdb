import datetime
import gzip
import json
import os
import re
from urllib.parse import unquote_plus

import boto3
import pyarrow as pa
import pyarrow.parquet as pq


DATABASE_BUCKET = os.environ['DATABASE_BUCKET']
ARCHIVE_BUCKET = os.environ['ARCHIVE_BUCKET']
PARQUET_COMPRESSION = 'zstd'
PARQUET_TARGET_ROW_GROUP_SIZE = 100000


SCHEMA = pa.schema(
    [
        pa.field('dns', pa.string()),
        pa.field('ns', pa.list_(pa.string())),
        pa.field('ip', pa.string()),
        pa.field('co', pa.string()),
        pa.field('web', pa.string()),
        pa.field('eml', pa.string()),
        pa.field('hold', pa.string()),
        pa.field('tel', pa.int64()),
        pa.field('rank', pa.int64()),
        pa.field('ts', pa.string()),
        pa.field('id', pa.string()),
        pa.field('sld', pa.string()),
        pa.field('tld', pa.string()),
        pa.field('asn', pa.int64()),
    ]
)


def _coerce_int(value):
    if value in (None, '', '-'):
        return None

    if isinstance(value, int):
        return value

    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def _coerce_ns(value):
    if isinstance(value, list):
        return [str(item) for item in value if item not in (None, '')]

    if value in (None, '', '-'):
        return ['-']

    return [str(value)]


def _normalize_record(record):
    return {
        'dns': str(record.get('dns', '-')),
        'ns': _coerce_ns(record.get('ns')),
        'ip': str(record.get('ip', '-')),
        'co': str(record.get('co', '-')),
        'web': str(record.get('web', '-')),
        'eml': str(record.get('eml', '-')),
        'hold': str(record.get('hold', '-')),
        'tel': _coerce_int(record.get('tel')),
        'rank': _coerce_int(record.get('rank')),
        'ts': str(record.get('ts', '-')),
        'id': str(record.get('id', '-')),
        'sld': str(record.get('sld', '-')),
        'tld': str(record.get('tld', '-')),
        'asn': _coerce_int(record.get('asn')),
    }


def _partition_date(source_key, records):
    source_filename = os.path.basename(source_key)
    filename_match = re.match(r'^(\d{4})[-_]?(\d{2})[-_]?(\d{2})', source_filename)

    if filename_match:
        year, month, day = filename_match.groups()
        try:
            return datetime.datetime(int(year), int(month), int(day))
        except ValueError:
            pass

    ts_value = str(records[0].get('ts', '')).strip()

    if len(ts_value) >= 10:
        candidate = ts_value[:10]
        try:
            dt = datetime.datetime.strptime(candidate, '%Y-%m-%d')
            return dt
        except ValueError:
            pass

    return datetime.datetime.utcnow()


def _convert_object(s3_client, source_bucket, source_key):
    source_obj = s3_client.get_object(Bucket = source_bucket, Key = source_key)
    source_bytes = source_obj['Body'].read()

    records = []
    for raw_line in source_bytes.decode('utf-8').splitlines():
        if not raw_line.strip():
            continue
        record = json.loads(raw_line)
        records.append(_normalize_record(record))

    if not records:
        print(f'Skipping empty file s3://{source_bucket}/{source_key}')
        return

    table = pa.Table.from_pylist(records, schema = SCHEMA)
    partition_dt = _partition_date(source_key, records)
    year = partition_dt.strftime('%Y')
    month = partition_dt.strftime('%m')
    day = partition_dt.strftime('%d')

    source_filename = os.path.basename(source_key)
    parquet_stem = os.path.splitext(source_filename)[0]
    parquet_key = f'year={year}/month={month}/day={day}/{parquet_stem}.parquet'
    local_parquet_path = f'/tmp/{parquet_stem}.parquet'

    row_group_size = min(len(records), PARQUET_TARGET_ROW_GROUP_SIZE)
    pq.write_table(
        table,
        local_parquet_path,
        compression = PARQUET_COMPRESSION,
        use_dictionary = True,
        write_statistics = True,
        row_group_size = row_group_size
    )

    s3_client.upload_file(
        local_parquet_path,
        DATABASE_BUCKET,
        parquet_key,
        ExtraArgs = {'ContentType': 'application/x-parquet'}
    )
    print(f'Uploaded s3://{DATABASE_BUCKET}/{parquet_key}')

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

    try:
        os.remove(local_parquet_path)
    except OSError:
        pass


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

            if not source_key.endswith('.jsonl'):
                print(f'Skipping non-jsonl key {source_key}')
                continue

            _convert_object(s3_client, source_bucket, source_key)

    return {'statusCode': 200}