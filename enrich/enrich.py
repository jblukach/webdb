import csv
import hashlib
import json
import os
from urllib.parse import unquote_plus

import boto3
import geoip2.database
import geoip2.errors


OUTPUT_BUCKET = 'webdb-us-east-2-insert'
MMDB_BUCKET = 'geolite-staged-lukach-io'
MMDB_KEY = 'GeoLite2-ASN.mmdb'
MMDB_LOCAL_PATH = '/tmp/GeoLite2-ASN.mmdb'


def _normalize_text(value):
    # Strip smart quotes (and any lingering plain quotes) from source values.
    return (
        value.replace('"', '')
        .replace('\u201c', '')
        .replace('\u201d', '')
        .strip()
    )


def _parse_csv_line(line):
    try:
        return next(csv.reader([line.rstrip('\n')], delimiter=';', quotechar='"'))
    except (StopIteration, csv.Error):
        return line.rstrip('\n').split(';')


def _safe_field(values, index):
    if index >= len(values):
        return '-'
    value = _normalize_text(values[index].strip())
    return value if value else '-'


def _split_ns(raw_ns):
    if raw_ns == '-':
        return ['-']
    parts = [_normalize_text(part.strip()) if part.strip() else '-' for part in raw_ns.split(',')]
    return parts if parts else ['-']


def _extract_ts_from_filename(filename):
    stem = os.path.splitext(filename)[0]
    if len(stem) >= 10:
        candidate = stem[:10]
        if (
            candidate[4] == '-'
            and candidate[7] == '-'
            and candidate[:4].isdigit()
            and candidate[5:7].isdigit()
            and candidate[8:10].isdigit()
        ):
            return candidate
    return stem


def _domain_parts(dns):
    if dns == '-':
        return '-', '-'
    labels = [label for label in dns.split('.') if label]
    if len(labels) < 2:
        return '-', '-'
    return labels[-2], labels[-1]


def _build_record(line, ts, reader):
    parts = _parse_csv_line(line)

    dns = _safe_field(parts, 0)
    ns_raw = _safe_field(parts, 1)
    ip = _safe_field(parts, 2)
    co = _safe_field(parts, 3)
    web = _safe_field(parts, 4)
    eml = _safe_field(parts, 5)
    hold = _safe_field(parts, 6)
    tel = _safe_field(parts, 7)
    rank = _safe_field(parts, 8)

    sld, tld = _domain_parts(dns)
    asn = '-'

    if ip != '-':
        try:
            response = reader.asn(ip)
            if response.autonomous_system_number is not None:
                asn = response.autonomous_system_number
        except (ValueError, TypeError, geoip2.errors.AddressNotFoundError):
            asn = '-'

    digest_input = f'{ts}-{dns}'
    record_id = hashlib.sha256(digest_input.encode('utf-8')).hexdigest()

    return {
        'dns': dns,
        'ns': _split_ns(ns_raw),
        'ip': ip,
        'co': co,
        'web': web,
        'eml': eml,
        'hold': hold,
        'tel': tel,
        'rank': rank,
        'ts': ts,
        'id': record_id,
        'sld': sld,
        'tld': tld,
        'asn': asn,
    }


def _process_object(s3_client, source_bucket, source_key):
    local_source_path = f'/tmp/{source_key}'
    local_source_dir = os.path.dirname(local_source_path)
    if local_source_dir:
        os.makedirs(local_source_dir, exist_ok=True)

    s3_client.download_file(source_bucket, source_key, local_source_path)
    print(f'Copied s3://{source_bucket}/{source_key} to {local_source_path}')

    s3_client.download_file(MMDB_BUCKET, MMDB_KEY, MMDB_LOCAL_PATH)
    print(f'Copied s3://{MMDB_BUCKET}/{MMDB_KEY} to {MMDB_LOCAL_PATH}')

    filename = os.path.basename(source_key)
    ts = _extract_ts_from_filename(filename)
    output_key = f'{os.path.splitext(source_key)[0]}.jsonl'
    output_filename = os.path.basename(output_key)
    local_output_path = f'/tmp/{output_filename}'

    with geoip2.database.Reader(MMDB_LOCAL_PATH) as reader:
        with open(local_source_path, 'r', encoding='utf-8') as src, open(
            local_output_path, 'w', encoding='utf-8'
        ) as dst:
            for raw_line in src:
                if not raw_line.strip():
                    continue
                row = _build_record(raw_line, ts, reader)
                dst.write(json.dumps(row, separators=(',', ':')))
                dst.write('\n')

    s3_client.upload_file(
        local_output_path,
        OUTPUT_BUCKET,
        output_key,
        ExtraArgs={'ContentType': 'application/x-ndjson'},
    )
    print(f'Uploaded s3://{OUTPUT_BUCKET}/{output_key}')

    s3_client.delete_object(Bucket=source_bucket, Key=source_key)
    print(f'Deleted s3://{source_bucket}/{source_key}')


def handler(event, _context):
    s3_client = boto3.client('s3')

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
                _process_object(s3_client, bucket, key)
            elif bucket:
                print(f'S3 record missing key for bucket {bucket}')
            else:
                print('S3 record missing bucket/key')

    return {'statusCode': 200}
