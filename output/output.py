import gzip
import json
import os
import time
from urllib.parse import unquote_plus

import boto3


DYNAMODB_TABLE = 'possibilities'
TTL_DAYS = 36


def _parse_domain(domain):
    parts = domain.rstrip('.').split('.')
    if len(parts) >= 2:
        tld = parts[-1]
        sld = parts[-2]
    elif len(parts) == 1:
        tld = parts[0]
        sld = parts[0]
    else:
        tld = '-'
        sld = '-'
    return sld, tld


def _batch_write(dynamo, table_name, items):
    with dynamo.Table(table_name).batch_writer() as batch:
        for item in items:
            batch.put_item(Item=item)


def handler(event, _context):
    s3_client = boto3.client('s3')
    dynamo = boto3.resource('dynamodb')

    ttl_value = int(time.time()) + TTL_DAYS * 86400

    for record in event['Records']:
        body = json.loads(record['body'])
        for s3_event in body.get('Records', []):
            bucket = s3_event['s3']['bucket']['name']
            key = unquote_plus(s3_event['s3']['object']['key'])

            first_folder = key.split('/')[0] if '/' in key else key
            filename = os.path.basename(key)
            local_path = f'/tmp/{filename}'

            try:
                s3_client.download_file(bucket, key, local_path)

                with gzip.open(local_path, 'rt', encoding='utf-8') as file_obj:
                    file_domains = {
                        line.strip().lower()
                        for line in file_obj
                        if line.strip()
                    }

                domains_to_insert = sorted(file_domains)

                insert_items = []
                for domain in domains_to_insert:
                    sld, tld = _parse_domain(domain)
                    insert_items.append(
                        {
                            'pk': 'LUNKER#',
                            'sk': f'LUNKER#{first_folder}#{domain}#',
                            'domain': domain,
                            'search': first_folder,
                            'sld': sld,
                            'tbl': 'possibilities',
                            'tld': tld,
                            'ttl': ttl_value,
                        }
                    )

                if insert_items:
                    _batch_write(dynamo, DYNAMODB_TABLE, insert_items)

                print(
                    f'Processed s3://{bucket}/{key} - '
                    f'file={len(file_domains)}, inserted={len(domains_to_insert)}'
                )

            finally:
                if os.path.exists(local_path):
                    os.remove(local_path)
