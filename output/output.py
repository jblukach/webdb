import gzip
import json
import os
import time
from urllib.parse import unquote_plus

import boto3
from boto3.dynamodb.conditions import Key


DYNAMODB_TABLE = 'possibilities'
TTL_DAYS = 30


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


def _batch_delete(dynamo, table_name, keys):
    with dynamo.Table(table_name).batch_writer() as batch:
        for key in keys:
            batch.delete_item(Key=key)


def _get_table_domains(dynamo, table_name, search):
    table = dynamo.Table(table_name)
    sk_prefix = f'LUNKER#{search}#'

    domains = set()
    query_kwargs = {
        'KeyConditionExpression': Key('pk').eq('LUNKER#') & Key('sk').begins_with(sk_prefix),
        'ProjectionExpression': '#d',
        'ExpressionAttributeNames': {
            '#d': 'domain'
        }
    }

    while True:
        response = table.query(**query_kwargs)
        for item in response.get('Items', []):
            domain = item.get('domain', '').strip().lower()
            if domain:
                domains.add(domain)

        last_key = response.get('LastEvaluatedKey')
        if not last_key:
            break
        query_kwargs['ExclusiveStartKey'] = last_key

    return domains


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

                table_domains = _get_table_domains(dynamo, DYNAMODB_TABLE, first_folder)

                domains_to_insert = sorted(file_domains - table_domains)
                domains_to_remove = sorted(table_domains - file_domains)

                insert_items = []
                for domain in domains_to_insert:
                    sld, tld = _parse_domain(domain)
                    insert_items.append(
                        {
                            'pk': 'LUNKER#',
                            'sk': f'LUNKER#{first_folder}#{domain}',
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

                remove_keys = [
                    {
                        'pk': 'LUNKER#',
                        'sk': f'LUNKER#{first_folder}#{domain}'
                    }
                    for domain in domains_to_remove
                ]

                if remove_keys:
                    _batch_delete(dynamo, DYNAMODB_TABLE, remove_keys)

                print(
                    f'Processed s3://{bucket}/{key} - '
                    f'file={len(file_domains)}, table={len(table_domains)}, '
                    f'inserted={len(domains_to_insert)}, removed={len(domains_to_remove)}'
                )

            finally:
                if os.path.exists(local_path):
                    os.remove(local_path)
