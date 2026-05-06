import datetime
import os
import re

import boto3


_STATE_PK = 'LUNKER#'
_SK_PREFIX = 'LUNKER#'
_SLD_PATTERN = re.compile(r'^LUNKER#([^#]+)#')
_BATCH_GET_CHUNK = 100
_BATCH_WRITE_CHUNK = 25


def _chunked(items, size):
    for i in range(0, len(items), size):
        yield items[i:i + size]


def _extract_sld(sk_value):
    if not sk_value:
        return ''

    match = _SLD_PATTERN.match(sk_value)
    if not match:
        return ''

    return (match.group(1) or '').strip().lower()


def _scan_unique_slds(permutation_client, permutation_table_arn):
    unique_slds = set()
    query_kwargs = {
        'TableName': permutation_table_arn,
        'ProjectionExpression': '#pk, #sk',
        'ExpressionAttributeNames': {
            '#pk': 'pk',
            '#sk': 'sk'
        },
        'KeyConditionExpression': '#pk = :pk AND begins_with(#sk, :prefix)',
        'ExpressionAttributeValues': {
            ':pk': {'S': _STATE_PK},
            ':prefix': {'S': _SK_PREFIX}
        }
    }

    while True:
        response = permutation_client.query(**query_kwargs)

        for row in response.get('Items', []):
            sld = _extract_sld((row.get('sk') or {}).get('S', ''))
            if sld:
                unique_slds.add(sld)

        last_key = response.get('LastEvaluatedKey')
        if not last_key:
            break

        query_kwargs['ExclusiveStartKey'] = last_key

    return sorted(unique_slds)


def _get_existing_state_sks(state_client, state_table_name, slds):
    existing_sks = set()

    for chunk in _chunked(slds, _BATCH_GET_CHUNK):
        request_items = {
            state_table_name: {
                'Keys': [
                    {
                        'pk': {'S': _STATE_PK},
                        'sk': {'S': _SK_PREFIX + sld + '#'}
                    }
                    for sld in chunk
                ],
                'ProjectionExpression': '#sk',
                'ExpressionAttributeNames': {
                    '#sk': 'sk'
                }
            }
        }

        response = state_client.batch_get_item(RequestItems=request_items)

        for row in response.get('Responses', {}).get(state_table_name, []):
            sk_value = (row.get('sk') or {}).get('S', '')
            if sk_value:
                existing_sks.add(sk_value)

        unprocessed = response.get('UnprocessedKeys', {})
        while unprocessed:
            response = state_client.batch_get_item(RequestItems=unprocessed)
            for row in response.get('Responses', {}).get(state_table_name, []):
                sk_value = (row.get('sk') or {}).get('S', '')
                if sk_value:
                    existing_sks.add(sk_value)
            unprocessed = response.get('UnprocessedKeys', {})

    return existing_sks


def _batch_write_requests(state_client, request_batches):
    for request in request_batches:
        response = state_client.batch_write_item(RequestItems=request)
        unprocessed = response.get('UnprocessedItems', {})

        while unprocessed:
            response = state_client.batch_write_item(RequestItems=unprocessed)
            unprocessed = response.get('UnprocessedItems', {})


def _write_missing_to_state_and_run(state_client, state_table_name, run_table_name, missing_slds, now_utc, ttl_days):
    ttl_epoch = int((now_utc + datetime.timedelta(days=ttl_days)).timestamp())

    state_writes = []
    run_writes = []

    for chunk in _chunked(missing_slds, _BATCH_WRITE_CHUNK):
        state_writes.append(
            {
                state_table_name: [
                    {
                        'PutRequest': {
                            'Item': {
                                'pk': {'S': _STATE_PK},
                                'sk': {'S': _SK_PREFIX + sld + '#'},
                                'sld': {'S': sld},
                                'lastday': {'S': now_utc.strftime('%Y-%m-%d')},
                                'ttl': {'N': str(ttl_epoch)}
                            }
                        }
                    }
                    for sld in chunk
                ]
            }
        )

        run_writes.append(
            {
                run_table_name: [
                    {
                        'PutRequest': {
                            'Item': {
                                'pk': {'S': _STATE_PK},
                                'sk': {'S': _SK_PREFIX + sld + '#'},
                                'sld': {'S': sld},
                                'lastday': {'S': now_utc.strftime('%Y-%m-%d')},
                                'ttl': {'N': str(ttl_epoch)}
                            }
                        }
                    }
                    for sld in chunk
                ]
            }
        )

    _batch_write_requests(state_client, state_writes)
    _batch_write_requests(state_client, run_writes)


def handler(_event, _context):
    permutation_table_arn = os.environ.get('DYNAMODB_TABLE', '').strip()
    state_table_name = os.environ.get('STATE_DYNAMODB_TABLE', '').strip()
    run_table_name = os.environ.get('RUN_DYNAMODB_TABLE', '').strip()
    state_region = os.environ.get('STATE_DYNAMODB_REGION', 'us-east-2').strip() or 'us-east-2'
    ttl_days = int(os.environ.get('TTL_DAYS', '365'))

    if not permutation_table_arn:
        raise ValueError('Missing DYNAMODB_TABLE environment variable')
    if not state_table_name:
        raise ValueError('Missing STATE_DYNAMODB_TABLE environment variable')
    if not run_table_name:
        raise ValueError('Missing RUN_DYNAMODB_TABLE environment variable')

    permutation_region = state_region
    if permutation_table_arn.startswith('arn:'):
        arn_parts = permutation_table_arn.split(':')
        if len(arn_parts) > 3 and arn_parts[3]:
            permutation_region = arn_parts[3]

    permutation_client = boto3.client('dynamodb', region_name=permutation_region)
    state_client = boto3.client('dynamodb', region_name=state_region)

    all_slds = _scan_unique_slds(permutation_client, permutation_table_arn)
    if not all_slds:
        return {
            'statusCode': 200,
            'seeded': 0,
            'message': 'No SLD values found in permutation table'
        }

    existing_sks = _get_existing_state_sks(state_client, state_table_name, all_slds)

    missing_slds = []
    for sld in all_slds:
        sk_value = _SK_PREFIX + sld + '#'
        if sk_value not in existing_sks:
            missing_slds.append(sld)

    if not missing_slds:
        return {
            'statusCode': 200,
            'seeded': 0,
            'scanned': len(all_slds),
            'message': 'All SLD entries already exist in state table'
        }

    now_utc = datetime.datetime.now(datetime.timezone.utc)
    _write_missing_to_state_and_run(
        state_client,
        state_table_name,
        run_table_name,
        missing_slds,
        now_utc,
        ttl_days
    )

    return {
        'statusCode': 200,
        'scanned': len(all_slds),
        'seeded': len(missing_slds),
        'message': 'Seeded missing LUNKER SLD entries to state and run tables'
    }
