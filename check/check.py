import datetime
import json
import os
import re

import boto3
from boto3.dynamodb.types import TypeDeserializer
from botocore.exceptions import ClientError


_STATE_PK = 'LUNKER#'
_SK_PATTERN = re.compile(r'^LUNKER#([^#]+)#')
_DESERIALIZER = TypeDeserializer()
_DYNAMODB_CLIENTS = {}


def _get_dynamodb_client(region_name):
    if region_name not in _DYNAMODB_CLIENTS:
        _DYNAMODB_CLIENTS[region_name] = boto3.client('dynamodb', region_name=region_name)
    return _DYNAMODB_CLIENTS[region_name]


def _extract_sld_from_sk(sk_value):
    if not sk_value:
        return ''

    match = _SK_PATTERN.match(sk_value)
    if not match:
        return ''

    return (match.group(1) or '').strip().lower()


def _parse_lastday_utc(value):
    if not value:
        return None

    text = str(value).strip()
    if not text:
        return None

    for fmt in ('%Y-%m-%d-%H', '%Y-%m-%d'):
        try:
            parsed = datetime.datetime.strptime(text, fmt)
            return parsed.replace(tzinfo=datetime.timezone.utc)
        except ValueError:
            continue

    try:
        parsed = datetime.datetime.fromisoformat(text)
    except ValueError:
        return None

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=datetime.timezone.utc)

    return parsed.astimezone(datetime.timezone.utc)


def _extract_permutations_from_attr(perm_attr):
    if not perm_attr:
        return []

    value = _DESERIALIZER.deserialize(perm_attr)
    raw_values = []

    if isinstance(value, (list, set, tuple)):
        raw_values.extend(str(entry) for entry in value)
    elif isinstance(value, str):
        try:
            parsed = json.loads(value)
            if isinstance(parsed, list):
                raw_values.extend(str(entry) for entry in parsed)
            else:
                raw_values.append(value)
        except (json.JSONDecodeError, TypeError):
            if ',' in value:
                raw_values.extend(part.strip() for part in value.split(','))
            else:
                raw_values.append(value)

    unique_permutations = []
    for entry in raw_values:
        normalized = (entry or '').strip().lower()
        if normalized and normalized not in unique_permutations:
            unique_permutations.append(normalized)

    return unique_permutations


def _get_state_candidates(state_table_name, state_region):
    dynamodb = _get_dynamodb_client(state_region)
    query_kwargs = {
        'TableName': state_table_name,
        'KeyConditionExpression': '#pk = :pk',
        'ExpressionAttributeNames': {
            '#pk': 'pk',
            '#sk': 'sk',
            '#sld': 'sld',
        },
        'ExpressionAttributeValues': {
            ':pk': {'S': _STATE_PK},
        },
        'ProjectionExpression': '#sk, #sld',
    }

    while True:
        response = dynamodb.query(**query_kwargs)

        for item in response.get('Items', []):
            sk_value = (item.get('sk') or {}).get('S', '')
            sld_value = (item.get('sld') or {}).get('S', '')

            normalized_sld = (sld_value or '').strip().lower()
            if not normalized_sld:
                normalized_sld = _extract_sld_from_sk(sk_value)

            if not normalized_sld or not sk_value:
                continue

            yield {
                'pk': _STATE_PK,
                'sk': sk_value,
                'sld': normalized_sld,
            }

        last_key = response.get('LastEvaluatedKey')
        if not last_key:
            break

        query_kwargs['ExclusiveStartKey'] = last_key


def _get_processed_sk_set(table_name, region_name):
    dynamodb = _get_dynamodb_client(region_name)
    query_kwargs = {
        'TableName': table_name,
        'KeyConditionExpression': '#pk = :pk',
        'ExpressionAttributeNames': {
            '#pk': 'pk',
            '#sk': 'sk',
        },
        'ExpressionAttributeValues': {
            ':pk': {'S': _STATE_PK},
        },
        'ProjectionExpression': '#sk',
    }

    processed_sk = set()
    while True:
        response = dynamodb.query(**query_kwargs)
        for item in response.get('Items', []):
            sk_value = (item.get('sk') or {}).get('S', '').strip()
            if sk_value:
                processed_sk.add(sk_value)

        last_key = response.get('LastEvaluatedKey')
        if not last_key:
            break

        query_kwargs['ExclusiveStartKey'] = last_key

    return processed_sk


def _get_check_lastday_by_sk(check_table_name, region_name):
    dynamodb = _get_dynamodb_client(region_name)
    query_kwargs = {
        'TableName': check_table_name,
        'KeyConditionExpression': '#pk = :pk',
        'ExpressionAttributeNames': {
            '#pk': 'pk',
            '#sk': 'sk',
            '#lastday': 'lastday',
        },
        'ExpressionAttributeValues': {
            ':pk': {'S': _STATE_PK},
        },
        'ProjectionExpression': '#sk, #lastday',
    }

    check_lastday_by_sk = {}
    while True:
        response = dynamodb.query(**query_kwargs)
        for item in response.get('Items', []):
            sk_value = (item.get('sk') or {}).get('S', '').strip()
            if not sk_value:
                continue

            check_lastday_by_sk[sk_value] = (item.get('lastday') or {}).get('S', '').strip()

        last_key = response.get('LastEvaluatedKey')
        if not last_key:
            break

        query_kwargs['ExclusiveStartKey'] = last_key

    return check_lastday_by_sk


def _should_skip_checked_candidate(lastday_value, now_utc):
    parsed = _parse_lastday_utc(lastday_value)
    if parsed is None:
        return True

    # Same UTC day is never eligible to rerun.
    if parsed.date() == now_utc.date():
        return True

    # Previous-day checks become eligible at/after 02:00 UTC.
    return now_utc.hour < 2


def _find_eligible_sld(state_table_name, run_table_name, check_table_name, state_region):
    run_sk = _get_processed_sk_set(run_table_name, state_region)
    check_lastday_by_sk = _get_check_lastday_by_sk(check_table_name, state_region)
    now_utc = datetime.datetime.now(datetime.timezone.utc)

    for candidate in _get_state_candidates(state_table_name, state_region):
        if candidate['sk'] in run_sk:
            continue

        if candidate['sk'] in check_lastday_by_sk and _should_skip_checked_candidate(
            check_lastday_by_sk[candidate['sk']],
            now_utc,
        ):
            continue

        return candidate

    return None


def _query_single_permutation_record(perm_table_name, perm_region, sld):
    sk_value = 'LUNKER#' + sld + '#'
    dynamodb = _get_dynamodb_client(perm_region)

    response = dynamodb.query(
        TableName=perm_table_name,
        KeyConditionExpression='#pk = :pk AND #sk = :sk',
        ExpressionAttributeNames={
            '#pk': 'pk',
            '#sk': 'sk',
            '#perm': 'perm',
        },
        ExpressionAttributeValues={
            ':pk': {'S': _STATE_PK},
            ':sk': {'S': sk_value},
        },
        ProjectionExpression='#perm',
        Limit=1,
    )

    item = (response.get('Items') or [{}])[0]
    return _extract_permutations_from_attr(item.get('perm', {}))


def _query_domains_for_sld(possibilities_table_name, state_region, sld):
    sk_prefix = 'LUNKER#' + sld + '#'
    dynamodb = _get_dynamodb_client(state_region)

    query_kwargs = {
        'TableName': possibilities_table_name,
        'KeyConditionExpression': '#pk = :pk AND begins_with(#sk, :sk_prefix)',
        'ExpressionAttributeNames': {
            '#pk': 'pk',
            '#sk': 'sk',
            '#domain': 'domain',
        },
        'ExpressionAttributeValues': {
            ':pk': {'S': _STATE_PK},
            ':sk_prefix': {'S': sk_prefix},
        },
        'ProjectionExpression': '#domain',
    }

    domains = []
    while True:
        response = dynamodb.query(**query_kwargs)
        for item in response.get('Items', []):
            domain = (item.get('domain') or {}).get('S', '').strip().lower()
            if domain:
                domains.append(domain)

        last_key = response.get('LastEvaluatedKey')
        if not last_key:
            break

        query_kwargs['ExclusiveStartKey'] = last_key

    return domains


def _count_permutation_occurrences(permutations, domains):
    counts = {}
    for perm in permutations:
        count = 0
        for domain in domains:
            if perm in domain:
                count += 1
        counts[perm] = count

    return counts


def _write_metrics(metrics_table_name, state_region, sld, counts, ttl_days):
    now = datetime.datetime.now(datetime.timezone.utc)
    ttl_epoch = int((now + datetime.timedelta(days=ttl_days)).timestamp())
    timestamp = now.isoformat()

    dynamodb = _get_dynamodb_client(state_region)

    for perm, total in counts.items():
        dynamodb.put_item(
            TableName=metrics_table_name,
            Item={
                'pk': {'S': _STATE_PK},
                'sk': {'S': 'LUNKER#' + sld + '#' + perm + '#'},
                'sld': {'S': sld},
                'perm': {'S': perm},
                'tbl': {'S': 'metrics'},
                'total': {'N': str(total)},
                'updated_at': {'S': timestamp},
                'ttl': {'N': str(ttl_epoch)},
            },
        )


def _mark_check_processed(check_table_name, state_region, candidate, ttl_days):
    now = datetime.datetime.now(datetime.timezone.utc)
    ttl_epoch = int((now + datetime.timedelta(days=ttl_days)).timestamp())

    dynamodb = _get_dynamodb_client(state_region)
    dynamodb.put_item(
        TableName=check_table_name,
        Item={
            'pk': {'S': candidate['pk']},
            'sk': {'S': candidate['sk']},
            'sld': {'S': candidate['sld']},
            'lastday': {'S': now.strftime('%Y-%m-%d-%H')},
            'ttl': {'N': str(ttl_epoch)},
        },
    )


def handler(_event, _context):
    state_table_name = os.environ.get('STATE_DYNAMODB_TABLE', '').strip()
    run_table_name = os.environ.get('RUN_DYNAMODB_TABLE', '').strip()
    check_table_name = os.environ.get('CHECK_DYNAMODB_TABLE', '').strip()
    possibilities_table_name = os.environ.get('POSSIBILITIES_DYNAMODB_TABLE', '').strip()
    metrics_table_name = os.environ.get('METRICS_DYNAMODB_TABLE', '').strip()
    perm_table_name = os.environ.get('PERM_DYNAMODB_TABLE', '').strip()
    state_region = os.environ.get('STATE_DYNAMODB_REGION', 'us-east-2').strip() or 'us-east-2'
    perm_region = os.environ.get('PERM_DYNAMODB_REGION', state_region).strip() or state_region
    ttl_days = int(os.environ.get('TTL_DAYS', '30'))

    required = {
        'STATE_DYNAMODB_TABLE': state_table_name,
        'RUN_DYNAMODB_TABLE': run_table_name,
        'CHECK_DYNAMODB_TABLE': check_table_name,
        'POSSIBILITIES_DYNAMODB_TABLE': possibilities_table_name,
        'METRICS_DYNAMODB_TABLE': metrics_table_name,
        'PERM_DYNAMODB_TABLE': perm_table_name,
    }

    for name, value in required.items():
        if not value:
            raise ValueError('Missing ' + name + ' environment variable')

    candidate = _find_eligible_sld(
        state_table_name=state_table_name,
        run_table_name=run_table_name,
        check_table_name=check_table_name,
        state_region=state_region,
    )

    if not candidate:
        return {
            'statusCode': 200,
            'processed': False,
            'message': 'No eligible SLD in state table',
        }

    try:
        permutations = _query_single_permutation_record(
            perm_table_name=perm_table_name,
            perm_region=perm_region,
            sld=candidate['sld'],
        )
    except ClientError as error:
        code = error.response.get('Error', {}).get('Code', 'Unknown')
        raise RuntimeError('Permutation query failed with code: ' + str(code)) from error

    if not permutations:
        _mark_check_processed(check_table_name, state_region, candidate, ttl_days)
        return {
            'statusCode': 200,
            'processed': True,
            'sld': candidate['sld'],
            'permutations': 0,
            'domains': 0,
            'message': 'No permutations found for selected SLD',
        }

    domains = _query_domains_for_sld(
        possibilities_table_name=possibilities_table_name,
        state_region=state_region,
        sld=candidate['sld'],
    )

    counts = _count_permutation_occurrences(permutations, domains)
    _write_metrics(metrics_table_name, state_region, candidate['sld'], counts, ttl_days)
    _mark_check_processed(check_table_name, state_region, candidate, ttl_days)

    return {
        'statusCode': 200,
        'processed': True,
        'sld': candidate['sld'],
        'permutations': len(permutations),
        'domains': len(domains),
        'message': 'Processed exactly one eligible SLD',
    }
