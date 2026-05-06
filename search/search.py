import boto3
import datetime
import json
import os
import re

from boto3.dynamodb.types import TypeDeserializer
from botocore.exceptions import ClientError


_ATHENA = boto3.client('athena')
_DYNAMODB_CLIENTS = {}
_DESERIALIZER = TypeDeserializer()
_STATE_PK = 'LUNKER#'


def _get_dynamodb_client(region_name):
    if region_name not in _DYNAMODB_CLIENTS:
        _DYNAMODB_CLIENTS[region_name] = boto3.client('dynamodb', region_name=region_name)
    return _DYNAMODB_CLIENTS[region_name]


def _safe_path_value(value):
    safe = re.sub(r'[^a-z0-9.-]+', '-', value.lower())
    return safe.strip('-') or 'unknown'


def _sql_string(value):
    return value.replace("'", "''")


def _sql_like_string(value):
    return _sql_string(value)


def _build_short_sld_where_clause(terms):
    normalized_terms = []
    for term in terms:
        normalized = (term or '').strip().lower()
        if normalized and normalized not in normalized_terms:
            normalized_terms.append(normalized)

    if not normalized_terms:
        return ''

    in_values = ', '.join("'" + _sql_string(term) + "'" for term in normalized_terms)
    return 'lower(sld) IN (' + in_values + ')'


def _build_long_sld_where_clause(terms):
    clauses = []
    for term in terms:
        normalized = (term or '').strip().lower()
        if not normalized:
            continue
        contains_pattern = _sql_like_string('%' + normalized + '%')
        clauses.append("lower(dns) LIKE '" + contains_pattern + "'")

    if not clauses:
        return ''

    return ' OR '.join(clauses)


def _build_search_terms(item, permutations):
    terms = []

    for candidate in [item] + list(permutations):
        normalized = (candidate or '').strip().lower()
        if normalized and normalized not in terms:
            terms.append(normalized)

    return terms


def _extract_sld_from_sk(sk_value):
    if not sk_value:
        return ''

    match = re.match(r'^LUNKER#([^#]+)#', sk_value)
    if not match:
        return ''

    return (match.group(1) or '').strip().lower()


def _extract_permutations_from_attr(perm_attr):
    raw_values = []

    if perm_attr:
        value = _DESERIALIZER.deserialize(perm_attr)

        if isinstance(value, (list, set, tuple)):
            raw_values.extend(str(entry) for entry in value)
        elif isinstance(value, str):
            # Support JSON-encoded arrays and CSV-like strings in legacy rows.
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

    perms = []
    for value in raw_values:
        normalized = (value or '').strip().lower()
        if normalized and normalized not in perms:
            perms.append(normalized)

    return perms


def _build_table_identifiers(perm_table_env):
    table_identifiers = []

    if perm_table_env:
        table_identifiers.append(perm_table_env)

    # Preserve order but de-duplicate.
    unique_identifiers = []
    for identifier in table_identifiers:
        if identifier and identifier not in unique_identifiers:
            unique_identifiers.append(identifier)

    return unique_identifiers


def _query_single_run_item(run_table_name, state_region):
    dynamodb = _get_dynamodb_client(state_region)
    response = dynamodb.query(
        TableName=run_table_name,
        KeyConditionExpression='#pk = :pk',
        ExpressionAttributeNames={
            '#pk': 'pk',
            '#sk': 'sk',
            '#sld': 'sld'
        },
        ExpressionAttributeValues={
            ':pk': {'S': _STATE_PK}
        },
        ProjectionExpression='#sk, #sld',
        Limit=1
    )

    items = response.get('Items', [])
    if not items:
        return None

    entry = items[0]
    sk_value = (entry.get('sk') or {}).get('S', '')
    sld_value = (entry.get('sld') or {}).get('S', '')

    normalized_sld = (sld_value or '').strip().lower()
    if not normalized_sld:
        normalized_sld = _extract_sld_from_sk(sk_value)

    if not normalized_sld or not sk_value:
        return None

    return {
        'pk': _STATE_PK,
        'sk': sk_value,
        'sld': normalized_sld
    }


def _put_execution_record(
    execution_table_name,
    state_region,
    query_execution_id,
    run_table_name,
    run_item,
    output_bucket,
    output_prefix,
    normalized_item,
    search_mode,
):
    now = datetime.datetime.now(datetime.timezone.utc)
    ttl_epoch = int(now.timestamp()) + (86400 * 7)
    dynamodb = _get_dynamodb_client(state_region)

    dynamodb.put_item(
        TableName=execution_table_name,
        Item={
            'pk': {'S': 'EXEC#' + query_execution_id},
            'sk': {'S': 'ATHENA'},
            'execution_type': {'S': 'ATHENA'},
            'status': {'S': 'PENDING'},
            'created_at': {'S': now.isoformat()},
            'ttl': {'N': str(ttl_epoch)},
            'item': {'S': normalized_item},
            'search_mode': {'S': search_mode},
            'run_table_name': {'S': run_table_name},
            'run_table_region': {'S': state_region},
            'run_pk': {'S': run_item['pk']},
            'run_sk': {'S': run_item['sk']},
            'output_bucket': {'S': output_bucket},
            'output_prefix': {'S': output_prefix},
        }
    )


def _get_permutations(perm_table_env, normalized_item, region_candidates):
    table_identifiers = _build_table_identifiers(perm_table_env)

    print('DynamoDB lookup - table_identifiers=' + str(table_identifiers))
    print('DynamoDB lookup - region_candidates=' + str(region_candidates))
    print('DynamoDB lookup - key=pk:LUNKER#, sk:LUNKER#' + normalized_item + '#')

    for perm_region in region_candidates:
        if not perm_region:
            continue

        dynamodb = _get_dynamodb_client(perm_region)

        for table_identifier in table_identifiers:
            try:
                print('Querying DynamoDB - region=' + perm_region + ' table=' + table_identifier)
                response = dynamodb.query(
                    TableName=table_identifier,
                    KeyConditionExpression='#pk = :pk AND #sk = :sk',
                    ExpressionAttributeNames={
                        '#pk': 'pk',
                        '#sk': 'sk',
                        '#perm': 'perm'
                    },
                    ExpressionAttributeValues={
                        ':pk': {'S': 'LUNKER#'},
                        ':sk': {'S': 'LUNKER#' + normalized_item + '#'}
                    },
                    ProjectionExpression='#perm',
                    Limit=1
                )
            except ClientError as e:
                code = e.response.get('Error', {}).get('Code')
                print(
                    'DynamoDB query failed: region=' + perm_region
                    + ' table=' + table_identifier
                    + ' code=' + str(code)
                )
                if code in ('ResourceNotFoundException', 'ResourceNotFound'):
                    continue
                raise

            items = response.get('Items', [])
            item = items[0] if items else {}
            if not item:
                print('DynamoDB item not found: region=' + perm_region + ' table=' + table_identifier)
                continue

            perm_attr = item.get('perm', {})
            perms = _extract_permutations_from_attr(perm_attr)

            print(
                'Permutation table hit: table=' + table_identifier
                + ' region=' + perm_region
                + ' permutations=' + str(len(perms))
            )

            return perms

    print('No permutations found in any region/table combination')
    return []


def handler(_event, _context):
    _ = _event
    _ = _context

    region_candidates = []
    perm_table_env = os.environ.get('DYNAMODB_TABLE', '').strip()
    run_table_name = os.environ.get('RUN_DYNAMODB_TABLE', '').strip()
    state_region = os.environ.get('STATE_DYNAMODB_REGION', 'us-east-2').strip() or 'us-east-2'
    if not perm_table_env:
        return {
            'statusCode': 500,
            'body': json.dumps({'message': 'Missing DYNAMODB_TABLE environment variable'})
        }
    if not run_table_name:
        return {
            'statusCode': 500,
            'body': json.dumps({'message': 'Missing RUN_DYNAMODB_TABLE environment variable'})
        }

    execution_table_name = os.environ.get('EXECUTION_TABLE', '').strip()
    if not execution_table_name:
        return {
            'statusCode': 500,
            'body': json.dumps({'message': 'Missing EXECUTION_TABLE environment variable'})
        }

    run_item = _query_single_run_item(run_table_name, state_region)
    if not run_item:
        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'No pending SLD entries in run table'})
        }

    normalized_item = run_item['sld']
    item = normalized_item
    print('Searching SLD from run table: ' + normalized_item)

    if perm_table_env.startswith('arn:'):
        arn_parts = perm_table_env.split(':')
        if len(arn_parts) > 3 and arn_parts[3]:
            region_candidates.append(arn_parts[3])

    for region_name in [os.environ.get('AWS_REGION', '').strip(), 'us-east-2']:
        if region_name and region_name not in region_candidates:
            region_candidates.append(region_name)

    permutations = _get_permutations(perm_table_env, normalized_item, region_candidates)
    print('Permutations retrieved: ' + str(len(permutations)))

    short_sld_mode = len(normalized_item) < 5

    terms = _build_search_terms(item, permutations)
    where_clause = ''

    if short_sld_mode:
        where_clause = _build_short_sld_where_clause(terms)
    else:
        where_clause = _build_long_sld_where_clause(terms)

    print('Search terms total: ' + str(len(terms)))

    if not where_clause:
        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'No WHERE clause generated from terms'})
        }

    print('WHERE clause terms: ' + str(len(terms)))
    print('WHERE clause mode: ' + ('lower(sld) IN (...)' if short_sld_mode else 'lower(dns) LIKE %sld%'))

    now = datetime.datetime.now(datetime.timezone.utc)
    date_stem = now.strftime('%Y-%m-%d-%H-%M-%S')

    output_prefix = _safe_path_value(normalized_item) + '/' + date_stem + '/'

    database = os.environ.get('ATHENA_DATABASE', 'webdb')
    table = os.environ.get('ATHENA_TABLE', 'domains')
    output_bucket = os.environ['OUTPUT_BUCKET']
    temp_bucket = os.environ['TEMP_BUCKET']

    search_mode = 'sld-exact-with-permutations' if short_sld_mode else 'dns-contains-sld'
    print('Search mode: ' + search_mode)

    query = (
        'UNLOAD ('
        'SELECT DISTINCT dns '
        'FROM ' + database + '.' + table + ' '
        'WHERE ' + where_clause + ' '
        'ORDER BY dns ASC'
        ') '
        "TO 's3://" + output_bucket + '/' + output_prefix + "' "
        "WITH (format = 'TEXTFILE', compression = 'GZIP')"
    )

    response = _ATHENA.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': database
        },
        WorkGroup=os.environ.get('ATHENA_WORKGROUP', 'webdb'),
        ResultConfiguration={
            'OutputLocation': 's3://' + temp_bucket + '/athena-results/'
        }
    )

    query_execution_id = response['QueryExecutionId']
    print('QueryExecutionId: ' + query_execution_id)

    _put_execution_record(
        execution_table_name=execution_table_name,
        state_region=state_region,
        query_execution_id=query_execution_id,
        run_table_name=run_table_name,
        run_item=run_item,
        output_bucket=output_bucket,
        output_prefix=output_prefix,
        normalized_item=normalized_item,
        search_mode=search_mode,
    )

    return {
        'statusCode': 200,
        'body': json.dumps(
            {
                'message': 'Athena search started',
                'item': normalized_item,
                'terms': terms,
                'searchMode': search_mode,
                'queryExecutionId': query_execution_id,
                'output': 's3://' + output_bucket + '/' + output_prefix,
                'executionTracking': 'stored'
            }
        )
    }
