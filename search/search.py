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
_STATE_TTL_DAYS = 36


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
    # Use '#' as LIKE escape character to avoid Athena backslash escape parsing issues.
    return _sql_string(value).replace('#', '##').replace('%', '#%').replace('_', '#_')


def _build_where_clause(terms, exact_sld_match=False):
    if exact_sld_match:
        return 'lower(sld) IN (' + ', '.join("'" + _sql_string(term) + "'" for term in terms) + ')'

    like_clauses = []
    for term in terms:
        like_term = _sql_like_string(term)
        like_clauses.append("lower(dns) LIKE '%" + like_term + "%' ESCAPE '#'")

    return ' OR '.join(like_clauses)


def _build_search_terms(item, permutations):
    terms = []

    for candidate in [item] + list(permutations):
        normalized = (candidate or '').strip().lower()
        if normalized and normalized not in terms:
            terms.append(normalized)

    return terms


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


def _get_permutations(perm_table_env, normalized_item, region_candidates):
    table_identifiers = _build_table_identifiers(perm_table_env)

    print('DynamoDB lookup - table_identifiers=' + str(table_identifiers))
    print('DynamoDB lookup - region_candidates=' + str(region_candidates))
    print('DynamoDB lookup - key=pk:LUNKER#, sk:LUNKER#' + normalized_item)

    for perm_region in region_candidates:
        if not perm_region:
            continue

        dynamodb = _get_dynamodb_client(perm_region)

        for table_identifier in table_identifiers:
            try:
                print('Querying DynamoDB - region=' + perm_region + ' table=' + table_identifier)
                response = dynamodb.get_item(
                    TableName=table_identifier,
                    Key={
                        'pk': {'S': 'LUNKER#'},
                        'sk': {'S': 'LUNKER#' + normalized_item}
                    },
                    ProjectionExpression='perm'
                )
            except ClientError as e:
                code = e.response.get('Error', {}).get('Code')
                print(
                    'DynamoDB get_item failed: region=' + perm_region
                    + ' table=' + table_identifier
                    + ' code=' + str(code)
                )
                if code in ('ResourceNotFoundException', 'ResourceNotFound'):
                    continue
                raise

            item = response.get('Item', {})
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


def _build_state_sk(normalized_item):
    return 'LUNKER#' + normalized_item + '#'


def _get_state_record(state_table_name, normalized_item, state_region):
    dynamodb = _get_dynamodb_client(state_region)
    response = dynamodb.get_item(
        TableName=state_table_name,
        Key={
            'pk': {'S': _STATE_PK},
            'sk': {'S': _build_state_sk(normalized_item)}
        }
    )
    return response.get('Item', {})


def _put_state_record(state_table_name, normalized_item, state_region, now_utc):
    ttl_value = int((now_utc + datetime.timedelta(days=_STATE_TTL_DAYS)).timestamp())
    dynamodb = _get_dynamodb_client(state_region)
    dynamodb.put_item(
        TableName=state_table_name,
        Item={
            'pk': {'S': _STATE_PK},
            'sk': {'S': _build_state_sk(normalized_item)},
            'lastday': {'S': now_utc.strftime('%Y-%m-%d')},
            'ttl': {'N': str(ttl_value)}
        }
    )


def _is_monthly_full_search_time(now_utc):
    return now_utc.day == 1 and now_utc.hour == 2


def handler(event, _context):
    print(event)

    item = event.get('Item')
    if not item:
        return {
            'statusCode': 400,
            'body': json.dumps({'message': 'Missing Item in payload'})
        }

    region_candidates = []
    perm_table_env = os.environ.get('DYNAMODB_TABLE', '').strip()
    state_table_name = os.environ.get('STATE_DYNAMODB_TABLE', '').strip()
    state_region = os.environ.get('STATE_DYNAMODB_REGION', 'us-east-2').strip() or 'us-east-2'
    if not perm_table_env:
        return {
            'statusCode': 500,
            'body': json.dumps({'message': 'Missing DYNAMODB_TABLE environment variable'})
        }
    if not state_table_name:
        return {
            'statusCode': 500,
            'body': json.dumps({'message': 'Missing STATE_DYNAMODB_TABLE environment variable'})
        }

    if perm_table_env.startswith('arn:'):
        arn_parts = perm_table_env.split(':')
        if len(arn_parts) > 3 and arn_parts[3]:
            region_candidates.append(arn_parts[3])

    for region_name in [os.environ.get('AWS_REGION', '').strip(), 'us-east-2']:
        if region_name and region_name not in region_candidates:
            region_candidates.append(region_name)

    normalized_item = item.strip().lower()
    print('Searching SLD: ' + normalized_item)

    permutations = _get_permutations(perm_table_env, normalized_item, region_candidates)
    print('Permutations retrieved: ' + str(len(permutations)))

    terms = _build_search_terms(item, permutations)
    print('Search terms total: ' + str(len(terms)) + ' (sld+permutations)')

    if not terms:
        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'No search terms'})
        }

    exact_sld_match = len(normalized_item) < 5
    where_clause = _build_where_clause(terms, exact_sld_match=exact_sld_match)
    print('WHERE clause terms: ' + str(len(terms)))
    print('WHERE clause mode: ' + ('lower(sld) exact match' if exact_sld_match else 'lower(dns) LIKE'))

    now = datetime.datetime.now(datetime.timezone.utc)
    date_stem = now.strftime('%Y-%m-%d-%H-%M-%S')

    output_prefix = _safe_path_value(normalized_item) + '/' + date_stem + '/'

    database = os.environ.get('ATHENA_DATABASE', 'webdb')
    table = os.environ.get('ATHENA_TABLE', 'domains')
    output_bucket = os.environ['OUTPUT_BUCKET']
    temp_bucket = os.environ['TEMP_BUCKET']

    state_item = _get_state_record(state_table_name, normalized_item, state_region)
    has_state_entry = bool(state_item)
    is_monthly_full = _is_monthly_full_search_time(now)

    partition_clause = ''
    if has_state_entry and not is_monthly_full:
        partition_clause = (
            'year = ' + str(now.year)
            + ' AND month = ' + str(now.month)
            + ' AND day = ' + str(now.day)
        )

    if partition_clause:
        where_clause = '(' + where_clause + ') AND ' + partition_clause

    search_mode = 'full' if not partition_clause else 'daily-current-day'
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

    _put_state_record(state_table_name, normalized_item, state_region, now)

    return {
        'statusCode': 200,
        'body': json.dumps(
            {
                'message': 'Athena search started',
                'item': item,
                'terms': terms,
                'searchMode': search_mode,
                'queryExecutionId': query_execution_id,
                'output': 's3://' + output_bucket + '/' + output_prefix
            }
        )
    }
