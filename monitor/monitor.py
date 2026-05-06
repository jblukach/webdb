import datetime
import gzip
import json
import os
from os.path import basename

import boto3
from boto3.dynamodb.types import TypeDeserializer


_DDB = boto3.client('dynamodb')
_S3 = boto3.client('s3')
_SNS = boto3.client('sns')
_DESERIALIZER = TypeDeserializer()

EXECUTION_TABLE = os.environ['EXECUTION_TABLE']
ARCHIVE_BUCKET = os.environ['ARCHIVE_BUCKET']
ALERT_TOPIC_ARN = os.environ['ALERT_TOPIC_ARN']


def _deserialize_item(raw_item):
    if not raw_item:
        return {}
    return {key: _DESERIALIZER.deserialize(value) for key, value in raw_item.items()}


def _get_execution_record(pk, sk):
    response = _DDB.get_item(
        TableName=EXECUTION_TABLE,
        Key={
            'pk': {'S': pk},
            'sk': {'S': sk},
        },
        ConsistentRead=True,
    )
    return _deserialize_item(response.get('Item'))


def _update_execution_status(pk, sk, status, reason=''):
    now = datetime.datetime.now(datetime.timezone.utc).isoformat()

    if reason:
        _DDB.update_item(
            TableName=EXECUTION_TABLE,
            Key={
                'pk': {'S': pk},
                'sk': {'S': sk},
            },
            UpdateExpression='SET #status = :status, completed_at = :completed_at, #reason = :reason',
            ExpressionAttributeNames={
                '#status': 'status',
                '#reason': 'reason',
            },
            ExpressionAttributeValues={
                ':status': {'S': status},
                ':completed_at': {'S': now},
                ':reason': {'S': reason[:1000]},
            },
        )
        return

    _DDB.update_item(
        TableName=EXECUTION_TABLE,
        Key={
            'pk': {'S': pk},
            'sk': {'S': sk},
        },
        UpdateExpression='SET #status = :status, completed_at = :completed_at REMOVE #reason',
        ExpressionAttributeNames={
            '#status': 'status',
            '#reason': 'reason',
        },
        ExpressionAttributeValues={
            ':status': {'S': status},
            ':completed_at': {'S': now},
        },
    )


def _publish_failure(subject, payload):
    _SNS.publish(
        TopicArn=ALERT_TOPIC_ARN,
        Subject=subject[:100],
        Message=json.dumps(payload, indent=2, sort_keys=True),
    )


def _output_has_objects(bucket, prefix):
    response = _S3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
    return bool(response.get('Contents'))


def _write_empty_output_marker(bucket, prefix):
    marker_key = prefix + '000-empty.gz'
    _S3.put_object(
        Bucket=bucket,
        Key=marker_key,
        Body=gzip.compress(b''),
        ContentType='application/gzip',
    )
    return marker_key


def _mark_as_processed(processed_table, source_bucket, source_key):
    if not processed_table:
        return

    now_epoch = int(datetime.datetime.now(datetime.timezone.utc).timestamp())
    ttl_epoch = now_epoch + (86400 * 30)
    pk = source_bucket + '#' + source_key

    _DDB.put_item(
        TableName=processed_table,
        Item={
            'pk': {'S': pk},
            'processed_at': {'N': str(now_epoch)},
            'ttl': {'N': str(ttl_epoch)},
        },
    )


def _archive_and_delete_source(source_bucket, source_key, partition_year, partition_month, partition_day):
    source_obj = _S3.get_object(Bucket=source_bucket, Key=source_key)
    source_bytes = source_obj['Body'].read()

    source_filename = basename(source_key)
    archive_key = (
        'year='
        + str(partition_year).zfill(4)
        + '/month='
        + str(partition_month).zfill(2)
        + '/day='
        + str(partition_day).zfill(2)
        + '/'
        + source_filename
        + '.gz'
    )

    _S3.put_object(
        Bucket=ARCHIVE_BUCKET,
        Key=archive_key,
        Body=gzip.compress(source_bytes),
        ContentType='application/x-ndjson',
        ContentEncoding='gzip',
    )

    _S3.delete_object(Bucket=source_bucket, Key=source_key)
    return archive_key


def _handle_athena_event(detail, event):
    query_execution_id = str(detail.get('queryExecutionId', '')).strip()
    state = str(detail.get('currentState', '')).strip()

    if not query_execution_id or not state:
        print('Athena event missing queryExecutionId/currentState')
        return

    pk = 'EXEC#' + query_execution_id
    sk = 'ATHENA'
    record = _get_execution_record(pk, sk)
    if not record:
        print('No execution record found for Athena query ' + query_execution_id)
        return

    current_status = str(record.get('status', ''))
    if current_status in ('SUCCEEDED', 'FAILED'):
        print('Execution record already terminal for Athena query ' + query_execution_id)
        return

    if state == 'SUCCEEDED':
        output_bucket = str(record.get('output_bucket', '')).strip()
        output_prefix = str(record.get('output_prefix', '')).strip()

        if output_bucket and output_prefix and not _output_has_objects(output_bucket, output_prefix):
            marker_key = _write_empty_output_marker(output_bucket, output_prefix)
            print('Wrote empty Athena output marker: s3://' + output_bucket + '/' + marker_key)

        run_table_name = str(record.get('run_table_name', '')).strip()
        run_table_region = str(record.get('run_table_region', '')).strip()
        run_pk = str(record.get('run_pk', '')).strip()
        run_sk = str(record.get('run_sk', '')).strip()

        if run_table_name and run_pk and run_sk:
            run_ddb = boto3.client('dynamodb', region_name=run_table_region or None)
            run_ddb.delete_item(
                TableName=run_table_name,
                Key={
                    'pk': {'S': run_pk},
                    'sk': {'S': run_sk},
                },
            )
            print('Deleted run item after Athena success: ' + run_sk)

        _update_execution_status(pk, sk, 'SUCCEEDED')
        return

    reason = str(detail.get('stateChangeReason', '')).strip() or 'Athena query ended in state ' + state
    _update_execution_status(pk, sk, 'FAILED', reason=reason)
    _publish_failure(
        subject='webdb Athena query failure',
        payload={
            'source': event.get('source', ''),
            'detail_type': event.get('detail-type', ''),
            'queryExecutionId': query_execution_id,
            'state': state,
            'reason': reason,
            'record': record,
        },
    )


def _handle_glue_event(detail, event):
    job_run_id = str(detail.get('jobRunId', '')).strip()
    state = str(detail.get('state', '')).strip()

    if not job_run_id or not state:
        print('Glue event missing jobRunId/state')
        return

    pk = 'EXEC#' + job_run_id
    sk = 'GLUE'
    record = _get_execution_record(pk, sk)
    if not record:
        print('No execution record found for Glue run ' + job_run_id)
        return

    current_status = str(record.get('status', ''))
    if current_status in ('SUCCEEDED', 'FAILED'):
        print('Execution record already terminal for Glue run ' + job_run_id)
        return

    if state == 'SUCCEEDED':
        source_bucket = str(record.get('source_bucket', '')).strip()
        source_key = str(record.get('source_key', '')).strip()
        partition_year = int(record.get('partition_year', 0) or 0)
        partition_month = int(record.get('partition_month', 0) or 0)
        partition_day = int(record.get('partition_day', 0) or 0)

        archive_key = _archive_and_delete_source(
            source_bucket=source_bucket,
            source_key=source_key,
            partition_year=partition_year,
            partition_month=partition_month,
            partition_day=partition_day,
        )
        print('Archived Glue source to s3://' + ARCHIVE_BUCKET + '/' + archive_key)

        processed_table = str(record.get('processed_objects_table', '')).strip()
        _mark_as_processed(processed_table, source_bucket, source_key)

        _update_execution_status(pk, sk, 'SUCCEEDED')
        return

    reason = str(detail.get('message', '')).strip() or 'Glue job ended in state ' + state
    _update_execution_status(pk, sk, 'FAILED', reason=reason)
    _publish_failure(
        subject='webdb Glue job failure',
        payload={
            'source': event.get('source', ''),
            'detail_type': event.get('detail-type', ''),
            'jobName': detail.get('jobName', ''),
            'jobRunId': job_run_id,
            'state': state,
            'reason': reason,
            'record': record,
        },
    )


def handler(event, _context):
    detail = event.get('detail', {})
    source = str(event.get('source', ''))

    print('Received monitor event from source=' + source)

    if source == 'aws.athena':
        _handle_athena_event(detail, event)
        return {'statusCode': 200}

    if source == 'aws.glue':
        _handle_glue_event(detail, event)
        return {'statusCode': 200}

    print('Ignoring unsupported source: ' + source)
    return {'statusCode': 200}
