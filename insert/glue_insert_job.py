import sys

from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType


def _normalized_ns_column(raw_df):
    if 'ns' not in raw_df.columns:
        return F.array(F.lit('-'))

    ns_field = raw_df.schema['ns']
    if isinstance(ns_field.dataType, ArrayType):
        candidate = F.expr("filter(transform(ns, x -> cast(x as string)), x -> x is not null and x <> '')")
        return F.when(F.size(candidate) > 0, candidate).otherwise(F.array(F.lit('-')))

    candidate = F.col('ns').cast('string')
    return F.when(candidate.isNull() | (candidate == ''), F.array(F.lit('-'))).otherwise(F.array(candidate))


def _coalesced_string(raw_df, name):
    if name not in raw_df.columns:
        return F.lit('-')

    candidate = F.col(name).cast('string')
    return F.when(candidate.isNull() | (candidate == ''), F.lit('-')).otherwise(candidate)


def _coalesced_bigint(raw_df, name):
    if name not in raw_df.columns:
        return F.lit(None).cast('bigint')

    return F.col(name).cast('bigint')


def _is_table_not_found(exc):
    message = str(exc).lower()
    return (
        'table_or_view_not_found' in message
        or 'cannot be found' in message
        or 'no such table' in message
    )


def _create_table_with_initial_data(normalized_df, target):
    (
        normalized_df.writeTo(target)
        .tableProperty('format-version', '2')
        .tableProperty('write.format.default', 'parquet')
        .tableProperty('write.parquet.compression-codec', 'zstd')
        .partitionedBy(F.col('year'), F.col('month'), F.col('day'))
        .create()
    )


def _ensure_partition_spec(spark, target):
    for field_name in ['year', 'month', 'day']:
        try:
            spark.sql('ALTER TABLE ' + target + ' ADD PARTITION FIELD ' + field_name)
            print('Added Iceberg partition field ' + field_name + ' to ' + target)
        except Exception as exc:  # pylint: disable=broad-except
            message = str(exc).lower()
            if _is_table_not_found(exc):
                print('Target table ' + target + ' does not exist yet; partition field update deferred')
                return
            # Partition fields can already exist on later runs.
            if 'already exists' in message or 'already partition' in message or 'duplicate partition field' in message:
                print('Partition field ' + field_name + ' already exists on ' + target)
                continue
            raise


def main():
    args = getResolvedOptions(
        sys.argv,
        [
            'JOB_NAME',
            'source_bucket',
            'source_key',
            'database',
            'table',
            'year',
            'month',
            'day',
            'warehouse_path',
        ],
    )

    spark = (
        SparkSession.builder.appName(args['JOB_NAME'])
        .config('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
        .config('spark.sql.catalog.glue_catalog', 'org.apache.iceberg.spark.SparkCatalog')
        .config('spark.sql.catalog.glue_catalog.warehouse', args['warehouse_path'])
        .config('spark.sql.catalog.glue_catalog.catalog-impl', 'org.apache.iceberg.aws.glue.GlueCatalog')
        .config('spark.sql.catalog.glue_catalog.io-impl', 'org.apache.iceberg.aws.s3.S3FileIO')
        .getOrCreate()
    )

    source_path = 's3://' + args['source_bucket'] + '/' + args['source_key']
    raw_df = spark.read.json(source_path)

    if raw_df.rdd.isEmpty():
        print('No rows found in ' + source_path)
        spark.stop()
        return

    normalized_df = raw_df.select(
        _coalesced_string(raw_df, 'dns').alias('dns'),
        _normalized_ns_column(raw_df).alias('ns'),
        _coalesced_string(raw_df, 'ip').alias('ip'),
        _coalesced_string(raw_df, 'co').alias('co'),
        _coalesced_string(raw_df, 'web').alias('web'),
        _coalesced_string(raw_df, 'eml').alias('eml'),
        _coalesced_string(raw_df, 'hold').alias('hold'),
        _coalesced_bigint(raw_df, 'tel').alias('tel'),
        _coalesced_bigint(raw_df, 'rank').alias('rank'),
        _coalesced_string(raw_df, 'ts').alias('ts'),
        _coalesced_string(raw_df, 'id').alias('id'),
        _coalesced_string(raw_df, 'sld').alias('sld'),
        _coalesced_string(raw_df, 'tld').alias('tld'),
        _coalesced_bigint(raw_df, 'asn').alias('asn'),
        F.lit(int(args['year'])).cast('int').alias('year'),
        F.lit(int(args['month'])).cast('int').alias('month'),
        F.lit(int(args['day'])).cast('int').alias('day'),
    )

    target = 'glue_catalog.' + args['database'] + '.' + args['table']
    _ensure_partition_spec(spark, target)
    try:
        normalized_df.writeTo(target).append()
    except Exception as exc:  # pylint: disable=broad-except
        if not _is_table_not_found(exc):
            raise

        print('Target table ' + target + ' is missing; creating with current batch')

        try:
            _create_table_with_initial_data(normalized_df, target)
            print('Created table and inserted initial batch into ' + target)
        except Exception as create_exc:  # pylint: disable=broad-except
            create_message = str(create_exc).lower()
            if 'already exists' in create_message or 'alreadyexistsexception' in create_message:
                # Another run may create the table concurrently between append/create attempts.
                normalized_df.writeTo(target).append()
            else:
                raise

    inserted = normalized_df.count()
    print('Inserted ' + str(inserted) + ' rows into ' + target)

    spark.stop()


if __name__ == '__main__':
    main()
