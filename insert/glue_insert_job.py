import json
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from py4j.protocol import Py4JJavaError
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException, ParseException


TARGET_COLUMNS = [
    ('dns', 'string'),
    ('ns', 'string'),
    ('ip', 'string'),
    ('co', 'string'),
    ('web', 'string'),
    ('eml', 'string'),
    ('hold', 'string'),
    ('tel', 'bigint'),
    ('rank', 'bigint'),
    ('ts', 'string'),
    ('id', 'string'),
    ('sld', 'string'),
    ('tld', 'string'),
    ('asn', 'bigint'),
]


def _cast_column(df, name, target_type):
    if name not in df.columns:
        return F.lit(None).cast(target_type).alias(name)

    col = F.col(name)
    if target_type == 'bigint':
        return F.when(col == '-', None).otherwise(col).cast('bigint').alias(name)

    dtype = dict(df.dtypes).get(name, '')
    if dtype.startswith('array') or dtype.startswith('map') or dtype.startswith('struct'):
        return F.to_json(col).alias(name)

    return col.cast('string').alias(name)


def main():
    args = getResolvedOptions(
        sys.argv,
        [
            'JOB_NAME',
            'SOURCE_BUCKET',
            'SOURCE_KEY',
            'TARGET_CATALOG',
            'TARGET_DATABASE',
            'TARGET_TABLE',
        ],
    )

    source_path = f"s3://{args['SOURCE_BUCKET']}/{args['SOURCE_KEY']}"
    target_catalog = args['TARGET_CATALOG']
    target_table = f"{args['TARGET_DATABASE']}.{args['TARGET_TABLE']}"

    sc = SparkContext.getOrCreate()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args['JOB_NAME'], args)

    df = spark.read.json(source_path)
    if df.rdd.isEmpty():
        print(f'No records in {source_path}')
        job.commit()
        return

    projected = [_cast_column(df, name, col_type) for name, col_type in TARGET_COLUMNS]
    staged = df.select(*projected)
    staged.createOrReplaceTempView('staged_domains')

    db = args['TARGET_DATABASE']
    table = args['TARGET_TABLE']
    destinations = [
        f'{db}.{table}',
        f'glue_catalog.{db}.{table}',
        f'{target_catalog}.{db}.{table}',
    ]
    if '/' in target_catalog:
        destinations.append(f'`{target_catalog}`.{db}.{table}')

    attempted = {}
    inserted = False
    for destination in destinations:
        try:
            spark.sql(
                f'''INSERT INTO {destination}
SELECT {', '.join(name for name, _ in TARGET_COLUMNS)}
FROM staged_domains'''
            )
            print(f'Inserted records from {source_path} into {destination}')
            inserted = True
            break
        except (AnalysisException, ParseException, Py4JJavaError) as exc:
            attempted[destination] = str(exc)

    if not inserted:
        raise RuntimeError(
            json.dumps(
                {
                    'message': 'glue_insert_failed',
                    'source_path': source_path,
                    'target_table': target_table,
                    'attempted_destinations': destinations,
                    'errors': attempted,
                },
                separators=(',', ':'),
            )
        )

    print(f'Inserted records from {source_path} into {target_table}')
    job.commit()


if __name__ == '__main__':
    main()
