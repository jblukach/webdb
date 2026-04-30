# webdb

`webdb` is an AWS CDK app that builds a domain-data pipeline on AWS with S3, Lambda, Glue, and Athena.

## Stacks

| Stack | Description |
| --- | --- |
| `WebdbStorage` | S3 buckets, Glue database/table, and Athena workgroup/query resources |
| `WebdbTransfer` | Scheduled Lambda that copies source data into the enrich bucket |
| `WebdbEnrich` | Event-driven Lambda that enriches domain records with GeoIP data |
| `WebdbInsert` | S3/SQS-triggered Docker Lambda that converts JSONL to Parquet, writes database data, and archives gzip JSONL |
| `WebdbGithub` | OIDC role for GitHub Actions deployments |

## Table Schema

`webdb.domains` Glue external table (partitioned by `year`,`month`,`day`):

| Column | Type |
| --- | --- |
| `dns` | string |
| `ns` | array(string) |
| `ip` | string |
| `co` | string |
| `web` | string |
| `eml` | string |
| `hold` | string |
| `tel` | bigint |
| `rank` | bigint |
| `ts` | string |
| `id` | string |
| `sld` | string |
| `tld` | string |
| `asn` | bigint |

## Prerequisites

- Python 3.12+
- AWS CDK v2
- AWS credentials configured with a `db` profile

```bash
pip install -r requirements.txt
```

## Deploy

```bash
cdk deploy --profile db --all
```

```bash
cdk diff --profile db --all
```

## Athena Performance

- Always filter by partitions (`year`, `month`, `day`) to reduce scanned data.
- Prefer selective columns over `SELECT *`.
- Use date-pruned queries for interactive searches.

Example:

```sql
SELECT dns, ip, rank, ts, asn
FROM webdb.domains
WHERE year = 2026
AND month = 4
AND day = 30
ORDER BY ts DESC
LIMIT 100;
```

## Insert Pipeline Behavior

`WebdbInsert` ingests `.jsonl` objects from the insert bucket and performs three actions:

1. Converts JSONL to Parquet and writes to the database bucket.
2. Writes the original payload as gzip JSONL to the archive bucket.
3. Deletes the original source object from the insert bucket.

Partition date resolution order:

1. Parse `YYYYMMDD`, `YYYY-MM-DD`, or `YYYY_MM_DD` from the beginning of the source filename.
2. Fallback to the first record `ts` field (`YYYY-MM-DD`).
3. Fallback to current UTC date.

Current object key layout:

- Database Parquet: `year=YYYY/month=MM/day=DD/<source-stem>.parquet`
- Archive gzip JSONL: `year=YYYY/month=MM/day=DD/<source-filename>.gz`

## Repository Layout

- [app.py](app.py) — CDK app entry point
- [webdb/](webdb/) — CDK stack definitions
- [enrich/](enrich/) — enrichment Lambda handler
- [insert/](insert/) — Docker Lambda for JSONL to Parquet conversion
- [transfer/](transfer/) — transfer Lambda handler
