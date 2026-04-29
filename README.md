# webdb

`webdb` is an AWS CDK app that builds a domain-data pipeline on AWS using S3 Tables (Iceberg).

## Stacks

| Stack | Description |
|---|---|
| `WebdbStorage` | S3 buckets: `enrich`, `insert`, `archive`, `temporary` |
| `WebdbTable` | S3 Tables bucket, namespace, and Iceberg table (`webdb.domains`) |
| `WebdbTransfer` | Scheduled Lambda that copies source data into the enrich bucket |
| `WebdbEnrich` | Event-driven Lambda that enriches domain records with GeoIP data |
| `WebdbInsert` | Event-driven Docker Lambda that ingests JSONL into the Iceberg table |
| `WebdbGithub` | OIDC role for GitHub Actions deployments |

## Insert Pipeline

JSONL files uploaded to `webdb-us-east-2-insert` are processed by an SQS-triggered Docker Lambda:

1. Downloads JSONL from insert bucket
2. Converts to DataFrame — coerces `'-'` strings to null, serializes nested objects to JSON strings
3. Writes Parquet to ephemeral storage (`/tmp`)
4. Fetches table location via S3 Tables SDK (`s3tables:GetTable`)
5. Uploads Parquet directly to `{tableLocation}/data/`
6. Gzips and archives source file to `webdb-us-east-2-archive/{YYYY/MM/DD/}`
7. Deletes original JSONL

## Table Schema

`webdb.domains` Iceberg table (namespace `webdb`, bucket `webdb`):

| Column | Type |
|---|---|
| `dns` | string |
| `ns` | string |
| `ip` | string |
| `co` | string |
| `web` | string |
| `eml` | string |
| `hold` | string |
| `tel` | long |
| `rank` | long |
| `ts` | string |
| `id` | string |
| `sld` | string |
| `tld` | string |
| `asn` | long |

## Prerequisites

- Python 3.12+
- AWS CDK v2
- Docker (for building the insert Lambda image)
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

## Repository Layout

- [app.py](app.py) — CDK app entry point
- [webdb/](webdb/) — CDK stack definitions
- [insert/](insert/) — Docker Lambda handler, Dockerfile, requirements
- [enrich/](enrich/) — enrichment Lambda handler
- [transfer/](transfer/) — transfer Lambda handler
